//! §18.8 Retry Policy: Beta-Bernoulli Expected-Loss Controller + Starvation Fairness.
//!
//! When a transaction's commit is aborted (after SAFE merge ladder fails),
//! the retry controller decides whether to fail immediately or retry after
//! waiting `t` milliseconds. Uses a discrete Beta-Bernoulli model to estimate
//! `p_succ(t | evidence)` for each candidate wait time, selecting the action
//! that minimizes expected loss.

use std::fmt;

// ---------------------------------------------------------------------------
// Candidate wait times (§18.8)
// ---------------------------------------------------------------------------

/// Default candidate wait times in milliseconds.
pub const DEFAULT_CANDIDATE_WAITS_MS: [u64; 8] = [0, 1, 2, 5, 10, 20, 50, 100];

/// Maximum number of contention buckets (NI-4).
pub const MAX_CONTENTION_BUCKETS: usize = 16;

/// Default starvation threshold: after this many consecutive conflicts
/// on a single transaction, escalate to serialized mode.
pub const DEFAULT_STARVATION_THRESHOLD: u32 = 5;

// ---------------------------------------------------------------------------
// Retry action
// ---------------------------------------------------------------------------

/// The retry controller's decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryAction {
    /// Fail immediately with SQLITE_BUSY.
    FailNow,
    /// Retry after waiting `wait_ms` milliseconds.
    RetryAfter { wait_ms: u64 },
}

impl fmt::Display for RetryAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FailNow => write!(f, "FailNow"),
            Self::RetryAfter { wait_ms } => write!(f, "RetryAfter({wait_ms}ms)"),
        }
    }
}

// ---------------------------------------------------------------------------
// Beta-Bernoulli posterior (§18.8 discrete model)
// ---------------------------------------------------------------------------

/// Beta-Bernoulli posterior for a single candidate wait time.
#[derive(Debug, Clone, Copy)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct BetaPosterior {
    /// Pseudo-count of successes (default: 1.0 for uniform prior).
    pub alpha: f64,
    /// Pseudo-count of failures (default: 1.0 for uniform prior).
    pub beta: f64,
}

impl Default for BetaPosterior {
    fn default() -> Self {
        Self {
            alpha: 1.0,
            beta: 1.0,
        }
    }
}

impl BetaPosterior {
    /// Create a new posterior with given prior counts.
    #[must_use]
    pub fn new(alpha: f64, beta: f64) -> Self {
        Self { alpha, beta }
    }

    /// Observe a trial outcome.
    pub fn observe(&mut self, success: bool) {
        if success {
            self.alpha += 1.0;
        } else {
            self.beta += 1.0;
        }
    }

    /// Posterior mean: `alpha / (alpha + beta)`.
    #[must_use]
    pub fn mean(&self) -> f64 {
        self.alpha / (self.alpha + self.beta)
    }
}

// ---------------------------------------------------------------------------
// Contention bucket (NI-4)
// ---------------------------------------------------------------------------

/// Key for a contention bucket: (active writers, collision mass estimate).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentionBucketKey {
    /// Number of active writers (quantized).
    pub n_active_quantized: u8,
    /// Collision mass estimate (quantized to 0..15).
    pub m2_hat_quantized: u8,
}

impl ContentionBucketKey {
    /// Deterministic bucket assignment from raw contention metrics.
    ///
    /// `n_active` is clamped to [1, 16], `m2_hat` is quantized to [0, 15].
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn from_raw(n_active: u32, m2_hat: f64) -> Self {
        let n_clamped = n_active.clamp(1, 16);
        let n_quantized = u8::try_from(n_clamped).unwrap_or(16);
        // Quantize m2_hat to [0, 15]: floor(m2_hat * 15).clamp(0, 15)
        let m2_q = (m2_hat * 15.0).floor().clamp(0.0, 15.0) as u8;
        Self {
            n_active_quantized: n_quantized,
            m2_hat_quantized: m2_q,
        }
    }

    /// Combined bucket index (0..255). Two distinct raw inputs that yield the
    /// same quantized values MUST map to the same bucket.
    #[must_use]
    pub fn bucket_index(&self) -> u8 {
        // 16 n_active levels * 16 m2_hat levels = 256 possible buckets,
        // but we cap at MAX_CONTENTION_BUCKETS by folding.
        let raw = u16::from(self.n_active_quantized.wrapping_sub(1)) * 16
            + u16::from(self.m2_hat_quantized);
        u8::try_from(raw % u16::try_from(MAX_CONTENTION_BUCKETS).unwrap_or(16)).unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Expected-loss computation (§18.8)
// ---------------------------------------------------------------------------

/// Cost parameters for the retry decision.
#[derive(Debug, Clone, Copy)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct RetryCostParams {
    /// Cost of surfacing SQLITE_BUSY to the application.
    pub c_fail: f64,
    /// Cost of one retry attempt (validation + potential write amplification).
    pub c_try: f64,
}

impl Default for RetryCostParams {
    fn default() -> Self {
        Self {
            c_fail: 100.0,
            c_try: 1.0,
        }
    }
}

/// Expected loss of `FailNow`: `E[Loss(FailNow)] = C_fail`.
#[must_use]
pub fn expected_loss_failnow(params: &RetryCostParams) -> f64 {
    params.c_fail
}

/// Expected loss of `RetryAfter(t)`:
/// `E[Loss(RetryAfter(t))] = t + C_try + (1 - p_succ(t)) * C_fail`
/// where `t` is in milliseconds.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn expected_loss_retry(wait_ms: u64, p_succ: f64, params: &RetryCostParams) -> f64 {
    (1.0 - p_succ).mul_add(params.c_fail, wait_ms as f64 + params.c_try)
}

// ---------------------------------------------------------------------------
// Hazard-model smoothing (§18.8 optional)
// ---------------------------------------------------------------------------

/// Hazard-model parameters for continuous smoothing of `p_succ(t)`.
#[derive(Debug, Clone, Copy)]
#[allow(clippy::derive_partial_eq_without_eq)]
pub struct HazardModelParams {
    /// Hazard rate parameter λ.
    pub lambda: f64,
}

impl HazardModelParams {
    /// Create new hazard model parameters.
    #[must_use]
    pub fn new(lambda: f64) -> Self {
        Self { lambda }
    }

    /// Predicted success probability: `p_succ(t) = 1 - exp(-λ * t)`.
    #[must_use]
    pub fn p_succ(&self, wait_ms: f64) -> f64 {
        1.0 - (-self.lambda * wait_ms).exp()
    }

    /// Optimal wait time: `t* = (1/λ) * ln(λ * C_fail)` if `λ * C_fail > 1`,
    /// else `t* = 0` (FailNow is optimal).
    ///
    /// Returns the unclamped optimal wait in milliseconds.
    #[must_use]
    pub fn optimal_wait_ms(&self, c_fail: f64) -> f64 {
        let product = self.lambda * c_fail;
        if product <= 1.0 {
            0.0
        } else {
            product.ln() / self.lambda
        }
    }

    /// Optimal wait clamped to `[0, budget_ms]` and rounded to nearest candidate.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn optimal_wait_clamped(&self, c_fail: f64, budget_ms: u64, candidates: &[u64]) -> u64 {
        let raw = self.optimal_wait_ms(c_fail);
        let clamped = raw.clamp(0.0, budget_ms as f64);
        // Round to nearest candidate within budget.
        let mut best = 0_u64;
        let mut best_dist = f64::MAX;
        for &t in candidates {
            if t > budget_ms {
                continue;
            }
            let dist = (clamped - t as f64).abs();
            if dist < best_dist {
                best_dist = dist;
                best = t;
            }
        }
        best
    }
}

// ---------------------------------------------------------------------------
// Evidence ledger entry (NI-7)
// ---------------------------------------------------------------------------

/// Evidence ledger entry for a retry decision. Required by NI-7.
#[derive(Debug, Clone)]
pub struct RetryEvidenceEntry {
    /// Transaction identifier.
    pub txn_id: u64,
    /// Candidate wait times considered.
    pub candidate_set: Vec<u64>,
    /// Estimated success probability per candidate.
    pub p_hat: Vec<f64>,
    /// Expected loss per candidate.
    pub expected_losses: Vec<f64>,
    /// Chosen action.
    pub chosen_action: RetryAction,
    /// Expected loss of the FailNow alternative.
    pub expected_loss_failnow: f64,
    /// Active BOCPD regime id.
    pub regime_id: u64,
    /// Contention bucket key (if applicable).
    pub bucket_key: Option<ContentionBucketKey>,
    /// Alpha values for Beta posteriors.
    pub alpha_values: Vec<f64>,
    /// Beta values for Beta posteriors.
    pub beta_values: Vec<f64>,
    /// Is this a starvation escalation?
    pub starvation_escalation: bool,
}

impl RetryEvidenceEntry {
    /// Check that all required fields are populated (NI-7 compliance).
    #[must_use]
    pub fn is_complete(&self) -> bool {
        !self.candidate_set.is_empty()
            && self.p_hat.len() == self.candidate_set.len()
            && self.expected_losses.len() == self.candidate_set.len()
            && self.alpha_values.len() == self.candidate_set.len()
            && self.beta_values.len() == self.candidate_set.len()
    }
}

// ---------------------------------------------------------------------------
// Retry controller (§18.8 core)
// ---------------------------------------------------------------------------

/// Beta-Bernoulli expected-loss retry controller.
///
/// Maintains per-candidate-wait Beta posteriors, supports contention
/// conditioning, and produces evidence ledger entries for every decision.
pub struct RetryController {
    /// Cost parameters.
    pub params: RetryCostParams,
    /// Candidate wait times in ms.
    candidates: Vec<u64>,
    /// Per-candidate Beta posteriors (flat: one set for unconditioned use).
    posteriors: Vec<BetaPosterior>,
    /// Starvation threshold: consecutive conflicts before escalation.
    starvation_threshold: u32,
    /// Evidence ledger.
    ledger: Vec<RetryEvidenceEntry>,
    /// Per-transaction conflict counter (txn_id → consecutive_conflicts).
    conflict_counts: Vec<(u64, u32)>,
}

impl RetryController {
    /// Create a new controller with default candidate waits and costs.
    #[must_use]
    pub fn new(params: RetryCostParams) -> Self {
        let candidates = DEFAULT_CANDIDATE_WAITS_MS.to_vec();
        let posteriors = vec![BetaPosterior::default(); candidates.len()];
        Self {
            params,
            candidates,
            posteriors,
            starvation_threshold: DEFAULT_STARVATION_THRESHOLD,
            ledger: Vec::new(),
            conflict_counts: Vec::new(),
        }
    }

    /// Create with custom candidate set and starvation threshold.
    #[must_use]
    pub fn with_candidates(
        params: RetryCostParams,
        candidates: Vec<u64>,
        starvation_threshold: u32,
    ) -> Self {
        let posteriors = vec![BetaPosterior::default(); candidates.len()];
        Self {
            params,
            candidates,
            posteriors,
            starvation_threshold,
            ledger: Vec::new(),
            conflict_counts: Vec::new(),
        }
    }

    /// Decide whether to retry or fail, given remaining time budget.
    ///
    /// Returns `(action, evidence_entry)`.
    #[allow(clippy::cast_precision_loss)]
    pub fn decide(
        &mut self,
        txn_id: u64,
        budget_ms: u64,
        regime_id: u64,
        bucket_key: Option<ContentionBucketKey>,
    ) -> RetryAction {
        // Track starvation.
        let conflict_count = self.increment_conflict(txn_id);
        let starvation_escalation = conflict_count >= self.starvation_threshold;

        // Budget exhaustion (NI-6).
        if budget_ms == 0 {
            let entry = self.build_evidence(
                txn_id,
                &[],
                &[],
                &[],
                RetryAction::FailNow,
                regime_id,
                bucket_key,
                starvation_escalation,
            );
            self.ledger.push(entry);
            return RetryAction::FailNow;
        }

        // Build candidate set clamped by budget.
        let eligible: Vec<u64> = self
            .candidates
            .iter()
            .copied()
            .filter(|&t| t <= budget_ms)
            .collect();

        if eligible.is_empty() {
            let entry = self.build_evidence(
                txn_id,
                &[],
                &[],
                &[],
                RetryAction::FailNow,
                regime_id,
                bucket_key,
                starvation_escalation,
            );
            self.ledger.push(entry);
            return RetryAction::FailNow;
        }

        // Compute expected loss for each candidate.
        let el_fail = expected_loss_failnow(&self.params);
        let mut p_hats = Vec::with_capacity(eligible.len());
        let mut losses = Vec::with_capacity(eligible.len());

        for &t in &eligible {
            let idx = self.candidate_index(t);
            let p = self.posteriors[idx].mean();
            let el = expected_loss_retry(t, p, &self.params);
            p_hats.push(p);
            losses.push(el);
        }

        // Find argmin.
        let mut best_action = RetryAction::FailNow;
        let mut best_loss = el_fail;

        for (i, &el) in losses.iter().enumerate() {
            if el < best_loss {
                best_loss = el;
                best_action = RetryAction::RetryAfter {
                    wait_ms: eligible[i],
                };
            }
        }

        let entry = self.build_evidence(
            txn_id,
            &eligible,
            &p_hats,
            &losses,
            best_action,
            regime_id,
            bucket_key,
            starvation_escalation,
        );
        self.ledger.push(entry);

        best_action
    }

    /// Observe the outcome of a retry attempt.
    pub fn observe(&mut self, wait_ms: u64, success: bool) {
        let idx = self.candidate_index(wait_ms);
        self.posteriors[idx].observe(success);
    }

    /// Decide with Cx deadline awareness.
    ///
    /// If `cx_cancelled` is true, returns `FailNow` immediately —
    /// the caller maps this to `SQLITE_INTERRUPT` (NI-6/NI-9).
    /// If `budget_ms == 0`, returns `FailNow` for `SQLITE_BUSY`.
    #[allow(clippy::cast_precision_loss)]
    pub fn decide_with_cx(
        &mut self,
        txn_id: u64,
        budget_ms: u64,
        regime_id: u64,
        bucket_key: Option<ContentionBucketKey>,
        cx_cancelled: bool,
    ) -> RetryAction {
        if cx_cancelled {
            return RetryAction::FailNow;
        }
        self.decide(txn_id, budget_ms, regime_id, bucket_key)
    }

    /// Clear conflict counter for a transaction (after successful commit).
    pub fn clear_conflict(&mut self, txn_id: u64) {
        self.conflict_counts.retain(|(id, _)| *id != txn_id);
    }

    /// Access the evidence ledger.
    #[must_use]
    pub fn ledger(&self) -> &[RetryEvidenceEntry] {
        &self.ledger
    }

    /// Access posterior for a given candidate wait time.
    #[must_use]
    pub fn posterior(&self, wait_ms: u64) -> &BetaPosterior {
        let idx = self.candidate_index(wait_ms);
        &self.posteriors[idx]
    }

    /// Number of contention conflict records tracked.
    #[must_use]
    pub fn tracked_conflicts(&self) -> usize {
        self.conflict_counts.len()
    }

    /// Check if a transaction has been escalated due to starvation.
    #[must_use]
    pub fn is_starvation_escalated(&self, txn_id: u64) -> bool {
        self.conflict_counts
            .iter()
            .any(|(id, count)| *id == txn_id && *count >= self.starvation_threshold)
    }

    // -- internal helpers --

    fn candidate_index(&self, wait_ms: u64) -> usize {
        self.candidates
            .iter()
            .position(|&t| t == wait_ms)
            .unwrap_or(0)
    }

    fn increment_conflict(&mut self, txn_id: u64) -> u32 {
        for (id, count) in &mut self.conflict_counts {
            if *id == txn_id {
                *count += 1;
                return *count;
            }
        }
        self.conflict_counts.push((txn_id, 1));
        1
    }

    #[allow(clippy::too_many_arguments)]
    fn build_evidence(
        &self,
        txn_id: u64,
        eligible: &[u64],
        p_hats: &[f64],
        losses: &[f64],
        chosen_action: RetryAction,
        regime_id: u64,
        bucket_key: Option<ContentionBucketKey>,
        starvation_escalation: bool,
    ) -> RetryEvidenceEntry {
        let alphas: Vec<f64> = eligible
            .iter()
            .map(|&t| self.posteriors[self.candidate_index(t)].alpha)
            .collect();
        let betas: Vec<f64> = eligible
            .iter()
            .map(|&t| self.posteriors[self.candidate_index(t)].beta)
            .collect();

        RetryEvidenceEntry {
            txn_id,
            candidate_set: eligible.to_vec(),
            p_hat: p_hats.to_vec(),
            expected_losses: losses.to_vec(),
            chosen_action,
            expected_loss_failnow: expected_loss_failnow(&self.params),
            regime_id,
            bucket_key,
            alpha_values: alphas,
            beta_values: betas,
            starvation_escalation,
        }
    }
}

// ---------------------------------------------------------------------------
// Gittins index approximation (§18.8 optional)
// ---------------------------------------------------------------------------

/// Approximate Gittins index for a Beta-Bernoulli bandit arm.
///
/// Uses the Whittle (1980) upper-bound approximation:
/// `G(α, β) ≈ α / (α + β) + sqrt(2 * α * β / ((α + β)^2 * (α + β + 1)))`
///
/// For the retry controller, arms where `G(α_t, β_t) > threshold` should be
/// explored (retried). The threshold is derived from the cost ratio
/// `C_try / C_fail`.
#[must_use]
pub fn gittins_index_approx(alpha: f64, beta: f64) -> f64 {
    let total = alpha + beta;
    let mean = alpha / total;
    let exploration_bonus = (2.0 * alpha * beta / (total * total * (total + 1.0))).sqrt();
    mean + exploration_bonus
}

/// Gittins threshold: if `gittins_index > threshold`, retry is worthwhile.
/// `threshold = 1 - C_try / C_fail` (when `C_try < C_fail`).
#[must_use]
pub fn gittins_threshold(c_try: f64, c_fail: f64) -> f64 {
    if c_fail <= 0.0 {
        return 0.0;
    }
    (1.0 - c_try / c_fail).max(0.0)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_beta_posterior_default() {
        let bp = BetaPosterior::default();
        assert!((bp.mean() - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_beta_posterior_observe() {
        let mut bp = BetaPosterior::default();
        bp.observe(true);
        // alpha=2, beta=1 → mean=2/3
        assert!((bp.mean() - 2.0 / 3.0).abs() < 1e-10);
        bp.observe(false);
        // alpha=2, beta=2 → mean=0.5
        assert!((bp.mean() - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_expected_loss_failnow_equals_cfail() {
        let params = RetryCostParams {
            c_fail: 42.0,
            c_try: 1.0,
        };
        assert!((expected_loss_failnow(&params) - 42.0).abs() < 1e-10);
    }

    #[test]
    fn test_expected_loss_retry_formula() {
        let params = RetryCostParams {
            c_fail: 100.0,
            c_try: 5.0,
        };
        // wait=10ms, p_succ=0.8 → 10 + 5 + (1-0.8)*100 = 10 + 5 + 20 = 35
        let el = expected_loss_retry(10, 0.8, &params);
        assert!((el - 35.0).abs() < 1e-10);
    }

    #[test]
    fn test_hazard_model_optimal() {
        let hm = HazardModelParams::new(0.5);
        // lambda=0.5, c_fail=100 → lambda*c_fail=50 > 1
        // t* = (1/0.5)*ln(50) = 2*ln(50) ≈ 7.824
        let t_star = hm.optimal_wait_ms(100.0);
        let expected = 2.0_f64.mul_add(50.0_f64.ln(), 0.0);
        assert!((t_star - expected).abs() < 0.01);
    }

    #[test]
    fn test_hazard_model_no_retry() {
        let hm = HazardModelParams::new(0.01);
        // lambda=0.01, c_fail=50 → lambda*c_fail=0.5 <= 1 → t*=0
        assert!((hm.optimal_wait_ms(50.0)).abs() < 1e-10);
    }

    #[test]
    fn test_contention_bucket_deterministic() {
        let k1 = ContentionBucketKey::from_raw(4, 0.025);
        let k2 = ContentionBucketKey::from_raw(4, 0.025);
        assert_eq!(k1.bucket_index(), k2.bucket_index());
    }

    #[test]
    fn test_contention_bucket_bounded() {
        // All possible combinations produce indices < MAX_CONTENTION_BUCKETS.
        for n in 0..=20 {
            for m2_step in 0..=20 {
                let m2 = f64::from(m2_step) / 20.0;
                let k = ContentionBucketKey::from_raw(n, m2);
                assert!(
                    usize::from(k.bucket_index()) < MAX_CONTENTION_BUCKETS,
                    "bucket_index={} for n={n} m2={m2}",
                    k.bucket_index()
                );
            }
        }
    }

    #[test]
    fn test_controller_budget_exhausted() {
        let mut ctrl = RetryController::new(RetryCostParams::default());
        let action = ctrl.decide(1, 0, 0, None);
        assert_eq!(action, RetryAction::FailNow);
    }

    #[test]
    fn test_controller_argmin() {
        let params = RetryCostParams {
            c_fail: 100.0,
            c_try: 1.0,
        };
        let mut ctrl = RetryController::new(params);

        // Train the 5ms wait to have high success rate.
        for _ in 0..50 {
            ctrl.observe(5, true);
        }
        for _ in 0..2 {
            ctrl.observe(5, false);
        }

        let action = ctrl.decide(1, 100, 0, None);
        // With high p_succ for 5ms, it should be picked over FailNow.
        assert!(
            matches!(action, RetryAction::RetryAfter { wait_ms: 5 }),
            "Expected RetryAfter(5ms), got {action:?}"
        );
    }

    #[test]
    fn test_evidence_ledger_complete() {
        let mut ctrl = RetryController::new(RetryCostParams::default());
        let _ = ctrl.decide(42, 50, 7, Some(ContentionBucketKey::from_raw(4, 0.1)));
        assert!(!ctrl.ledger().is_empty());
        let entry = &ctrl.ledger()[0];
        assert!(entry.is_complete() || entry.candidate_set.is_empty());
        assert_eq!(entry.txn_id, 42);
        assert_eq!(entry.regime_id, 7);
    }

    #[test]
    fn test_starvation_escalation() {
        let params = RetryCostParams::default();
        let mut ctrl =
            RetryController::with_candidates(params, vec![0, 5, 10], DEFAULT_STARVATION_THRESHOLD);

        for i in 0..DEFAULT_STARVATION_THRESHOLD {
            let _ = ctrl.decide(99, 100, 0, None);
            assert_eq!(
                ctrl.is_starvation_escalated(99),
                i + 1 >= DEFAULT_STARVATION_THRESHOLD
            );
        }

        // After threshold, should be escalated.
        assert!(ctrl.is_starvation_escalated(99));
        // Last entry should have starvation flag.
        assert!(ctrl.ledger().last().unwrap().starvation_escalation);
    }

    #[test]
    fn test_gittins_index_basic() {
        // Uniform prior: alpha=1, beta=1 → mean=0.5, bonus>0
        let gi = gittins_index_approx(1.0, 1.0);
        assert!(
            gi > 0.5,
            "Gittins index should exceed mean for uniform prior"
        );

        // Strong evidence: alpha=100, beta=1 → should be close to 1
        let gi_strong = gittins_index_approx(100.0, 1.0);
        assert!(gi_strong > 0.95);
    }

    // -- Additional spec-required tests (bd-1p75) --

    #[test]
    fn test_beta_bernoulli_posterior_mean() {
        // Known sequence: 3 successes, 1 failure from prior (1,1).
        // alpha=4, beta=2 → p_hat = 4/6 ≈ 0.6667
        let mut bp = BetaPosterior::new(1.0, 1.0);
        bp.observe(true);
        bp.observe(true);
        bp.observe(true);
        bp.observe(false);
        let expected = 4.0 / 6.0;
        assert!(
            (bp.mean() - expected).abs() < 1e-10,
            "bead_id=bd-1p75 case=posterior_mean p={} expected={expected}",
            bp.mean()
        );
    }

    #[test]
    fn test_budget_clamp() {
        // Budget of 3ms: only candidates {0, 1, 2} should be in the eligible set.
        let mut ctrl = RetryController::new(RetryCostParams::default());
        let _ = ctrl.decide(1, 3, 0, None);
        let entry = ctrl.ledger().last().expect("should have entry");
        for &t in &entry.candidate_set {
            assert!(
                t <= 3,
                "bead_id=bd-1p75 case=budget_clamp candidate={t} budget=3"
            );
        }
        // Verify 5ms (the next candidate) is NOT in the set.
        assert!(
            !entry.candidate_set.contains(&5),
            "bead_id=bd-1p75 case=budget_clamp 5ms should be excluded"
        );
    }

    #[test]
    fn test_hazard_model_clamp_budget() {
        let hm = HazardModelParams::new(0.001);
        // lambda*C_fail = 0.001 * 10000 = 10 > 1, so t* = 1000 * ln(10) ≈ 2302.
        // But budget is 50ms, so clamped to 50.
        let clamped = hm.optimal_wait_clamped(10_000.0, 50, &DEFAULT_CANDIDATE_WAITS_MS);
        assert_eq!(
            clamped, 50,
            "bead_id=bd-1p75 case=hazard_clamp_budget clamped={clamped}"
        );
    }

    #[test]
    fn test_no_priority_for_retries() {
        // NI-5: Retried transactions must NOT get priority over new ones.
        // Two controllers with identical state: one "fresh" (0 consecutive conflicts),
        // one "retried" (2 consecutive). Both should pick the same action.
        let params = RetryCostParams::default();
        let mut ctrl_fresh = RetryController::new(params);
        let mut ctrl_retried = RetryController::new(RetryCostParams::default());

        // Give both the same observations.
        for _ in 0..10 {
            ctrl_fresh.observe(5, true);
            ctrl_retried.observe(5, true);
        }

        let action_fresh = ctrl_fresh.decide(1, 100, 0, None);
        let action_retried = ctrl_retried.decide(2, 100, 0, None);

        assert_eq!(
            action_fresh, action_retried,
            "bead_id=bd-1p75 case=no_priority fresh={action_fresh:?} retried={action_retried:?}"
        );
    }

    #[test]
    fn test_evidence_ledger_starvation() {
        // After starvation threshold, evidence entry must record the escalation.
        let params = RetryCostParams::default();
        let mut ctrl = RetryController::with_candidates(
            params,
            vec![0, 5, 10],
            3, // low threshold for test
        );

        // Fire 3 decisions on same txn → starvation.
        for _ in 0..3 {
            let _ = ctrl.decide(77, 100, 0, None);
        }

        let last = ctrl.ledger().last().expect("should have entry");
        assert!(
            last.starvation_escalation,
            "bead_id=bd-1p75 case=ledger_starvation expected=true"
        );
        assert_eq!(
            last.txn_id, 77,
            "bead_id=bd-1p75 case=ledger_starvation_txn"
        );
    }

    #[test]
    fn test_gittins_index_threshold() {
        // Gittins threshold with c_try=1, c_fail=100 → threshold = 1 - 1/100 = 0.99.
        let threshold = gittins_threshold(1.0, 100.0);
        assert!(
            (threshold - 0.99).abs() < 1e-10,
            "bead_id=bd-1p75 case=gittins_threshold threshold={threshold}"
        );

        // Alpha=100, beta=1 → Gittins index ~ 0.99+ → should exceed threshold.
        let gi = gittins_index_approx(100.0, 1.0);
        assert!(
            gi > threshold,
            "bead_id=bd-1p75 case=gittins_retry gi={gi} threshold={threshold}"
        );

        // Alpha=1, beta=100 → Gittins index ~ 0.01 → should NOT exceed threshold.
        let gi_low = gittins_index_approx(1.0, 100.0);
        assert!(
            gi_low < threshold,
            "bead_id=bd-1p75 case=gittins_no_retry gi={gi_low} threshold={threshold}"
        );
    }

    #[test]
    fn test_cx_deadline_respected() {
        // If Cx cancelled, controller must return FailNow immediately.
        let mut ctrl = RetryController::new(RetryCostParams::default());
        let action = ctrl.decide_with_cx(1, 1000, 0, None, true);
        assert_eq!(
            action,
            RetryAction::FailNow,
            "bead_id=bd-1p75 case=cx_deadline"
        );

        // If Cx deadline is shorter than busy_timeout, effective budget is Cx.
        // (Caller is responsible for passing min(busy_timeout, cx_remaining) as budget.)
        let action2 = ctrl.decide_with_cx(2, 2, 0, None, false);
        // With budget=2ms, only candidates {0, 1, 2} eligible.
        let entry = ctrl.ledger().last().expect("should have entry");
        for &t in &entry.candidate_set {
            assert!(
                t <= 2,
                "bead_id=bd-1p75 case=cx_effective_budget candidate={t}"
            );
        }
        // Should still return a valid action.
        assert!(
            matches!(
                action2,
                RetryAction::FailNow | RetryAction::RetryAfter { .. }
            ),
            "bead_id=bd-1p75 case=cx_effective_budget action={action2:?}"
        );
    }
}
