//! Anytime-valid e-process gate for skipping Cahill/Fekete SSI validation.
//!
//! # Motivation
//!
//! Every `BEGIN CONCURRENT` commit runs `ssi_validate_and_publish` which
//! discovers incoming/outgoing rw-antidependency edges and tests for the
//! "dangerous structure" pivot (an inbound rw edge and an outbound rw
//! edge on the same transaction, per Cahill & Fekete 2008). On workloads
//! where true SSI pivots are very rare, this validation is a constant
//! per-commit tax on the successful path.
//!
//! This module implements an **anytime-valid sequential test** (e-process)
//! that accumulates evidence across historical commit outcomes. When the
//! evidence strongly supports H0 ("no SSI pivot would be detected") **and**
//! the current commit's coarse (rw_in, rw_out) pattern falls inside a
//! "safe region" that has been pivot-free for K consecutive commits, the
//! caller is permitted to skip the full SSI validation.
//!
//! # Mathematical shape
//!
//! The e-process is a non-negative supermartingale under H0. On every
//! commit we feed one observation `x ∈ {0, 1}` where `x = 1` iff the full
//! SSI check (when it ran) detected a pivot.
//!
//! Let `p0` be the assumed conflict rate under H0 and `q = alt_mult · p0`
//! the conflict rate under H1 (with `alt_mult` a multiplicative effect
//! size clamped into `[2, 1/p0 - 1]`). The likelihood ratio for a
//! simple-vs-simple Bernoulli test is exactly
//!
//! ```text
//! LR(x=1) = q / p0           = alt_mult
//! LR(x=0) = (1 - q)/(1 - p0)
//! ```
//!
//! which gives `E[LR | H0] = p0·(q/p0) + (1-p0)·((1-q)/(1-p0)) = 1`.
//! The product of LRs is therefore a (tight) martingale under H0 and a
//! supermartingale under any true rate `≤ p0`. By Ville's inequality,
//! for any stopping rule τ:
//!
//! ```text
//! P(sup_n E_n ≥ 1/α | H0) ≤ α
//! ```
//!
//! # Decision rule
//!
//! The gate answers `should_skip_ssi() -> bool`:
//!
//! 1. The e-process must not be in `Alert` state (i.e. no evidence of H1).
//! 2. The last `min_clean_streak` consecutive *observed* commits must have
//!    had `x = 0` (no pivot).
//! 3. There must have been at least `min_observations` total samples so
//!    the gate does not open on a cold start.
//!
//! Any caller that skips SSI **must also periodically sample** (by running
//! the full validation anyway and feeding the outcome to `observe`), so
//! the e-process keeps a live sliding signal instead of trusting a stale
//! history. The skip rate is tunable by `periodic_sample_rate`.
//!
//! # Safety argument
//!
//! Skipping SSI validation is safe to *serializability* only if the null
//! hypothesis ("no pivot present for this commit") is true. The e-process
//! bounds the *long-run* rate at which we incorrectly skip a commit that
//! did have a pivot by `α`, **provided the per-commit conflict events are
//! exchangeable under H0** — which is a standard but non-trivial
//! assumption. Workloads with sudden regime changes (hot-spot bursts)
//! violate exchangeability; we mitigate this by resetting the e-process
//! whenever the caller observes a conflict and by requiring a strictly
//! positive `periodic_sample_rate` so the skip region cannot become
//! absorbing.
//!
//! This gate is gated behind the `write_merge = LAB_UNSAFE` PRAGMA
//! (spec §5.10). Default SAFE mode never consults this gate.
//!
//! # Relationship to asupersync::obligation::eprocess
//!
//! `asupersync::obligation::eprocess::LeakMonitor` is a similar
//! construction specialised for *obligation-age observations* (Exp-family
//! likelihood). This module uses a Bernoulli likelihood suited to
//! SSI-outcome observations, and is intentionally self-contained so
//! `fsqlite-mvcc` does not gain a dependency on `asupersync` just for
//! this optimisation. The supermartingale property and Ville's inequality
//! apply identically.

use std::fmt;

/// Configuration for the e-process SSI-skip gate.
#[derive(Debug, Clone, Copy)]
pub struct SsiEProcessConfig {
    /// Type-I error bound: the long-run probability of skipping a commit
    /// that should have been flagged is bounded by `alpha` under H0.
    /// Must be in `(0, 1)`. Default: `1e-3`.
    pub alpha: f64,
    /// Assumed conflict rate under the null hypothesis. Smaller values
    /// accumulate evidence faster but punish single conflicts harder.
    /// Must be in `(0, 0.5)`. Default: `1e-4`.
    pub p0: f64,
    /// Multiplicative effect size: the alternative hypothesis is
    /// Bernoulli(`alt_mult · p0`). Must be in `[2, (1/p0) - 1]`. Larger
    /// values flag bursts of conflicts faster; smaller values are more
    /// sensitive to slow drift. Default: `50.0`.
    pub alt_mult: f64,
    /// Minimum number of observations before the gate may open.
    /// Default: `64`.
    pub min_observations: u64,
    /// Minimum number of consecutive conflict-free observed commits
    /// before the gate may open. Default: `32`.
    pub min_clean_streak: u64,
    /// Per-commit probability (in `[0, 1]`) of forcing a full SSI
    /// validation even when the gate would allow a skip. This is the
    /// audit sampling rate that keeps the e-process honest when the skip
    /// path is the dominant outcome. Default: `0.05` (5%).
    pub periodic_sample_rate: f64,
}

impl Default for SsiEProcessConfig {
    fn default() -> Self {
        Self {
            alpha: 1e-3,
            p0: 1e-4,
            alt_mult: 50.0,
            min_observations: 64,
            min_clean_streak: 32,
            periodic_sample_rate: 0.05,
        }
    }
}

impl SsiEProcessConfig {
    /// Replaces `alpha`, returning the modified config. Returns `None` if
    /// `alpha` is outside `(0, 1)` (rather than panicking — PRAGMA values
    /// are user-controlled).
    #[must_use]
    pub fn with_alpha(mut self, alpha: f64) -> Option<Self> {
        if alpha.is_finite() && alpha > 0.0 && alpha < 1.0 {
            self.alpha = alpha;
            Some(self)
        } else {
            None
        }
    }
}

/// Alert state of the gate — mirrors the semantics of
/// `asupersync::obligation::eprocess::AlertState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateAlertState {
    /// E-value has not crossed the `1/α` threshold and enough observations
    /// have accumulated for a decision.
    Clear,
    /// E-value is above 1 (some weak evidence against H0) but below
    /// threshold. Skip is still disallowed.
    Watching,
    /// E-value has crossed `1/α`: H0 is rejected. Skip is forbidden until
    /// the gate is manually reset.
    Alert,
}

impl fmt::Display for GateAlertState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clear => f.write_str("clear"),
            Self::Watching => f.write_str("watching"),
            Self::Alert => f.write_str("ALERT"),
        }
    }
}

/// E-process gate for SSI validation skipping.
///
/// Not internally synchronised — callers (one per `Connection`) are
/// responsible for mutual exclusion. This matches the single-connection
/// usage pattern elsewhere in `fsqlite-core`.
#[derive(Debug)]
pub struct SsiEProcessGate {
    config: SsiEProcessConfig,
    /// log(e-value). Starts at 0 (e-value = 1).
    log_e: f64,
    /// Rejection threshold: `1 / alpha`. Cached.
    threshold: f64,
    /// Log of the threshold. Cached.
    log_threshold: f64,
    /// Total number of observations fed to `observe`.
    observations: u64,
    /// Number of consecutive recent observations with `x = 0`.
    clean_streak: u64,
    /// Peak e-value observed (for diagnostics).
    peak_log_e: f64,
    /// Number of times the gate crossed into the `Alert` state.
    alert_count: u64,
    /// Counter for periodic audit sampling. Increments on each
    /// `should_skip_ssi` consultation.
    skip_consultations: u64,
    /// Number of times the gate opened (returned `true`).
    skip_grants: u64,
    /// Cached log(lr_one) = log(alt_mult).
    log_lr_one: f64,
    /// Cached log(lr_zero) = log((1 - q)/(1 - p0)).
    log_lr_zero: f64,
}

impl SsiEProcessGate {
    /// Creates a new gate with the given configuration.
    ///
    /// Invalid configs are clamped to safe defaults: `alpha` is pinned
    /// into `(0, 1)`, `p0` into `(0, 0.5)`. This keeps the PRAGMA-driven
    /// configuration path panic-free.
    #[must_use]
    pub fn new(mut config: SsiEProcessConfig) -> Self {
        if !(config.alpha.is_finite() && config.alpha > 0.0 && config.alpha < 1.0) {
            config.alpha = 1e-3;
        }
        if !(config.p0.is_finite() && config.p0 > 0.0 && config.p0 < 0.5) {
            config.p0 = 1e-4;
        }
        if !(config.periodic_sample_rate.is_finite()
            && (0.0..=1.0).contains(&config.periodic_sample_rate))
        {
            config.periodic_sample_rate = 0.05;
        }
        // Clamp alt_mult into [2, (1/p0) - 1] so q = alt_mult * p0 stays
        // in (p0, 1) and the simple-vs-simple LRT stays well-defined.
        let max_mult = (1.0 / config.p0 - 1.0).max(2.0);
        if !config.alt_mult.is_finite() || config.alt_mult < 2.0 {
            config.alt_mult = 50.0_f64.min(max_mult);
        } else if config.alt_mult > max_mult {
            config.alt_mult = max_mult;
        }
        let threshold = 1.0 / config.alpha;
        let log_threshold = threshold.ln();
        let q = (config.alt_mult * config.p0).min(0.999_999);
        let log_lr_one = config.alt_mult.ln();
        let log_lr_zero = ((1.0 - q) / (1.0 - config.p0)).ln();
        Self {
            config,
            log_e: 0.0,
            threshold,
            log_threshold,
            observations: 0,
            clean_streak: 0,
            peak_log_e: 0.0,
            alert_count: 0,
            skip_consultations: 0,
            skip_grants: 0,
            log_lr_one,
            log_lr_zero,
        }
    }

    /// Feeds one SSI outcome into the e-process.
    ///
    /// `conflict_detected` must be the real outcome of a
    /// `ssi_validate_and_publish` call for a concurrent-mode commit. It
    /// is the *only* entry point that moves the e-value forward; callers
    /// must never feed synthesized outcomes.
    pub fn observe(&mut self, conflict_detected: bool) {
        let was_alert = self.is_alert();
        self.observations = self.observations.saturating_add(1);

        // Simple-vs-simple Bernoulli LRT. Under H0 (true rate = p0):
        //   E[LR | H0] = p0 · (q/p0) + (1-p0) · ((1-q)/(1-p0)) = 1.
        // So `log_e` is a martingale under H0 (supermartingale under any
        // true rate ≤ p0). The log-LRs are cached at construction.
        let delta = if conflict_detected {
            self.clean_streak = 0;
            self.log_lr_one
        } else {
            self.clean_streak = self.clean_streak.saturating_add(1);
            self.log_lr_zero
        };

        self.log_e += delta;
        if self.log_e > self.peak_log_e {
            self.peak_log_e = self.log_e;
        }

        let is_alert_now = self.is_alert();
        if !was_alert && is_alert_now {
            self.alert_count = self.alert_count.saturating_add(1);
        }
    }

    /// Returns the current alert state.
    #[must_use]
    pub fn alert_state(&self) -> GateAlertState {
        if self.observations < self.config.min_observations {
            return GateAlertState::Clear;
        }
        if self.log_e >= self.log_threshold {
            GateAlertState::Alert
        } else if self.log_e > 0.0 {
            GateAlertState::Watching
        } else {
            GateAlertState::Clear
        }
    }

    /// Returns `true` iff the gate is currently asserting H1.
    #[must_use]
    pub fn is_alert(&self) -> bool {
        self.log_e >= self.log_threshold && self.observations >= self.config.min_observations
    }

    /// The core decision rule. Returns `true` iff the caller may skip the
    /// full SSI validation for this commit.
    ///
    /// `force_audit_hash` is any integer that is uniformly distributed
    /// across commits (e.g. the low bits of the commit sequence). It is
    /// used to deterministically select a subset of commits for audit
    /// sampling, instead of relying on RNG state.
    #[must_use]
    pub fn should_skip_ssi(&mut self, force_audit_hash: u64) -> bool {
        self.skip_consultations = self.skip_consultations.saturating_add(1);

        if self.is_alert() {
            return false;
        }
        if self.observations < self.config.min_observations {
            return false;
        }
        if self.clean_streak < self.config.min_clean_streak {
            return false;
        }

        // Deterministic audit sampling: force a full validation when
        // (force_audit_hash mod stride) == 0, where stride rounds to the
        // nearest integer ≥ 1.
        let rate = self.config.periodic_sample_rate.clamp(0.0, 1.0);
        if rate > 0.0 {
            let stride_f = (1.0 / rate).round();
            let stride = if stride_f.is_finite() && stride_f >= 1.0 {
                // Safe cast: stride_f is finite, ≥ 1, and bounded above
                // by 1/periodic_sample_rate which is ≤ f64::MAX.
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let s = stride_f as u64;
                s.max(1)
            } else {
                1
            };
            if force_audit_hash % stride == 0 {
                return false;
            }
        }

        self.skip_grants = self.skip_grants.saturating_add(1);
        true
    }

    /// Resets the e-process state while keeping the configuration. Call
    /// this on transaction begin when the caller suspects a regime change
    /// (e.g. a schema change, a bulk DDL operation, a long idle period).
    pub fn reset(&mut self) {
        self.log_e = 0.0;
        self.peak_log_e = 0.0;
        self.observations = 0;
        self.clean_streak = 0;
        self.alert_count = 0;
        self.skip_consultations = 0;
        self.skip_grants = 0;
    }

    /// Returns the current e-value.
    #[must_use]
    pub fn e_value(&self) -> f64 {
        self.log_e.exp()
    }

    /// Returns the rejection threshold `1 / alpha`.
    #[must_use]
    pub fn threshold(&self) -> f64 {
        self.threshold
    }

    /// Returns the number of observations.
    #[must_use]
    pub fn observations(&self) -> u64 {
        self.observations
    }

    /// Returns the consecutive conflict-free run length.
    #[must_use]
    pub fn clean_streak(&self) -> u64 {
        self.clean_streak
    }

    /// Returns the number of alert-threshold crossings.
    #[must_use]
    pub fn alert_count(&self) -> u64 {
        self.alert_count
    }

    /// Returns the number of `should_skip_ssi` consultations so far.
    #[must_use]
    pub fn skip_consultations(&self) -> u64 {
        self.skip_consultations
    }

    /// Returns the number of times `should_skip_ssi` returned `true`.
    #[must_use]
    pub fn skip_grants(&self) -> u64 {
        self.skip_grants
    }

    /// Returns an immutable view of the configuration.
    #[must_use]
    pub fn config(&self) -> &SsiEProcessConfig {
        &self.config
    }

    /// Returns a diagnostic snapshot.
    #[must_use]
    pub fn snapshot(&self) -> SsiEProcessSnapshot {
        SsiEProcessSnapshot {
            e_value: self.e_value(),
            threshold: self.threshold,
            observations: self.observations,
            clean_streak: self.clean_streak,
            alert_state: self.alert_state(),
            peak_e_value: self.peak_log_e.exp(),
            alert_count: self.alert_count,
            skip_consultations: self.skip_consultations,
            skip_grants: self.skip_grants,
        }
    }
}

/// Diagnostic snapshot of gate state.
#[derive(Debug, Clone, Copy)]
pub struct SsiEProcessSnapshot {
    /// Current e-value (`exp(log_e)`).
    pub e_value: f64,
    /// Rejection threshold.
    pub threshold: f64,
    /// Total observations.
    pub observations: u64,
    /// Current consecutive conflict-free observation streak.
    pub clean_streak: u64,
    /// Alert state.
    pub alert_state: GateAlertState,
    /// Peak e-value ever observed.
    pub peak_e_value: f64,
    /// Alert-threshold crossings.
    pub alert_count: u64,
    /// `should_skip_ssi` consultations.
    pub skip_consultations: u64,
    /// `should_skip_ssi` grants.
    pub skip_grants: u64,
}

impl fmt::Display for SsiEProcessSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SsiEProcessGate[{}]: e={:.4} thr={:.1} obs={} streak={} peak={:.4} alerts={} \
             skip_consults={} skip_grants={}",
            self.alert_state,
            self.e_value,
            self.threshold,
            self.observations,
            self.clean_streak,
            self.peak_e_value,
            self.alert_count,
            self.skip_consultations,
            self.skip_grants,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{GateAlertState, SsiEProcessConfig, SsiEProcessGate};

    fn fast_open_config() -> SsiEProcessConfig {
        SsiEProcessConfig {
            alpha: 1e-3,
            p0: 1e-3,
            alt_mult: 100.0,
            min_observations: 8,
            min_clean_streak: 4,
            periodic_sample_rate: 0.0,
        }
    }

    #[test]
    fn new_gate_starts_clear_and_locked() {
        let mut gate = SsiEProcessGate::new(fast_open_config());
        assert_eq!(gate.alert_state(), GateAlertState::Clear);
        assert!(!gate.should_skip_ssi(0));
        assert_eq!(gate.observations(), 0);
        assert!((gate.e_value() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn invalid_config_is_clamped_not_panic() {
        let bad = SsiEProcessConfig {
            alpha: -1.0,
            p0: 42.0,
            alt_mult: -5.0,
            min_observations: 0,
            min_clean_streak: 0,
            periodic_sample_rate: 17.0,
        };
        let gate = SsiEProcessGate::new(bad);
        assert!(gate.config().alpha > 0.0 && gate.config().alpha < 1.0);
        assert!(gate.config().p0 > 0.0 && gate.config().p0 < 0.5);
        assert!(gate.config().alt_mult >= 2.0);
        assert!(
            gate.config().periodic_sample_rate >= 0.0 && gate.config().periodic_sample_rate <= 1.0
        );
    }

    #[test]
    fn clean_history_opens_gate() {
        let mut gate = SsiEProcessGate::new(fast_open_config());
        // Need both min_observations (8) and min_clean_streak (4).
        for _ in 0..8 {
            gate.observe(false);
        }
        assert_eq!(gate.alert_state(), GateAlertState::Clear);
        assert!(gate.should_skip_ssi(1));
    }

    #[test]
    fn single_conflict_resets_streak() {
        let mut gate = SsiEProcessGate::new(fast_open_config());
        for _ in 0..8 {
            gate.observe(false);
        }
        assert!(gate.should_skip_ssi(1));
        gate.observe(true); // conflict resets streak
        assert_eq!(gate.clean_streak(), 0);
        assert!(!gate.should_skip_ssi(1));
    }

    #[test]
    fn min_observations_gate_holds() {
        let mut gate = SsiEProcessGate::new(SsiEProcessConfig {
            min_observations: 100,
            min_clean_streak: 1,
            ..fast_open_config()
        });
        for _ in 0..10 {
            gate.observe(false);
        }
        // Too few observations: should not skip.
        assert!(!gate.should_skip_ssi(1));
    }

    #[test]
    fn alert_fires_on_repeated_conflicts() {
        let mut gate = SsiEProcessGate::new(SsiEProcessConfig {
            alpha: 1e-3,
            p0: 1e-4,
            alt_mult: 1000.0, // q = 0.1 — a big regime shift
            min_observations: 3,
            min_clean_streak: 1,
            periodic_sample_rate: 0.0,
        });
        // Under H1 (q = 0.1), ln(LR(x=1)) = ln(1000) ≈ 6.9. After 3
        // conflicts log_e ≈ 20.7, far above ln(1000) ≈ 6.9.
        for _ in 0..3 {
            gate.observe(true);
        }
        assert!(gate.is_alert(), "snapshot={}", gate.snapshot());
        assert_eq!(gate.alert_state(), GateAlertState::Alert);
        assert!(!gate.should_skip_ssi(1));
        assert!(gate.alert_count() >= 1);
    }

    #[test]
    fn reset_clears_state() {
        let mut gate = SsiEProcessGate::new(fast_open_config());
        // 8 observations (min_observations) then a few conflicts to
        // actually trigger alert state.
        for _ in 0..8 {
            gate.observe(false);
        }
        for _ in 0..3 {
            gate.observe(true);
        }
        assert!(gate.is_alert(), "snap={}", gate.snapshot());
        gate.reset();
        assert!(!gate.is_alert());
        assert_eq!(gate.observations(), 0);
        assert!((gate.e_value() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn periodic_sampling_forces_audit() {
        let mut gate = SsiEProcessGate::new(SsiEProcessConfig {
            periodic_sample_rate: 0.5, // stride = 2
            ..fast_open_config()
        });
        for _ in 0..8 {
            gate.observe(false);
        }
        // Even hashes (0, 2, 4, ...) force an audit (no skip).
        assert!(!gate.should_skip_ssi(0));
        assert!(!gate.should_skip_ssi(2));
        // Odd hashes allow skipping.
        assert!(gate.should_skip_ssi(1));
        assert!(gate.should_skip_ssi(3));
    }

    #[test]
    fn supermartingale_under_null_stays_bounded() {
        // Under H0 (true rate = p0), 1000 clean observations should not
        // push the e-value above 1: every clean sample carries a
        // negative log-LR because q > p0.
        let mut gate = SsiEProcessGate::new(SsiEProcessConfig {
            alpha: 1e-3,
            p0: 1e-3,
            alt_mult: 100.0,
            min_observations: 1,
            min_clean_streak: 1,
            periodic_sample_rate: 0.0,
        });
        for _ in 0..1000 {
            gate.observe(false);
        }
        assert!(gate.e_value() <= 1.0, "e={}", gate.e_value());
        assert!(!gate.is_alert());
    }

    #[test]
    fn snapshot_is_displayable() {
        let mut gate = SsiEProcessGate::new(fast_open_config());
        gate.observe(false);
        let snap = gate.snapshot();
        let s = format!("{snap}");
        assert!(s.contains("SsiEProcessGate"));
        assert!(s.contains("obs=1"));
    }

    #[test]
    fn gate_alert_state_display() {
        assert_eq!(format!("{}", GateAlertState::Clear), "clear");
        assert_eq!(format!("{}", GateAlertState::Watching), "watching");
        assert_eq!(format!("{}", GateAlertState::Alert), "ALERT");
    }

    #[test]
    #[ignore = "microbench: run with `cargo test --profile release-perf -- --ignored bench_gate_hot_path`"]
    fn bench_gate_hot_path() {
        // Hot-path microbenchmark. Not a correctness test. Reports
        // nanoseconds per `observe` and per `should_skip_ssi` call to
        // gauge the overhead a LAB_UNSAFE gate consultation adds to
        // the commit path.
        let n: u64 = 1_000_000;
        let mut gate = SsiEProcessGate::new(SsiEProcessConfig::default());

        let t0 = std::time::Instant::now();
        for i in 0..n {
            gate.observe(i % 10000 == 0); // 0.01% conflict rate
        }
        // Prevent the compiler from DCE'ing the loop: consume the
        // observable state via a forced read.
        let sentinel = gate.e_value() + gate.observations() as f64;
        assert!(sentinel.is_finite());
        let obs_ns = {
            #[allow(clippy::cast_precision_loss)]
            let elapsed = t0.elapsed().as_nanos() as f64;
            #[allow(clippy::cast_precision_loss)]
            let denom = n as f64;
            elapsed / denom
        };
        println!("observe(): {obs_ns:.1} ns/call");

        gate.reset();
        for _ in 0..1024 {
            gate.observe(false);
        }

        let t0 = std::time::Instant::now();
        let mut grants = 0u64;
        for i in 0..n {
            if gate.should_skip_ssi(i) {
                grants += 1;
            }
        }
        let skip_ns = {
            #[allow(clippy::cast_precision_loss)]
            let elapsed = t0.elapsed().as_nanos() as f64;
            #[allow(clippy::cast_precision_loss)]
            let denom = n as f64;
            elapsed / denom
        };
        println!("should_skip_ssi(): {skip_ns:.1} ns/call, grants={grants}/{n}");
        println!("snapshot: {}", gate.snapshot());
    }

    #[test]
    fn with_alpha_validates() {
        let cfg = SsiEProcessConfig::default();
        assert!(cfg.with_alpha(0.5).is_some());
        assert!(SsiEProcessConfig::default().with_alpha(-1.0).is_none());
        assert!(SsiEProcessConfig::default().with_alpha(1.0).is_none());
        assert!(SsiEProcessConfig::default().with_alpha(f64::NAN).is_none());
    }
}
