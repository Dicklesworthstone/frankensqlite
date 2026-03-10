//! Lightweight e-process oracle for statistical query shedding.
//!
//! An e-process is a non-negative supermartingale under H₀ with E₀ = 1.
//! When the running e-value exceeds 1/α we reject H₀ (anomaly detected).
//! This module provides a self-contained implementation used by [`Cx`] to
//! shed low-priority queries when anomaly pressure is high.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Configuration for the e-process martingale.
#[derive(Debug, Clone, Copy)]
pub struct EProcessConfig {
    /// Null hypothesis anomaly rate bound.
    pub p0: f64,
    /// Betting parameter in `E_{t+1} = E_t * (1 + lambda * (x_t - p0))`.
    pub lambda: f64,
    /// Significance level (reject when e-value >= 1/alpha).
    pub alpha: f64,
    /// Cap on e-value to prevent overflow.
    pub max_evalue: f64,
}

/// Weighted health telemetry feeding the e-process update.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EProcessSignal {
    /// First-committer-wins abort rate or equivalent write-conflict signal.
    pub fcw_abort_rate: f64,
    /// Pager/cache miss ratio on the `[0, 1]` interval.
    pub cache_miss_ratio: f64,
    /// Memory or cache pressure on the `[0, 1]` interval.
    pub memory_pressure: f64,
    /// Final anomaly score used to classify the observation.
    pub anomaly_score: f64,
}

impl EProcessSignal {
    /// Create a signal from raw telemetry, using the connection default weights.
    #[must_use]
    pub fn new(fcw_abort_rate: f64, cache_miss_ratio: f64, memory_pressure: f64) -> Self {
        let fcw_abort_rate = sanitize_unit_interval(fcw_abort_rate);
        let cache_miss_ratio = sanitize_unit_interval(cache_miss_ratio);
        let memory_pressure = sanitize_unit_interval(memory_pressure);
        let anomaly_score =
            fcw_abort_rate.mul_add(0.5, cache_miss_ratio.mul_add(0.3, memory_pressure * 0.2));
        Self {
            fcw_abort_rate,
            cache_miss_ratio,
            memory_pressure,
            anomaly_score: sanitize_unit_interval(anomaly_score),
        }
    }

    /// Override the derived anomaly score with an externally computed one.
    #[must_use]
    pub fn with_anomaly_score(mut self, anomaly_score: f64) -> Self {
        self.anomaly_score = sanitize_unit_interval(anomaly_score);
        self
    }

    #[must_use]
    fn is_anomalous(self) -> bool {
        self.anomaly_score >= 0.5
    }
}

/// Snapshot of oracle state for diagnostics.
#[derive(Debug, Clone, PartialEq)]
pub struct EProcessSnapshot {
    /// Current e-value (encoded as f64 bits).
    pub evalue: f64,
    /// Total observations processed.
    pub observations: u64,
    /// Current rejection threshold (`1 / alpha`).
    pub rejection_threshold: f64,
    /// Priority must be strictly greater than this threshold to be shed.
    pub priority_threshold: u8,
    /// Most recent telemetry signal, when one was recorded.
    pub last_signal: Option<EProcessSignal>,
}

/// Decision artifact captured when a checkpoint consults the oracle.
#[derive(Debug, Clone, PartialEq)]
pub struct EProcessDecision {
    /// Snapshot used to make the decision.
    pub snapshot: EProcessSnapshot,
    /// Priority level of the checked context.
    pub priority: u8,
    /// Whether the oracle recommends shedding the checked context.
    pub should_shed: bool,
}

/// Lock-free bridge for publishing e-process telemetry from runtime code.
#[derive(Debug, Default)]
pub struct EProcessTelemetryBridge {
    signal_present: AtomicBool,
    fcw_abort_rate_bits: AtomicU64,
    cache_miss_ratio_bits: AtomicU64,
    memory_pressure_bits: AtomicU64,
    anomaly_score_bits: AtomicU64,
}

impl EProcessTelemetryBridge {
    /// Create an empty bridge.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish a fully materialized signal.
    pub fn record_signal(&self, signal: EProcessSignal) {
        self.fcw_abort_rate_bits
            .store(signal.fcw_abort_rate.to_bits(), Ordering::Relaxed);
        self.cache_miss_ratio_bits
            .store(signal.cache_miss_ratio.to_bits(), Ordering::Relaxed);
        self.memory_pressure_bits
            .store(signal.memory_pressure.to_bits(), Ordering::Relaxed);
        self.anomaly_score_bits
            .store(signal.anomaly_score.to_bits(), Ordering::Relaxed);
        self.signal_present.store(true, Ordering::Release);
    }

    /// Publish telemetry components using the default anomaly-score weights.
    pub fn record_components(
        &self,
        fcw_abort_rate: f64,
        cache_miss_ratio: f64,
        memory_pressure: f64,
    ) {
        self.record_signal(EProcessSignal::new(
            fcw_abort_rate,
            cache_miss_ratio,
            memory_pressure,
        ));
    }

    /// Read the most recently published signal.
    #[must_use]
    pub fn snapshot(&self) -> Option<EProcessSignal> {
        if !self.signal_present.load(Ordering::Acquire) {
            return None;
        }
        Some(EProcessSignal {
            fcw_abort_rate: f64::from_bits(self.fcw_abort_rate_bits.load(Ordering::Relaxed)),
            cache_miss_ratio: f64::from_bits(self.cache_miss_ratio_bits.load(Ordering::Relaxed)),
            memory_pressure: f64::from_bits(self.memory_pressure_bits.load(Ordering::Relaxed)),
            anomaly_score: f64::from_bits(self.anomaly_score_bits.load(Ordering::Relaxed)),
        })
    }
}

/// Anytime-valid anomaly oracle that signals when to shed low-priority work.
///
/// Thread-safe: all state is stored in atomics.
#[derive(Debug)]
pub struct EProcessOracle {
    config: EProcessConfig,
    /// Priority threshold: only shed contexts with priority strictly above this value.
    priority_threshold: u8,
    /// Running e-value stored as f64 bits in an AtomicU64.
    evalue_bits: AtomicU64,
    /// Total observation count.
    observations: AtomicU64,
    /// Most recent telemetry signal, when supplied.
    signal_present: AtomicBool,
    fcw_abort_rate_bits: AtomicU64,
    cache_miss_ratio_bits: AtomicU64,
    memory_pressure_bits: AtomicU64,
    anomaly_score_bits: AtomicU64,
}

impl EProcessOracle {
    /// Create a new oracle.
    #[must_use]
    pub fn new(config: EProcessConfig, priority_threshold: u8) -> Self {
        let config = sanitize_config(config);
        Self {
            config,
            priority_threshold,
            evalue_bits: AtomicU64::new(1.0_f64.to_bits()),
            observations: AtomicU64::new(0),
            signal_present: AtomicBool::new(false),
            fcw_abort_rate_bits: AtomicU64::new(0.0_f64.to_bits()),
            cache_miss_ratio_bits: AtomicU64::new(0.0_f64.to_bits()),
            memory_pressure_bits: AtomicU64::new(0.0_f64.to_bits()),
            anomaly_score_bits: AtomicU64::new(0.0_f64.to_bits()),
        }
    }

    /// Record an observation. `anomaly = true` means an anomaly was observed.
    pub fn observe_sample(&self, anomaly: bool) {
        self.observations.fetch_add(1, Ordering::Relaxed);

        // Betting-martingale update (anytime-valid under H0):
        //   E_{t+1} = E_t * (1 + lambda * (x_t - p0))
        // where x_t = 1 for anomaly and 0 for normal.
        let x_t = if anomaly { 1.0 } else { 0.0 };
        let factor = self
            .config
            .lambda
            .mul_add(x_t - self.config.p0, 1.0)
            .max(0.0);

        // Atomic CAS loop to update the e-value.
        loop {
            let old_bits = self.evalue_bits.load(Ordering::Relaxed);
            let old_val = f64::from_bits(old_bits);
            let mut new_val = old_val * factor;
            if !new_val.is_finite() {
                new_val = self.config.max_evalue;
            }
            new_val = new_val.min(self.config.max_evalue).max(0.0);
            let new_bits = new_val.to_bits();
            if self
                .evalue_bits
                .compare_exchange_weak(old_bits, new_bits, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Record a full telemetry signal and update the e-process classification.
    pub fn observe_signal(&self, signal: EProcessSignal) {
        self.store_signal(signal);
        self.observe_sample(signal.is_anomalous());
    }

    /// Record the latest signal published into a telemetry bridge.
    ///
    /// Returns `true` when a signal was present and consumed.
    pub fn observe_bridge(&self, bridge: &EProcessTelemetryBridge) -> bool {
        let Some(signal) = bridge.snapshot() else {
            return false;
        };
        self.observe_signal(signal);
        true
    }

    /// Returns `true` if the oracle recommends shedding a context at the given
    /// priority level. Only sheds when priority > threshold AND e-value >= 1/alpha.
    #[must_use]
    pub fn should_shed(&self, priority: u8) -> bool {
        if priority <= self.priority_threshold {
            return false;
        }
        let evalue = f64::from_bits(self.evalue_bits.load(Ordering::Acquire));
        evalue >= self.rejection_threshold()
    }

    /// Build a decision artifact for a specific priority level.
    #[must_use]
    pub fn decision(&self, priority: u8) -> EProcessDecision {
        let snapshot = self.snapshot();
        let should_shed = priority > snapshot.priority_threshold
            && snapshot.evalue >= snapshot.rejection_threshold;
        EProcessDecision {
            snapshot,
            priority,
            should_shed,
        }
    }

    /// Current e-value for the oracle.
    #[must_use]
    pub fn e_value(&self) -> f64 {
        f64::from_bits(self.evalue_bits.load(Ordering::Acquire))
    }

    /// Rejection threshold `1/alpha` for the current oracle configuration.
    #[must_use]
    pub fn rejection_threshold(&self) -> f64 {
        1.0 / self.config.alpha
    }

    /// Priority threshold for shedding.
    #[must_use]
    pub const fn priority_threshold(&self) -> u8 {
        self.priority_threshold
    }

    /// Snapshot current oracle state.
    #[must_use]
    pub fn snapshot(&self) -> EProcessSnapshot {
        EProcessSnapshot {
            evalue: self.e_value(),
            observations: self.observations.load(Ordering::Relaxed),
            rejection_threshold: self.rejection_threshold(),
            priority_threshold: self.priority_threshold,
            last_signal: self.last_signal(),
        }
    }

    fn store_signal(&self, signal: EProcessSignal) {
        self.fcw_abort_rate_bits
            .store(signal.fcw_abort_rate.to_bits(), Ordering::Relaxed);
        self.cache_miss_ratio_bits
            .store(signal.cache_miss_ratio.to_bits(), Ordering::Relaxed);
        self.memory_pressure_bits
            .store(signal.memory_pressure.to_bits(), Ordering::Relaxed);
        self.anomaly_score_bits
            .store(signal.anomaly_score.to_bits(), Ordering::Relaxed);
        self.signal_present.store(true, Ordering::Release);
    }

    #[must_use]
    fn last_signal(&self) -> Option<EProcessSignal> {
        if !self.signal_present.load(Ordering::Acquire) {
            return None;
        }
        Some(EProcessSignal {
            fcw_abort_rate: f64::from_bits(self.fcw_abort_rate_bits.load(Ordering::Relaxed)),
            cache_miss_ratio: f64::from_bits(self.cache_miss_ratio_bits.load(Ordering::Relaxed)),
            memory_pressure: f64::from_bits(self.memory_pressure_bits.load(Ordering::Relaxed)),
            anomaly_score: f64::from_bits(self.anomaly_score_bits.load(Ordering::Relaxed)),
        })
    }
}

fn sanitize_config(mut config: EProcessConfig) -> EProcessConfig {
    const EPS: f64 = 1e-9;

    if !config.p0.is_finite() {
        config.p0 = 0.1;
    }
    config.p0 = config.p0.clamp(EPS, 1.0 - EPS);

    if !config.alpha.is_finite() || config.alpha <= 0.0 {
        config.alpha = 0.05;
    }
    config.alpha = config.alpha.clamp(EPS, 1.0);

    if !config.max_evalue.is_finite() || config.max_evalue < 1.0 {
        config.max_evalue = 1.0;
    }

    let lambda_min = -1.0 / (1.0 - config.p0) + EPS;
    let lambda_max = 1.0 / config.p0 - EPS;
    if !config.lambda.is_finite() {
        config.lambda = 0.0;
    }
    config.lambda = config.lambda.clamp(lambda_min, lambda_max);

    config
}

fn sanitize_unit_interval(value: f64) -> f64 {
    if !value.is_finite() {
        return 0.0;
    }
    value.clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lcg_next(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *state
    }

    fn bernoulli_sample(state: &mut u64, p: f64) -> bool {
        let raw = (lcg_next(state) >> 11) as f64 / ((1_u64 << 53) as f64);
        raw < p
    }

    fn test_config() -> EProcessConfig {
        EProcessConfig {
            p0: 0.1,
            lambda: 5.0,
            alpha: 0.05,
            max_evalue: 1e12,
        }
    }

    #[test]
    fn eprocess_threshold_crossing_triggers_shed() {
        let oracle = EProcessOracle::new(test_config(), 1);
        oracle.observe_sample(true);
        oracle.observe_sample(true);
        let snapshot = oracle.snapshot();
        assert!(snapshot.evalue >= oracle.rejection_threshold());
        assert_eq!(snapshot.rejection_threshold, oracle.rejection_threshold());
        assert_eq!(snapshot.priority_threshold, 1);
        assert!(oracle.should_shed(3));
    }

    #[test]
    fn eprocess_priority_threshold_blocks_shed() {
        let oracle = EProcessOracle::new(test_config(), 1);
        oracle.observe_sample(true);
        oracle.observe_sample(true);
        assert!(!oracle.should_shed(1));
    }

    #[test]
    fn eprocess_healthy_stream_does_not_false_alarm() {
        let oracle = EProcessOracle::new(
            EProcessConfig {
                p0: 0.1,
                lambda: 0.5,
                alpha: 0.01,
                max_evalue: 1e12,
            },
            0,
        );

        for _ in 0..500 {
            oracle.observe_sample(false);
        }

        let snapshot = oracle.snapshot();
        assert!(snapshot.evalue < oracle.rejection_threshold());
        assert!(!oracle.should_shed(2));
    }

    #[test]
    fn eprocess_null_rate_stream_stays_below_threshold() {
        let oracle = EProcessOracle::new(
            EProcessConfig {
                p0: 0.1,
                lambda: 0.5,
                alpha: 0.01,
                max_evalue: 1e12,
            },
            0,
        );

        let mut state = 0x5eed_u64;
        for _ in 0..2_000 {
            let anomaly = bernoulli_sample(&mut state, 0.02);
            oracle.observe_sample(anomaly);
        }

        assert!(oracle.snapshot().evalue < oracle.rejection_threshold());
    }

    #[test]
    fn eprocess_snapshot_tracks_observations() {
        let oracle = EProcessOracle::new(test_config(), 1);
        oracle.observe_sample(true);
        oracle.observe_sample(false);
        oracle.observe_sample(true);
        assert_eq!(oracle.snapshot().observations, 3);
    }

    #[test]
    fn eprocess_signal_snapshot_records_diagnostics() {
        let oracle = EProcessOracle::new(test_config(), 1);
        let signal = EProcessSignal::new(0.8, 0.5, 0.25);
        oracle.observe_signal(signal);
        let snapshot = oracle.snapshot();
        assert_eq!(snapshot.last_signal, Some(signal));
        assert_eq!(snapshot.priority_threshold, 1);
        assert_eq!(snapshot.rejection_threshold, 20.0);
    }

    #[test]
    fn eprocess_bridge_ingestion_updates_last_signal() {
        let oracle = EProcessOracle::new(test_config(), 1);
        let bridge = EProcessTelemetryBridge::new();
        bridge.record_components(0.9, 0.6, 0.5);
        assert!(oracle.observe_bridge(&bridge));
        let signal = bridge
            .snapshot()
            .expect("bridge should hold the latest signal");
        assert_eq!(oracle.snapshot().last_signal, Some(signal));
    }

    #[test]
    fn eprocess_decision_captures_priority_and_snapshot() {
        let oracle = EProcessOracle::new(test_config(), 1);
        oracle.observe_signal(EProcessSignal::new(1.0, 1.0, 1.0));
        oracle.observe_signal(EProcessSignal::new(1.0, 1.0, 1.0));
        let decision = oracle.decision(3);
        assert_eq!(decision.priority, 3);
        assert!(decision.should_shed);
        assert_eq!(decision.snapshot.priority_threshold, 1);
    }

    #[test]
    fn eprocess_sanitizes_invalid_config() {
        let oracle = EProcessOracle::new(
            EProcessConfig {
                p0: 5.0,
                lambda: f64::INFINITY,
                alpha: 0.0,
                max_evalue: -1.0,
            },
            0,
        );

        // Should remain finite and non-negative after updates.
        oracle.observe_sample(false);
        oracle.observe_sample(true);
        let snapshot = oracle.snapshot();
        assert!(snapshot.evalue.is_finite());
        assert!(snapshot.evalue >= 0.0);
        assert!(oracle.rejection_threshold().is_finite());
    }
}
