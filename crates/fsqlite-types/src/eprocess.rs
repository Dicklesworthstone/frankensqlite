//! Lightweight e-process oracle for statistical query shedding.
//!
//! An e-process is a non-negative supermartingale under H₀ with E₀ = 1.
//! When the running e-value exceeds 1/α we reject H₀ (anomaly detected).
//! This module provides a self-contained implementation used by [`Cx`] to
//! shed low-priority queries when anomaly pressure is high.

use std::sync::atomic::{AtomicU64, Ordering};

/// Configuration for the e-process martingale.
#[derive(Debug, Clone, Copy)]
pub struct EProcessConfig {
    /// Null hypothesis anomaly rate bound.
    pub p0: f64,
    /// GROW mixing weight (controls sensitivity vs. robustness).
    pub lambda: f64,
    /// Significance level (reject when e-value >= 1/alpha).
    pub alpha: f64,
    /// Cap on e-value to prevent overflow.
    pub max_evalue: f64,
}

/// Snapshot of oracle state for diagnostics.
#[derive(Debug, Clone)]
pub struct EProcessSnapshot {
    /// Current e-value (encoded as f64 bits).
    pub evalue: f64,
    /// Total observations processed.
    pub observations: u64,
}

/// Anytime-valid anomaly oracle that signals when to shed low-priority work.
///
/// Thread-safe: all state is stored in atomics.
#[derive(Debug)]
pub struct EProcessOracle {
    config: EProcessConfig,
    /// Priority threshold: only shed contexts with priority >= this value.
    priority_threshold: u8,
    /// Running e-value stored as f64 bits in an AtomicU64.
    evalue_bits: AtomicU64,
    /// Total observation count.
    observations: AtomicU64,
}

impl EProcessOracle {
    /// Create a new oracle.
    #[must_use]
    pub fn new(config: EProcessConfig, priority_threshold: u8) -> Self {
        Self {
            config,
            priority_threshold,
            evalue_bits: AtomicU64::new(1.0_f64.to_bits()),
            observations: AtomicU64::new(0),
        }
    }

    /// Record an observation. `anomaly = true` means an anomaly was observed.
    pub fn observe_sample(&self, anomaly: bool) {
        self.observations.fetch_add(1, Ordering::Relaxed);

        // GROW likelihood ratio update:
        //   E_{t+1} = E_t * (lambda * (x_t / p0) + (1 - lambda))
        // where x_t = 1 for anomaly, 0 for normal.
        let factor = if anomaly {
            self.config.lambda * (1.0 / self.config.p0) + (1.0 - self.config.lambda)
        } else {
            // Under normal observation: multiply by (1 - lambda + lambda * 0/p0) = 1 - lambda
            // But this would make e-value shrink to zero. Correct GROW:
            // factor = lambda * (0 / p0) + (1 - lambda) = 1 - lambda
            1.0 - self.config.lambda
        };

        // Atomic CAS loop to update the e-value.
        loop {
            let old_bits = self.evalue_bits.load(Ordering::Relaxed);
            let old_val = f64::from_bits(old_bits);
            let new_val = (old_val * factor).min(self.config.max_evalue).max(0.0);
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

    /// Returns `true` if the oracle recommends shedding a context at the given
    /// priority level. Only sheds when priority > threshold AND e-value >= 1/alpha.
    #[must_use]
    pub fn should_shed(&self, priority: u8) -> bool {
        if priority <= self.priority_threshold {
            return false;
        }
        let evalue = f64::from_bits(self.evalue_bits.load(Ordering::Acquire));
        evalue >= 1.0 / self.config.alpha
    }

    /// Snapshot current oracle state.
    #[must_use]
    pub fn snapshot(&self) -> EProcessSnapshot {
        EProcessSnapshot {
            evalue: f64::from_bits(self.evalue_bits.load(Ordering::Acquire)),
            observations: self.observations.load(Ordering::Relaxed),
        }
    }
}
