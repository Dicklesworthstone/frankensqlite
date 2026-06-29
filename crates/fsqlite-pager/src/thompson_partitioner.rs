//! Thompson-sampled cache partitioner primitive (IMPL-11 / AAC-P9).
//!
//! Maintains a Bayesian posterior (Beta distribution) per candidate
//! hot-partition ratio and uses Thompson sampling to pick the current arm.
//! Wired into the page cache via `PageCacheEvictionPolicy::S3FifoAdaptive`
//! (see `crate::page_cache`), which uses [`ThompsonPartitioner::current_hot_ratio`]
//! to tune the S3-FIFO small/main split online from cache hit/miss feedback.
//!
//! Sampling details:
//! - Beta(alpha, beta) drawn via two Gamma draws (`X / (X + Y)`).
//! - Gamma drawn via Marsaglia-Tsang with Johnk-style boost for shape < 1.
//! - Uniform and normal variates derived from an inline SplitMix64 PRNG
//!   seeded from `access_count` so that tests are deterministic.
//!
//! No external crates, no unsafe.

use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// How often (in accesses) a `tick` call triggers a resample.
pub const RESAMPLE_INTERVAL: u64 = 10_000;

/// A single Beta-distributed arm over a candidate hot-partition ratio.
#[derive(Debug)]
pub struct BetaArm {
    /// Successes + 1 prior. Stored as `u64` bits to allow atomic updates.
    pub alpha: AtomicU64,
    /// Failures + 1 prior.
    pub beta: AtomicU64,
    /// Candidate hot-partition ratio this arm represents.
    pub arm_ratio: f64,
}

impl BetaArm {
    /// Create a new arm with uniform Beta(1, 1) prior.
    #[must_use]
    pub fn new(arm_ratio: f64) -> Self {
        Self {
            alpha: AtomicU64::new(1),
            beta: AtomicU64::new(1),
            arm_ratio,
        }
    }
}

impl Clone for BetaArm {
    fn clone(&self) -> Self {
        Self {
            alpha: AtomicU64::new(self.alpha.load(Ordering::Relaxed)),
            beta: AtomicU64::new(self.beta.load(Ordering::Relaxed)),
            arm_ratio: self.arm_ratio,
        }
    }
}

/// Thompson-sampled partitioner over a fixed grid of hot-partition ratios.
#[derive(Debug)]
pub struct ThompsonPartitioner {
    arms: Vec<BetaArm>,
    current_arm: AtomicUsize,
    access_count: AtomicU64,
}

impl Clone for ThompsonPartitioner {
    fn clone(&self) -> Self {
        Self {
            arms: self.arms.clone(),
            current_arm: AtomicUsize::new(self.current_arm.load(Ordering::Relaxed)),
            access_count: AtomicU64::new(self.access_count.load(Ordering::Relaxed)),
        }
    }
}

impl Default for ThompsonPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl ThompsonPartitioner {
    /// Construct a partitioner with 9 arms at ratios `0.1..=0.9` in `0.1`
    /// increments. The default current arm is the middle one (0.5).
    #[must_use]
    pub fn new() -> Self {
        let arms: Vec<BetaArm> = (1..=9).map(|i| BetaArm::new(f64::from(i) / 10.0)).collect();
        // Middle arm index: 4 for 9 arms (ratio 0.5).
        Self {
            arms,
            current_arm: AtomicUsize::new(4),
            access_count: AtomicU64::new(0),
        }
    }

    /// Return the hot-partition ratio of the currently selected arm.
    #[must_use]
    pub fn current_hot_ratio(&self) -> f64 {
        let idx = self.current_arm.load(Ordering::Relaxed);
        self.arms[idx].arm_ratio
    }

    /// Record the outcome of a cache access against the currently selected
    /// arm. `hot_hit` means the access landed on the hot partition (success).
    pub fn record_outcome(&self, hot_hit: bool) {
        let idx = self.current_arm.load(Ordering::Relaxed);
        let arm = &self.arms[idx];
        if hot_hit {
            arm.alpha.fetch_add(1, Ordering::Relaxed);
        } else {
            arm.beta.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Increment the access counter. When the counter crosses a multiple of
    /// `RESAMPLE_INTERVAL`, resample and return `true`; otherwise `false`.
    pub fn tick(&self) -> bool {
        let prev = self.access_count.fetch_add(1, Ordering::Relaxed);
        let count = prev.wrapping_add(1);
        if count % RESAMPLE_INTERVAL == 0 {
            self.resample();
            true
        } else {
            false
        }
    }

    /// For each arm, draw a sample from its current Beta posterior and pick
    /// the argmax as the new `current_arm`.
    pub fn resample(&self) {
        // Deterministic seed from access_count so tests are reproducible.
        let seed = self
            .access_count
            .load(Ordering::Relaxed)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(0xD1B5_4A32_D192_ED03);
        let mut rng = SplitMix64::new(seed);

        let mut best_idx = 0usize;
        let mut best_sample = f64::NEG_INFINITY;
        for (idx, arm) in self.arms.iter().enumerate() {
            let a = arm.alpha.load(Ordering::Relaxed) as f64;
            let b = arm.beta.load(Ordering::Relaxed) as f64;
            let sample = sample_beta(&mut rng, a, b);
            if sample > best_sample {
                best_sample = sample;
                best_idx = idx;
            }
        }
        self.current_arm.store(best_idx, Ordering::Relaxed);
    }

    /// Number of arms. Useful for tests.
    #[must_use]
    pub fn arm_count(&self) -> usize {
        self.arms.len()
    }

    /// Access to underlying arms. Read-only view for tests/introspection.
    #[must_use]
    pub fn arms(&self) -> &[BetaArm] {
        &self.arms
    }

    /// Index of the currently selected arm.
    #[must_use]
    pub fn current_arm_index(&self) -> usize {
        self.current_arm.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// PRNG + distribution helpers
// ---------------------------------------------------------------------------

/// Minimal SplitMix64 PRNG. Not cryptographically secure.
#[derive(Debug, Clone)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        // Avoid degenerate all-zero state.
        let state = if seed == 0 {
            0xDEAD_BEEF_DEAD_BEEF
        } else {
            seed
        };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform `f64` in the half-open interval `(0, 1)`.
    fn next_f64_open(&mut self) -> f64 {
        // Use 53 random bits; then nudge away from exact zero.
        let bits = self.next_u64() >> 11; // 53 bits
        let x = (bits as f64) * (1.0f64 / ((1u64 << 53) as f64));
        if x <= 0.0 { f64::MIN_POSITIVE } else { x }
    }

    /// Standard normal via Box-Muller (polar form).
    fn next_normal(&mut self) -> f64 {
        loop {
            let u1 = 2.0_f64.mul_add(self.next_f64_open(), -1.0);
            let u2 = 2.0_f64.mul_add(self.next_f64_open(), -1.0);
            let s = u1.mul_add(u1, u2 * u2);
            if s > 0.0 && s < 1.0 {
                let factor = (-2.0 * s.ln() / s).sqrt();
                return u1 * factor;
            }
        }
    }
}

/// Draw from a Gamma(shape, 1) distribution using Marsaglia-Tsang for
/// `shape >= 1` and the boost trick for `shape < 1`.
#[allow(clippy::cast_precision_loss)]
fn sample_gamma(rng: &mut SplitMix64, shape: f64) -> f64 {
    debug_assert!(shape > 0.0);
    if shape < 1.0 {
        // Boost: Gamma(k) = Gamma(k+1) * U^(1/k).
        let g = sample_gamma(rng, shape + 1.0);
        let u = rng.next_f64_open();
        return g * u.powf(1.0 / shape);
    }
    // Marsaglia-Tsang (shape >= 1).
    let d = shape - 1.0 / 3.0;
    let c = 1.0 / (9.0 * d).sqrt();
    loop {
        let x = rng.next_normal();
        let v_base = 1.0 + c * x;
        if v_base <= 0.0 {
            continue;
        }
        let v = v_base * v_base * v_base;
        let u = rng.next_f64_open();
        let x2 = x * x;
        // Squeeze step.
        if u < (0.0331 * x2).mul_add(-x2, 1.0) {
            return d * v;
        }
        // Full acceptance.
        if u.ln() < 0.5_f64.mul_add(x2, d * (1.0 - v + v.ln())) {
            return d * v;
        }
    }
}

/// Draw from Beta(alpha, beta) via two Gamma draws.
fn sample_beta(rng: &mut SplitMix64, alpha: f64, beta: f64) -> f64 {
    let x = sample_gamma(rng, alpha);
    let y = sample_gamma(rng, beta);
    let denom = x + y;
    if denom <= 0.0 {
        // Fall back to uniform if both underflow (pathological).
        return rng.next_f64_open();
    }
    x / denom
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitioner_has_nine_arms_with_expected_ratios() {
        let p = ThompsonPartitioner::new();
        assert_eq!(p.arm_count(), 9);
        for (i, arm) in p.arms().iter().enumerate() {
            let expected = f64::from(u32::try_from(i + 1).unwrap()) / 10.0;
            assert!(
                (arm.arm_ratio - expected).abs() < 1e-9,
                "arm {i} ratio = {} expected {expected}",
                arm.arm_ratio
            );
            assert_eq!(arm.alpha.load(Ordering::Relaxed), 1);
            assert_eq!(arm.beta.load(Ordering::Relaxed), 1);
        }
        // Middle arm (0.5) is the default.
        assert!((p.current_hot_ratio() - 0.5).abs() < 1e-9);
        assert_eq!(p.current_arm_index(), 4);
    }

    #[test]
    fn heavily_rewarded_arm_is_selected_after_resample() {
        let p = ThompsonPartitioner::new();
        // Force current arm to the 0.5 arm (already default, but be explicit).
        p.current_arm.store(4, Ordering::Relaxed);
        for _ in 0..1_000 {
            p.record_outcome(true);
        }
        // Alpha of arm 4 should now be 1 + 1000 = 1001; others untouched.
        assert_eq!(p.arms()[4].alpha.load(Ordering::Relaxed), 1_001);
        for (i, arm) in p.arms().iter().enumerate() {
            if i == 4 {
                continue;
            }
            assert_eq!(arm.alpha.load(Ordering::Relaxed), 1);
            assert_eq!(arm.beta.load(Ordering::Relaxed), 1);
        }
        p.resample();
        // With a Beta(1001, 1) vs Beta(1, 1) posterior on others, arm 4
        // overwhelmingly dominates Thompson sampling.
        assert_eq!(p.current_arm_index(), 4);
        assert!((p.current_hot_ratio() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn uniform_priors_produce_valid_arm_index() {
        let p = ThompsonPartitioner::new();
        // All arms at alpha=beta=1 (uniform). Resample must pick a valid index.
        p.resample();
        let idx = p.current_arm_index();
        assert!(idx < p.arm_count(), "idx {idx} out of range");
        let ratio = p.current_hot_ratio();
        assert!(
            (0.1..=0.9).contains(&ratio),
            "ratio {ratio} not in [0.1, 0.9]"
        );
    }

    #[test]
    fn tick_resamples_every_interval() {
        let p = ThompsonPartitioner::new();
        // Reward arm index 8 (ratio 0.9) so resample is likely to pick it.
        p.current_arm.store(8, Ordering::Relaxed);
        for _ in 0..5_000 {
            p.record_outcome(true);
        }
        // Most ticks return false.
        for _ in 0..(RESAMPLE_INTERVAL as usize - 1) {
            assert!(!p.tick());
        }
        // Exactly on the boundary, tick triggers resample.
        assert!(p.tick());
        assert_eq!(p.current_arm_index(), 8);
    }

    #[test]
    fn gamma_samples_are_positive_and_finite() {
        let mut rng = SplitMix64::new(42);
        for shape in [0.25_f64, 0.5, 1.0, 1.5, 5.0, 50.0] {
            for _ in 0..32 {
                let g = sample_gamma(&mut rng, shape);
                assert!(g.is_finite() && g > 0.0, "gamma({shape}) = {g}");
            }
        }
    }

    #[test]
    fn beta_samples_are_in_unit_interval() {
        let mut rng = SplitMix64::new(12345);
        for (a, b) in [(1.0_f64, 1.0), (2.0, 5.0), (100.0, 1.0), (1.0, 100.0)] {
            for _ in 0..64 {
                let s = sample_beta(&mut rng, a, b);
                assert!(
                    s.is_finite() && (0.0..=1.0).contains(&s),
                    "beta({a},{b}) = {s}"
                );
            }
        }
    }

    #[test]
    fn record_outcome_miss_increments_beta() {
        let p = ThompsonPartitioner::new();
        let idx = p.current_arm_index();
        let beta_before = p.arms()[idx].beta.load(Ordering::Relaxed);
        let alpha_before = p.arms()[idx].alpha.load(Ordering::Relaxed);

        for _ in 0..50 {
            p.record_outcome(false);
        }

        assert_eq!(p.arms()[idx].beta.load(Ordering::Relaxed), beta_before + 50);
        assert_eq!(p.arms()[idx].alpha.load(Ordering::Relaxed), alpha_before);
    }

    #[test]
    fn default_trait_matches_new() {
        let d = ThompsonPartitioner::default();
        let n = ThompsonPartitioner::new();
        assert_eq!(d.arm_count(), n.arm_count());
        assert_eq!(d.current_arm_index(), n.current_arm_index());
        assert!((d.current_hot_ratio() - n.current_hot_ratio()).abs() < 1e-9);
    }

    #[test]
    fn resample_is_deterministic_for_same_access_count() {
        let p1 = ThompsonPartitioner::new();
        let p2 = ThompsonPartitioner::new();

        for _ in 0..100 {
            p1.record_outcome(true);
            p2.record_outcome(true);
        }
        p1.access_count.store(42, Ordering::Relaxed);
        p2.access_count.store(42, Ordering::Relaxed);

        p1.resample();
        p2.resample();
        assert_eq!(p1.current_arm_index(), p2.current_arm_index());
    }

    #[test]
    fn splitmix64_zero_seed_avoids_degenerate_state() {
        let mut rng = SplitMix64::new(0);
        let mut all_zero = true;
        for _ in 0..10 {
            if rng.next_u64() != 0 {
                all_zero = false;
                break;
            }
        }
        assert!(!all_zero, "zero-seeded PRNG must not produce all zeros");
    }

    #[test]
    fn penalized_arm_loses_to_rewarded_arm() {
        let p = ThompsonPartitioner::new();
        p.current_arm.store(0, Ordering::Relaxed);
        for _ in 0..500 {
            p.record_outcome(false);
        }
        p.current_arm.store(8, Ordering::Relaxed);
        for _ in 0..500 {
            p.record_outcome(true);
        }

        p.resample();
        assert_eq!(
            p.current_arm_index(),
            8,
            "arm 8 (heavily rewarded) must beat arm 0 (heavily penalized)"
        );
    }

    #[test]
    fn beta_arm_new_initializes_uniform_prior() {
        let arm = BetaArm::new(0.42);
        assert!((arm.arm_ratio - 0.42).abs() < f64::EPSILON);
        assert_eq!(arm.alpha.load(Ordering::Relaxed), 1);
        assert_eq!(arm.beta.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn splitmix64_next_f64_open_stays_in_unit_interval() {
        let mut rng = SplitMix64::new(0xCAFE);
        for i in 0..1_000 {
            let v = rng.next_f64_open();
            assert!(v > 0.0 && v < 1.0, "sample {i}: {v} not in (0,1)");
        }
    }

    #[test]
    fn splitmix64_next_normal_mean_near_zero() {
        let mut rng = SplitMix64::new(999);
        let n = 10_000;
        let sum: f64 = (0..n).map(|_| rng.next_normal()).sum();
        let mean = sum / n as f64;
        assert!(
            mean.abs() < 0.1,
            "normal mean {mean} too far from 0 over {n} samples"
        );
    }

    #[test]
    fn tick_returns_false_on_first_call() {
        let p = ThompsonPartitioner::new();
        assert!(!p.tick(), "first tick must not trigger resample");
    }

    #[test]
    fn beta_arm_debug_contains_fields() {
        let arm = BetaArm::new(0.75);
        arm.alpha.store(10, Ordering::Relaxed);
        arm.beta.store(20, Ordering::Relaxed);
        let dbg = format!("{arm:?}");
        assert!(dbg.contains("BetaArm"));
        assert!(dbg.contains("alpha"));
        assert!(dbg.contains("beta"));
        assert!(dbg.contains("arm_ratio"));
        assert!(dbg.contains("0.75"));
    }

    #[test]
    fn thompson_partitioner_debug_contains_fields() {
        let p = ThompsonPartitioner::new();
        let dbg = format!("{p:?}");
        assert!(dbg.contains("ThompsonPartitioner"));
        assert!(dbg.contains("arms"));
        assert!(dbg.contains("current_arm"));
        assert!(dbg.contains("access_count"));
    }

    #[test]
    fn splitmix64_clone_produces_independent_stream() {
        let mut rng1 = SplitMix64::new(0xBEEF);
        let _ = rng1.next_u64();
        let mut rng2 = rng1.clone();
        let a = rng1.next_u64();
        let b = rng2.next_u64();
        assert_eq!(a, b, "cloned PRNG must produce same next value");
        let _ = rng1.next_u64();
        let c = rng1.next_u64();
        let d = rng2.next_u64();
        assert_ne!(c, d, "diverged PRNGs must produce different values");
    }

    #[test]
    fn resample_interval_constant_is_ten_thousand() {
        assert_eq!(RESAMPLE_INTERVAL, 10_000);
    }

    #[test]
    fn beta_arm_new_starts_with_uniform_prior() {
        let arm = BetaArm::new(0.3);
        assert_eq!(arm.alpha.load(Ordering::Relaxed), 1);
        assert_eq!(arm.beta.load(Ordering::Relaxed), 1);
        assert!((arm.arm_ratio - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn thompson_partitioner_default_starts_at_middle_arm() {
        let tp = ThompsonPartitioner::default();
        let ratio = tp.current_hot_ratio();
        assert!((ratio - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn record_outcome_updates_alpha_beta() {
        let tp = ThompsonPartitioner::new();
        let idx = tp.current_arm.load(Ordering::Relaxed);
        let a_before = tp.arms[idx].alpha.load(Ordering::Relaxed);
        let b_before = tp.arms[idx].beta.load(Ordering::Relaxed);
        tp.record_outcome(true);
        tp.record_outcome(false);
        tp.record_outcome(true);
        assert_eq!(tp.arms[idx].alpha.load(Ordering::Relaxed), a_before + 2);
        assert_eq!(tp.arms[idx].beta.load(Ordering::Relaxed), b_before + 1);
    }

    #[test]
    fn tick_returns_false_below_resample_interval() {
        let tp = ThompsonPartitioner::new();
        for _ in 0..100 {
            assert!(!tp.tick());
        }
        assert_eq!(tp.access_count.load(Ordering::Relaxed), 100);
    }
}
