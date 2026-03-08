//! S3-FIFO cache-queue state machine for pager eviction experiments (`bd-1xfri.1`).
//!
//! This module implements a three-queue FIFO policy:
//! - `SMALL`: admission queue
//! - `MAIN`: long-lived queue with bounded reinsertion
//! - `GHOST`: metadata-only queue for pages evicted from `SMALL`
//!
//! The structure tracks queue membership in a hash map for O(1) queue-location
//! lookup and performs deterministic transitions suitable for unit testing.

use std::collections::{HashMap, VecDeque};

use fsqlite_types::PageNumber;

const DEFAULT_SMALL_RATIO_NUM: usize = 1;
const DEFAULT_SMALL_RATIO_DEN: usize = 10;
const DEFAULT_MAX_REINSERT: u8 = 1;
const ADAPT_MIN_SMALL_PERCENT: usize = 5;
const ADAPT_MAX_SMALL_PERCENT: usize = 30;
const ADAPT_PERCENT_DEN: usize = 100;
const DEFAULT_ADAPT_EVERY_EVICTIONS: usize = 1000;

/// Queue kind for S3-FIFO membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueKind {
    Small,
    Main,
    Ghost,
}

/// O(1) lookup result describing which queue currently owns a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueLocation {
    pub kind: QueueKind,
}

/// Configuration for [`S3Fifo`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3FifoConfig {
    capacity: usize,
    small_capacity: usize,
    main_capacity: usize,
    ghost_capacity: usize,
    max_reinsert: u8,
}

impl S3FifoConfig {
    /// Build default capacities from total resident capacity.
    ///
    /// Default split:
    /// - `small`: ceil(10% of capacity), min 1
    /// - `main`: remainder
    /// - `ghost`: same as `small`
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");

        let scaled_capacity = capacity.saturating_mul(DEFAULT_SMALL_RATIO_NUM);
        let mut small_capacity = scaled_capacity / DEFAULT_SMALL_RATIO_DEN;
        if scaled_capacity % DEFAULT_SMALL_RATIO_DEN != 0 {
            small_capacity = small_capacity.saturating_add(1);
        }
        if small_capacity == 0 {
            small_capacity = 1;
        }

        let main_capacity = capacity.saturating_sub(small_capacity);
        let ghost_capacity = small_capacity;

        Self {
            capacity,
            small_capacity,
            main_capacity,
            ghost_capacity,
            max_reinsert: DEFAULT_MAX_REINSERT,
        }
    }

    /// Build an explicit-capacity configuration.
    #[must_use]
    pub fn with_limits(
        capacity: usize,
        small_capacity: usize,
        ghost_capacity: usize,
        max_reinsert: u8,
    ) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        assert!(
            small_capacity <= capacity,
            "small_capacity must be <= capacity"
        );

        Self {
            capacity,
            small_capacity,
            main_capacity: capacity.saturating_sub(small_capacity),
            ghost_capacity,
            max_reinsert,
        }
    }

    #[inline]
    #[must_use]
    pub const fn capacity(self) -> usize {
        self.capacity
    }

    #[inline]
    #[must_use]
    pub const fn small_capacity(self) -> usize {
        self.small_capacity
    }

    #[inline]
    #[must_use]
    pub const fn main_capacity(self) -> usize {
        self.main_capacity
    }

    #[inline]
    #[must_use]
    pub const fn ghost_capacity(self) -> usize {
        self.ghost_capacity
    }

    #[inline]
    #[must_use]
    pub const fn max_reinsert(self) -> u8 {
        self.max_reinsert
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResidentState {
    queue: QueueKind,
    accessed: bool,
    reinsert_count: u8,
}

impl ResidentState {
    #[must_use]
    const fn small() -> Self {
        Self {
            queue: QueueKind::Small,
            accessed: false,
            reinsert_count: 0,
        }
    }

    #[must_use]
    const fn main() -> Self {
        Self {
            queue: QueueKind::Main,
            accessed: false,
            reinsert_count: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryState {
    Resident(ResidentState),
    Ghost,
}

/// Deterministic transition events emitted by [`S3Fifo::insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3FifoEvent {
    Inserted(PageNumber),
    AlreadyResident {
        page_id: PageNumber,
        queue: QueueKind,
    },
    GhostReadmission(PageNumber),
    PromotedToMain(PageNumber),
    EvictedFromSmallToGhost(PageNumber),
    ReinsertedInMain {
        page_id: PageNumber,
        reinsert_count: u8,
    },
    EvictedFromMain(PageNumber),
    GhostTrimmed(PageNumber),
    AdaptiveSplitChanged {
        old_small_capacity: usize,
        new_small_capacity: usize,
    },
}

/// Active eviction policy during S3-FIFO rollout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RolloutPolicy {
    Arc,
    S3Fifo,
}

/// Decision emitted after evaluating one rollout window (`bd-1xfri.6`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RolloutDecision {
    KeepArc,
    KeepS3Fifo,
    FallbackToArc,
}

/// Per-window comparator metrics used by [`S3FifoRolloutGate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RolloutMetrics {
    miss_rate_ppm: u32,
    p99_read_latency_micros: u64,
}

impl RolloutMetrics {
    #[must_use]
    pub const fn new(miss_rate_ppm: u32, p99_read_latency_micros: u64) -> Self {
        Self {
            miss_rate_ppm,
            p99_read_latency_micros,
        }
    }

    #[inline]
    #[must_use]
    pub const fn miss_rate_ppm(self) -> u32 {
        self.miss_rate_ppm
    }

    #[inline]
    #[must_use]
    pub const fn p99_read_latency_micros(self) -> u64 {
        self.p99_read_latency_micros
    }
}

/// Rollout safety/quality gate for deterministic ARC fallback.
///
/// Trigger policy from `bd-1xfri.6`:
/// - miss-rate regression > 5% over baseline for 3 consecutive windows, OR
/// - p99 regression > 10% over baseline for 3 consecutive windows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3FifoRolloutGate {
    miss_regression_threshold_bps: u32,
    p99_regression_threshold_bps: u32,
    required_consecutive_bad_windows: usize,
    consecutive_bad_windows: usize,
    active_policy: RolloutPolicy,
}

impl Default for S3FifoRolloutGate {
    fn default() -> Self {
        Self {
            miss_regression_threshold_bps: 500,
            p99_regression_threshold_bps: 1000,
            required_consecutive_bad_windows: 3,
            consecutive_bad_windows: 0,
            active_policy: RolloutPolicy::S3Fifo,
        }
    }
}

impl S3FifoRolloutGate {
    /// Build a gate with explicit thresholds.
    #[must_use]
    pub fn new(
        miss_regression_threshold_bps: u32,
        p99_regression_threshold_bps: u32,
        required_consecutive_bad_windows: usize,
        active_policy: RolloutPolicy,
    ) -> Self {
        Self {
            miss_regression_threshold_bps,
            p99_regression_threshold_bps,
            required_consecutive_bad_windows: required_consecutive_bad_windows.max(1),
            consecutive_bad_windows: 0,
            active_policy,
        }
    }

    #[inline]
    #[must_use]
    pub const fn active_policy(&self) -> RolloutPolicy {
        self.active_policy
    }

    #[inline]
    #[must_use]
    pub const fn consecutive_bad_windows(&self) -> usize {
        self.consecutive_bad_windows
    }

    /// Evaluate one comparison window (`baseline` vs `candidate`) and
    /// return the rollout decision.
    pub fn evaluate_window(
        &mut self,
        baseline: RolloutMetrics,
        candidate: RolloutMetrics,
    ) -> RolloutDecision {
        if self.active_policy == RolloutPolicy::Arc {
            return RolloutDecision::KeepArc;
        }

        let miss_regression_bps = regression_bps(
            u64::from(baseline.miss_rate_ppm()),
            u64::from(candidate.miss_rate_ppm()),
        );
        let p99_regression_bps = regression_bps(
            baseline.p99_read_latency_micros(),
            candidate.p99_read_latency_micros(),
        );

        let bad_window = miss_regression_bps > self.miss_regression_threshold_bps
            || p99_regression_bps > self.p99_regression_threshold_bps;
        if bad_window {
            self.consecutive_bad_windows = self.consecutive_bad_windows.saturating_add(1);
        } else {
            self.consecutive_bad_windows = 0;
        }

        if self.consecutive_bad_windows >= self.required_consecutive_bad_windows {
            self.active_policy = RolloutPolicy::Arc;
            return RolloutDecision::FallbackToArc;
        }

        RolloutDecision::KeepS3Fifo
    }

    /// Reset bad-window streak and force active policy.
    pub fn reset(&mut self, active_policy: RolloutPolicy) {
        self.consecutive_bad_windows = 0;
        self.active_policy = active_policy;
    }
}

/// Three-queue S3-FIFO state machine.
#[derive(Debug)]
pub struct S3Fifo {
    config: S3FifoConfig,
    small: VecDeque<PageNumber>,
    main: VecDeque<PageNumber>,
    ghost: VecDeque<PageNumber>,
    index: HashMap<PageNumber, EntryState>,
    small_capacity_min: usize,
    small_capacity_max: usize,
    adapt_every_evictions: usize,
    evictions_since_adapt: usize,
    ghost_hits_since_adapt: usize,
    small_hits_since_adapt: usize,
    main_hits_since_adapt: usize,
    main_pressure_since_adapt: usize,
    posterior_alpha: u64,
    posterior_beta: u64,
}

impl S3Fifo {
    /// Create with default split from `capacity`.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self::with_config(S3FifoConfig::new(capacity))
    }

    /// Create with explicit configuration.
    #[must_use]
    pub fn with_config(config: S3FifoConfig) -> Self {
        let mut small_capacity_min =
            ceil_ratio(config.capacity, ADAPT_MIN_SMALL_PERCENT, ADAPT_PERCENT_DEN);
        if small_capacity_min == 0 {
            small_capacity_min = 1;
        }

        let mut small_capacity_max =
            ceil_ratio(config.capacity, ADAPT_MAX_SMALL_PERCENT, ADAPT_PERCENT_DEN);
        if small_capacity_max < small_capacity_min {
            small_capacity_max = small_capacity_min;
        }
        if small_capacity_max > config.capacity {
            small_capacity_max = config.capacity;
        }

        Self {
            config,
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: VecDeque::new(),
            index: HashMap::new(),
            small_capacity_min,
            small_capacity_max,
            adapt_every_evictions: DEFAULT_ADAPT_EVERY_EVICTIONS,
            evictions_since_adapt: 0,
            ghost_hits_since_adapt: 0,
            small_hits_since_adapt: 0,
            main_hits_since_adapt: 0,
            main_pressure_since_adapt: 0,
            posterior_alpha: 1,
            posterior_beta: 1,
        }
    }

    #[inline]
    #[must_use]
    pub const fn config(&self) -> S3FifoConfig {
        self.config
    }

    /// Current adaptation interval in eviction events.
    #[inline]
    #[must_use]
    pub const fn adaptation_interval(&self) -> usize {
        self.adapt_every_evictions
    }

    /// Current adaptive SMALL-capacity bounds `(min, max)`.
    #[inline]
    #[must_use]
    pub const fn adaptive_bounds(&self) -> (usize, usize) {
        (self.small_capacity_min, self.small_capacity_max)
    }

    /// Configure adaptation interval in eviction events.
    ///
    /// Values below `1` are clamped to `1`.
    pub fn set_adaptation_interval(&mut self, evictions: usize) {
        self.adapt_every_evictions = evictions.max(1);
    }

    /// Configure adaptive SMALL-capacity bounds.
    ///
    /// Bounds are clamped to `[1, capacity]`, and the current SMALL capacity
    /// is also clamped into the resulting range.
    pub fn set_adaptive_bounds(&mut self, small_capacity_min: usize, small_capacity_max: usize) {
        let min = small_capacity_min.clamp(1, self.config.capacity);
        let max = small_capacity_max.clamp(min, self.config.capacity);
        self.small_capacity_min = min;
        self.small_capacity_max = max;

        let clamped_small = self.config.small_capacity.clamp(min, max);
        if clamped_small != self.config.small_capacity {
            self.config.small_capacity = clamped_small;
            self.config.main_capacity = self.config.capacity.saturating_sub(clamped_small);
            self.config.ghost_capacity = clamped_small;
        }
    }

    #[inline]
    #[must_use]
    pub fn resident_len(&self) -> usize {
        self.small.len().saturating_add(self.main.len())
    }

    #[inline]
    #[must_use]
    pub fn ghost_len(&self) -> usize {
        self.ghost.len()
    }

    #[inline]
    #[must_use]
    pub fn small_pages(&self) -> Vec<PageNumber> {
        self.small.iter().copied().collect()
    }

    #[inline]
    #[must_use]
    pub fn main_pages(&self) -> Vec<PageNumber> {
        self.main.iter().copied().collect()
    }

    #[inline]
    #[must_use]
    pub fn ghost_pages(&self) -> Vec<PageNumber> {
        self.ghost.iter().copied().collect()
    }

    /// O(1) queue-location lookup.
    #[inline]
    #[must_use]
    pub fn lookup(&self, page_id: PageNumber) -> Option<QueueLocation> {
        match self.index.get(&page_id).copied() {
            Some(EntryState::Resident(state)) => Some(QueueLocation { kind: state.queue }),
            Some(EntryState::Ghost) => Some(QueueLocation {
                kind: QueueKind::Ghost,
            }),
            None => None,
        }
    }

    /// Set access bit for a resident page without reordering any queue.
    pub fn access(&mut self, page_id: PageNumber) -> bool {
        match self.index.get_mut(&page_id) {
            Some(EntryState::Resident(state)) => {
                state.accessed = true;
                match state.queue {
                    QueueKind::Small => {
                        self.small_hits_since_adapt = self.small_hits_since_adapt.saturating_add(1);
                    }
                    QueueKind::Main => {
                        self.main_hits_since_adapt = self.main_hits_since_adapt.saturating_add(1);
                    }
                    QueueKind::Ghost => {}
                }
                true
            }
            Some(EntryState::Ghost) | None => false,
        }
    }

    /// Admit a page to `SMALL`, then rebalance.
    ///
    /// Returns the ordered transition events performed for this call.
    #[must_use]
    pub fn insert(&mut self, page_id: PageNumber) -> Vec<S3FifoEvent> {
        let mut events = Vec::new();

        match self.index.get(&page_id).copied() {
            Some(EntryState::Resident(state)) => {
                self.access(page_id);
                events.push(S3FifoEvent::AlreadyResident {
                    page_id,
                    queue: state.queue,
                });
                return events;
            }
            Some(EntryState::Ghost) => {
                self.remove_ghost(page_id);
                self.ghost_hits_since_adapt = self.ghost_hits_since_adapt.saturating_add(1);

                self.main.push_back(page_id);
                self.index
                    .insert(page_id, EntryState::Resident(ResidentState::main()));
                events.push(S3FifoEvent::GhostReadmission(page_id));

                self.rebalance(&mut events);
                return events;
            }
            None => {}
        }

        self.small.push_back(page_id);
        self.index
            .insert(page_id, EntryState::Resident(ResidentState::small()));
        events.push(S3FifoEvent::Inserted(page_id));

        self.rebalance(&mut events);
        events
    }

    fn rebalance(&mut self, events: &mut Vec<S3FifoEvent>) {
        while self.small.len() > self.config.small_capacity {
            self.evict_from_small(events);
        }

        while self.main.len() > self.config.main_capacity {
            if !self.evict_from_main(events) {
                break;
            }
        }

        while self.resident_len() > self.config.capacity {
            if !self.small.is_empty() {
                self.evict_from_small(events);
                continue;
            }

            if !self.evict_from_main(events) {
                break;
            }
        }

        self.trim_ghosts(events);
        self.maybe_adapt_split(events);

        while self.small.len() > self.config.small_capacity {
            self.evict_from_small(events);
        }

        while self.main.len() > self.config.main_capacity {
            if !self.evict_from_main(events) {
                break;
            }
        }

        while self.resident_len() > self.config.capacity {
            if !self.small.is_empty() {
                self.evict_from_small(events);
                continue;
            }

            if !self.evict_from_main(events) {
                break;
            }
        }

        self.trim_ghosts(events);
    }

    fn evict_from_small(&mut self, events: &mut Vec<S3FifoEvent>) {
        let Some(page_id) = self.small.pop_front() else {
            return;
        };

        let state = if let Some(EntryState::Resident(state)) = self.index.get(&page_id).copied() {
            state
        } else {
            // Invariant violation: page in SMALL queue but missing/wrong in index.
            // Self-heal by treating as unaccessed.
            ResidentState::small()
        };

        self.evictions_since_adapt = self.evictions_since_adapt.saturating_add(1);

        if state.accessed {
            self.main.push_back(page_id);
            self.index
                .insert(page_id, EntryState::Resident(ResidentState::main()));
            events.push(S3FifoEvent::PromotedToMain(page_id));
            return;
        }

        self.ghost.push_back(page_id);
        self.index.insert(page_id, EntryState::Ghost);
        events.push(S3FifoEvent::EvictedFromSmallToGhost(page_id));
    }

    fn evict_from_main(&mut self, events: &mut Vec<S3FifoEvent>) -> bool {
        let Some(page_id) = self.main.pop_front() else {
            return false;
        };

        let mut state = if let Some(EntryState::Resident(state)) = self.index.get(&page_id).copied()
        {
            state
        } else {
            // Invariant violation: page in MAIN queue but missing/wrong in index.
            // Self-heal by treating as unaccessed.
            ResidentState::main()
        };

        self.evictions_since_adapt = self.evictions_since_adapt.saturating_add(1);
        if state.accessed {
            self.main_pressure_since_adapt = self.main_pressure_since_adapt.saturating_add(1);
        }

        if state.accessed && state.reinsert_count < self.config.max_reinsert {
            state.accessed = false;
            state.reinsert_count = state.reinsert_count.saturating_add(1);
            self.main.push_back(page_id);
            self.index.insert(page_id, EntryState::Resident(state));
            events.push(S3FifoEvent::ReinsertedInMain {
                page_id,
                reinsert_count: state.reinsert_count,
            });
            return true;
        }

        self.index.remove(&page_id);
        events.push(S3FifoEvent::EvictedFromMain(page_id));
        true
    }

    fn trim_ghosts(&mut self, events: &mut Vec<S3FifoEvent>) {
        while self.ghost.len() > self.config.ghost_capacity {
            let Some(page_id) = self.ghost.pop_front() else {
                break;
            };
            if matches!(self.index.get(&page_id), Some(EntryState::Ghost)) {
                self.index.remove(&page_id);
            }
            events.push(S3FifoEvent::GhostTrimmed(page_id));
        }
    }

    fn remove_ghost(&mut self, page_id: PageNumber) {
        // PERF: Avoid O(N) `retain` scan on the ghost queue. Dead entries will be
        // lazily ignored by `trim_ghosts` since they will no longer have `EntryState::Ghost`.
        if matches!(self.index.get(&page_id), Some(EntryState::Ghost)) {
            self.index.remove(&page_id);
        }
    }

    fn maybe_adapt_split(&mut self, events: &mut Vec<S3FifoEvent>) {
        if self.adapt_every_evictions == 0
            || self.evictions_since_adapt < self.adapt_every_evictions
        {
            return;
        }

        self.evictions_since_adapt = 0;

        let ghost_hits = self.ghost_hits_since_adapt;
        let small_hits = self.small_hits_since_adapt;
        let main_hits = self.main_hits_since_adapt;
        let main_pressure = self.main_pressure_since_adapt;
        self.ghost_hits_since_adapt = 0;
        self.small_hits_since_adapt = 0;
        self.main_hits_since_adapt = 0;
        self.main_pressure_since_adapt = 0;

        let need_small = ghost_hits.saturating_add(small_hits);
        let need_main = main_hits.saturating_add(main_pressure);
        let total_signal = need_small.saturating_add(need_main);
        if total_signal == 0 {
            return;
        }

        let alpha_update = u64::try_from(need_small).unwrap_or(u64::MAX);
        let beta_update = u64::try_from(need_main).unwrap_or(u64::MAX);
        self.posterior_alpha = self.posterior_alpha.saturating_add(alpha_update);
        self.posterior_beta = self.posterior_beta.saturating_add(beta_update);

        // Decay posterior counts to prevent saturation and maintain relative proportions.
        // If the sum approaches u64::MAX, halve both values.
        if self.posterior_alpha.saturating_add(self.posterior_beta) > (u64::MAX / 2) {
            self.posterior_alpha = (self.posterior_alpha / 2).max(1);
            self.posterior_beta = (self.posterior_beta / 2).max(1);
        }

        let denominator = self.posterior_alpha.saturating_add(self.posterior_beta);
        if denominator == 0 {
            return;
        }

        let span = self
            .small_capacity_max
            .saturating_sub(self.small_capacity_min);
        let target_offset = rounded_ratio(span, self.posterior_alpha, denominator);
        let target_small = self
            .small_capacity_min
            .saturating_add(target_offset)
            .clamp(self.small_capacity_min, self.small_capacity_max);

        let old_small = self.config.small_capacity;
        if target_small == old_small {
            return;
        }

        // Blend toward the posterior target with a step size proportional to
        // ghost-hit ratio; a tiny minimum step avoids deadlock at zero ghosts.
        let step_numerator = ghost_hits.max(1);
        let step_denominator = total_signal;
        let blended_small = step_toward(old_small, target_small, step_numerator, step_denominator)
            .clamp(self.small_capacity_min, self.small_capacity_max);
        if blended_small == old_small {
            return;
        }

        self.config.small_capacity = blended_small;
        self.config.main_capacity = self.config.capacity.saturating_sub(blended_small);
        self.config.ghost_capacity = blended_small;

        events.push(S3FifoEvent::AdaptiveSplitChanged {
            old_small_capacity: old_small,
            new_small_capacity: blended_small,
        });
    }
}

#[inline]
fn ceil_ratio(value: usize, numerator: usize, denominator: usize) -> usize {
    assert!(denominator > 0, "denominator must be > 0");
    let scaled = value.saturating_mul(numerator);
    let mut out = scaled / denominator;
    if scaled % denominator != 0 {
        out = out.saturating_add(1);
    }
    out
}

#[inline]
fn rounded_ratio(value: usize, numerator: u64, denominator: u64) -> usize {
    if denominator == 0 {
        return 0;
    }
    let value_u128 = u128::try_from(value).unwrap_or(u128::MAX);
    let numerator_u128 = u128::from(numerator);
    let denominator_u128 = u128::from(denominator);
    let scaled = value_u128.saturating_mul(numerator_u128);
    let rounded = scaled
        .saturating_add(denominator_u128 / 2)
        .saturating_div(denominator_u128);
    usize::try_from(rounded).unwrap_or(usize::MAX)
}

#[inline]
fn step_toward(current: usize, target: usize, numerator: usize, denominator: usize) -> usize {
    if denominator == 0 || current == target {
        return current;
    }

    let bounded_numerator = numerator.clamp(1, denominator);
    let distance = current.abs_diff(target);
    let delta = ceil_ratio(distance, bounded_numerator, denominator).max(1);
    if target > current {
        current.saturating_add(delta).min(target)
    } else {
        current.saturating_sub(delta).max(target)
    }
}

#[inline]
fn regression_bps(baseline: u64, candidate: u64) -> u32 {
    if candidate <= baseline {
        return 0;
    }
    if baseline == 0 {
        return u32::MAX;
    }
    let delta = candidate.saturating_sub(baseline);
    let scaled = u128::from(delta).saturating_mul(10_000);
    let bps = scaled.saturating_div(u128::from(baseline));
    u32::try_from(bps).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashbrown::HashMap;
    use std::collections::{HashSet, VecDeque};
    use std::sync::{Arc, Mutex};

    use proptest::collection::vec;
    use proptest::prelude::{ProptestConfig, any};
    use proptest::proptest;

    fn pg(n: u32) -> PageNumber {
        match PageNumber::new(n) {
            Some(page) => page,
            None => panic!("page number must be non-zero"),
        }
    }

    #[test]
    fn default_split_matches_spec() {
        let config = S3FifoConfig::new(10);
        assert_eq!(config.small_capacity(), 1);
        assert_eq!(config.main_capacity(), 9);
        assert_eq!(config.ghost_capacity(), 1);
        assert_eq!(config.max_reinsert(), 1);
    }

    #[test]
    fn access_sets_flag_without_reordering_small() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(4, 2, 2, 1));
        let _ = fifo.insert(pg(1));
        let _ = fifo.insert(pg(2));
        assert_eq!(fifo.small_pages(), vec![pg(1), pg(2)]);

        assert!(fifo.access(pg(1)));
        assert_eq!(fifo.small_pages(), vec![pg(1), pg(2)]);

        let location = fifo.lookup(pg(1));
        assert_eq!(
            location,
            Some(QueueLocation {
                kind: QueueKind::Small
            })
        );
    }

    #[test]
    fn accessed_small_page_promotes_to_main() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(3, 2, 2, 1));
        let _ = fifo.insert(pg(1));
        let _ = fifo.insert(pg(2));
        assert!(fifo.access(pg(1)));

        let events = fifo.insert(pg(3));
        assert!(events.contains(&S3FifoEvent::PromotedToMain(pg(1))));
        assert_eq!(fifo.main_pages(), vec![pg(1)]);
        assert_eq!(fifo.small_pages(), vec![pg(2), pg(3)]);
        assert_eq!(
            fifo.lookup(pg(1)),
            Some(QueueLocation {
                kind: QueueKind::Main
            })
        );
    }

    #[test]
    fn main_reinsertion_is_bounded_by_max_reinsert() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(2, 1, 1, 1));

        let _ = fifo.insert(pg(1));
        assert!(fifo.access(pg(1)));
        let _ = fifo.insert(pg(2)); // 1 promoted to MAIN

        assert!(fifo.access(pg(1)));
        let _ = fifo.insert(pg(3)); // 2 -> GHOST
        assert!(fifo.access(pg(3)));
        let events_reinsert = fifo.insert(pg(4)); // 3 -> MAIN, then MAIN overflow

        assert!(events_reinsert.contains(&S3FifoEvent::ReinsertedInMain {
            page_id: pg(1),
            reinsert_count: 1
        }));

        assert!(fifo.access(pg(1)));
        assert!(fifo.access(pg(4)));
        let events_evict = fifo.insert(pg(5)); // second overflow; 1 should now evict

        assert!(events_evict.contains(&S3FifoEvent::EvictedFromMain(pg(1))));
        assert_eq!(fifo.lookup(pg(1)), None);
    }

    #[test]
    fn ghost_queue_is_bounded_and_trimmed() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(2, 1, 1, 1));
        let _ = fifo.insert(pg(1));
        let _ = fifo.insert(pg(2)); // 1 -> GHOST

        assert_eq!(fifo.ghost_pages(), vec![pg(1)]);
        assert_eq!(
            fifo.lookup(pg(1)),
            Some(QueueLocation {
                kind: QueueKind::Ghost
            })
        );

        let events = fifo.insert(pg(3)); // 2 -> GHOST, then trim 1
        assert!(events.contains(&S3FifoEvent::GhostTrimmed(pg(1))));
        assert_eq!(fifo.ghost_pages(), vec![pg(2)]);
        assert_eq!(fifo.lookup(pg(1)), None);
    }

    #[test]
    fn queue_lookup_tracks_small_main_and_ghost() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(3, 2, 2, 1));
        let _ = fifo.insert(pg(10));
        let _ = fifo.insert(pg(11));
        assert_eq!(
            fifo.lookup(pg(10)),
            Some(QueueLocation {
                kind: QueueKind::Small
            })
        );

        assert!(fifo.access(pg(10)));
        let _ = fifo.insert(pg(12)); // promote 10 -> MAIN
        assert_eq!(
            fifo.lookup(pg(10)),
            Some(QueueLocation {
                kind: QueueKind::Main
            })
        );

        let _ = fifo.insert(pg(13)); // likely pushes 11 to ghost path
        assert_eq!(
            fifo.lookup(pg(11)),
            Some(QueueLocation {
                kind: QueueKind::Ghost
            })
        );
    }

    #[test]
    fn adaptive_split_increases_small_when_ghost_hits_dominate() {
        let mut fifo = S3Fifo::new(10);
        fifo.set_adaptive_bounds(1, 3);
        fifo.set_adaptation_interval(2);

        let before = fifo.config().small_capacity();
        let _ = fifo.insert(pg(1));
        let _ = fifo.insert(pg(2)); // 1 -> ghost, eviction count = 1
        let _ = fifo.insert(pg(1)); // ghost readmission, ghost_hits = 1
        let events = fifo.insert(pg(3)); // 2 -> ghost, eviction count = 2 => adapt

        assert!(events.iter().any(|event| matches!(
            event,
            S3FifoEvent::AdaptiveSplitChanged {
                old_small_capacity,
                new_small_capacity,
            } if *new_small_capacity > *old_small_capacity
        )));
        assert!(fifo.config().small_capacity() > before);
        assert!(fifo.config().small_capacity() <= 3);
    }

    #[test]
    fn adaptive_split_decreases_small_when_main_hits_dominate() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(10, 3, 3, 1));
        fifo.set_adaptive_bounds(1, 3);
        fifo.set_adaptation_interval(2);

        let _ = fifo.insert(pg(1));
        let _ = fifo.insert(pg(2));
        let _ = fifo.insert(pg(3));
        assert!(fifo.access(pg(1)));
        let _ = fifo.insert(pg(4)); // 1 promoted to MAIN, eviction count = 1
        assert!(fifo.access(pg(1))); // MAIN hit signal
        let events = fifo.insert(pg(5)); // eviction count = 2 => adapt

        assert!(events.iter().any(|event| matches!(
            event,
            S3FifoEvent::AdaptiveSplitChanged {
                old_small_capacity,
                new_small_capacity,
            } if *new_small_capacity < *old_small_capacity
        )));
        assert!(fifo.config().small_capacity() < 3);
        assert!(fifo.config().small_capacity() >= 1);
    }

    #[test]
    fn adaptive_split_respects_configured_bounds() {
        let mut fifo = S3Fifo::new(20);
        fifo.set_adaptive_bounds(2, 3);
        fifo.set_adaptation_interval(1);

        for n in 1_u32..30_u32 {
            let _ = fifo.insert(pg(n));
            if n > 1 {
                let _ = fifo.insert(pg(n - 1)); // drive ghost-hit pressure
            }
        }

        let small_capacity = fifo.config().small_capacity();
        assert!(small_capacity >= 2);
        assert!(small_capacity <= 3);
    }

    #[test]
    fn adaptive_split_scan_workload_increases_small_capacity() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(8, 1, 3, 1));
        fifo.set_adaptive_bounds(1, 3);
        fifo.set_adaptation_interval(2);
        let before = fifo.config().small_capacity();
        let mut observed_increase = false;

        let has_increase = |events: &[S3FifoEvent]| {
            events.iter().any(|event| {
                matches!(
                    event,
                    S3FifoEvent::AdaptiveSplitChanged {
                        old_small_capacity,
                        new_small_capacity,
                    } if *new_small_capacity > *old_small_capacity
                )
            })
        };

        for n in 2_u32..1_000_u32 {
            let events1 = fifo.insert(pg(n));
            let events2 = fifo.insert(pg(n - 1)); // immediate scan readmission pressure
            if has_increase(&events1) || has_increase(&events2) {
                observed_increase = true;
                break;
            }
        }

        assert!(observed_increase);
        assert!(fifo.config().small_capacity() > before);
    }

    #[test]
    fn adaptive_split_randomish_hot_main_workload_decreases_small_capacity() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(4, 2, 2, 1));
        fifo.set_adaptive_bounds(1, 2);
        fifo.set_adaptation_interval(8);

        let _ = fifo.insert(pg(1));
        let _ = fifo.insert(pg(2));
        assert!(fifo.access(pg(1)));
        let _ = fifo.insert(pg(3)); // promote 1 -> MAIN

        for n in 4_u32..4_000_u32 {
            assert!(fifo.access(pg(1))); // keep MAIN pressure high
            let _ = fifo.insert(pg(n));
        }

        assert_eq!(fifo.config().small_capacity(), 1);
    }

    #[test]
    fn adaptive_split_converges_within_ten_thousand_evictions() {
        let mut fifo = S3Fifo::with_config(S3FifoConfig::with_limits(4, 2, 2, 1));
        fifo.set_adaptive_bounds(1, 2);
        fifo.set_adaptation_interval(16);

        let _ = fifo.insert(pg(1));
        let _ = fifo.insert(pg(2));
        assert!(fifo.access(pg(1)));
        let _ = fifo.insert(pg(3)); // promote 1 -> MAIN

        let mut eviction_events = 0_usize;
        let mut last_change_eviction = 0_usize;

        for n in 4_u32..20_000_u32 {
            assert!(fifo.access(pg(1)));
            let events = fifo.insert(pg(n));
            for event in events {
                if matches!(
                    event,
                    S3FifoEvent::PromotedToMain(_)
                        | S3FifoEvent::EvictedFromSmallToGhost(_)
                        | S3FifoEvent::ReinsertedInMain { .. }
                        | S3FifoEvent::EvictedFromMain(_)
                ) {
                    eviction_events = eviction_events.saturating_add(1);
                }
                if matches!(event, S3FifoEvent::AdaptiveSplitChanged { .. }) {
                    last_change_eviction = eviction_events;
                }
            }
        }

        assert!(eviction_events > 10_000);
        assert!(last_change_eviction > 0);
        assert!(last_change_eviction <= 10_000);
        assert_eq!(fifo.config().small_capacity(), 1);
    }

    #[test]
    fn rollout_gate_falls_back_after_three_consecutive_bad_miss_windows() {
        let baseline = RolloutMetrics::new(100_000, 1_000);
        let bad_candidate = RolloutMetrics::new(106_000, 1_000); // +6% miss
        let mut gate = S3FifoRolloutGate::default();

        assert_eq!(
            gate.evaluate_window(baseline, bad_candidate),
            RolloutDecision::KeepS3Fifo
        );
        assert_eq!(
            gate.evaluate_window(baseline, bad_candidate),
            RolloutDecision::KeepS3Fifo
        );
        assert_eq!(
            gate.evaluate_window(baseline, bad_candidate),
            RolloutDecision::FallbackToArc
        );
        assert_eq!(gate.active_policy(), RolloutPolicy::Arc);
    }

    #[test]
    fn rollout_gate_bad_streak_resets_on_good_window() {
        let baseline = RolloutMetrics::new(100_000, 1_000);
        let bad_candidate = RolloutMetrics::new(106_000, 1_000); // +6% miss
        let good_candidate = RolloutMetrics::new(99_000, 950);
        let mut gate = S3FifoRolloutGate::default();

        assert_eq!(
            gate.evaluate_window(baseline, bad_candidate),
            RolloutDecision::KeepS3Fifo
        );
        assert_eq!(
            gate.evaluate_window(baseline, good_candidate),
            RolloutDecision::KeepS3Fifo
        );
        assert_eq!(gate.consecutive_bad_windows(), 0);
    }

    #[test]
    fn rollout_gate_falls_back_on_p99_regression() {
        let baseline = RolloutMetrics::new(100_000, 1_000);
        let p99_bad_candidate = RolloutMetrics::new(100_000, 1_120); // +12% p99
        let mut gate = S3FifoRolloutGate::default();

        let _ = gate.evaluate_window(baseline, p99_bad_candidate);
        let _ = gate.evaluate_window(baseline, p99_bad_candidate);
        assert_eq!(
            gate.evaluate_window(baseline, p99_bad_candidate),
            RolloutDecision::FallbackToArc
        );
        assert_eq!(gate.active_policy(), RolloutPolicy::Arc);
    }

    #[test]
    fn rollout_gate_thresholds_are_strictly_greater_than_contract_cutoffs() {
        let baseline = RolloutMetrics::new(100_000, 1_000);
        let exactly_five_percent_miss = RolloutMetrics::new(105_000, 1_000); // == 5%
        let exactly_ten_percent_p99 = RolloutMetrics::new(100_000, 1_100); // == 10%
        let mut gate = S3FifoRolloutGate::default();

        for _ in 0..10 {
            assert_eq!(
                gate.evaluate_window(baseline, exactly_five_percent_miss),
                RolloutDecision::KeepS3Fifo
            );
            assert_eq!(
                gate.evaluate_window(baseline, exactly_ten_percent_p99),
                RolloutDecision::KeepS3Fifo
            );
        }
        assert_eq!(gate.active_policy(), RolloutPolicy::S3Fifo);
    }

    #[test]
    fn rollout_gate_sticks_with_arc_after_fallback() {
        let baseline = RolloutMetrics::new(100_000, 1_000);
        let bad_candidate = RolloutMetrics::new(106_000, 1_000); // +6% miss
        let good_candidate = RolloutMetrics::new(80_000, 700);
        let mut gate = S3FifoRolloutGate::default();

        let _ = gate.evaluate_window(baseline, bad_candidate);
        let _ = gate.evaluate_window(baseline, bad_candidate);
        let _ = gate.evaluate_window(baseline, bad_candidate);
        assert_eq!(gate.active_policy(), RolloutPolicy::Arc);
        assert_eq!(
            gate.evaluate_window(baseline, good_candidate),
            RolloutDecision::KeepArc
        );
        assert_eq!(gate.active_policy(), RolloutPolicy::Arc);
    }

    #[derive(Debug, Default)]
    struct InvariantTracker {
        tick: usize,
        small_stamp: HashMap<PageNumber, usize>,
        main_stamp: HashMap<PageNumber, usize>,
        ghost_stamp: HashMap<PageNumber, usize>,
        small_accessed: HashSet<PageNumber>,
        ghost_from_small: HashSet<PageNumber>,
    }

    impl InvariantTracker {
        fn bump_small(&mut self, page_id: PageNumber) {
            self.tick = self.tick.saturating_add(1);
            self.small_stamp.insert(page_id, self.tick);
            self.main_stamp.remove(&page_id);
            self.ghost_stamp.remove(&page_id);
            self.small_accessed.remove(&page_id);
            self.ghost_from_small.remove(&page_id);
        }

        fn bump_main(&mut self, page_id: PageNumber) {
            self.tick = self.tick.saturating_add(1);
            self.main_stamp.insert(page_id, self.tick);
            self.small_stamp.remove(&page_id);
            self.ghost_stamp.remove(&page_id);
            self.small_accessed.remove(&page_id);
            self.ghost_from_small.remove(&page_id);
        }

        fn bump_ghost(&mut self, page_id: PageNumber) {
            self.tick = self.tick.saturating_add(1);
            self.ghost_stamp.insert(page_id, self.tick);
            self.small_stamp.remove(&page_id);
            self.main_stamp.remove(&page_id);
            self.small_accessed.remove(&page_id);
            self.ghost_from_small.insert(page_id);
        }

        fn note_access(&mut self, fifo: &S3Fifo, page_id: PageNumber) {
            if matches!(
                fifo.lookup(page_id),
                Some(QueueLocation {
                    kind: QueueKind::Small
                })
            ) {
                self.small_accessed.insert(page_id);
            }
        }

        fn observe_events(&mut self, events: &[S3FifoEvent]) {
            for event in events {
                match *event {
                    S3FifoEvent::Inserted(page_id) => self.bump_small(page_id),
                    S3FifoEvent::AlreadyResident { page_id, queue } => {
                        if queue == QueueKind::Small {
                            self.small_accessed.insert(page_id);
                        }
                    }
                    S3FifoEvent::GhostReadmission(page_id) => {
                        self.ghost_stamp.remove(&page_id);
                        self.ghost_from_small.remove(&page_id);
                        self.bump_main(page_id);
                    }
                    S3FifoEvent::GhostTrimmed(page_id) => {
                        self.ghost_stamp.remove(&page_id);
                        self.ghost_from_small.remove(&page_id);
                    }
                    S3FifoEvent::PromotedToMain(page_id) => {
                        assert!(
                            self.small_accessed.remove(&page_id),
                            "promoted page must have been accessed while in SMALL"
                        );
                        self.small_stamp.remove(&page_id);
                        self.bump_main(page_id);
                    }
                    S3FifoEvent::EvictedFromSmallToGhost(page_id) => {
                        assert!(
                            !self.small_accessed.contains(&page_id),
                            "SMALL page marked accessed must promote, not ghost-evict"
                        );
                        self.small_accessed.remove(&page_id);
                        self.small_stamp.remove(&page_id);
                        self.bump_ghost(page_id);
                    }
                    S3FifoEvent::ReinsertedInMain { page_id, .. } => self.bump_main(page_id),
                    S3FifoEvent::EvictedFromMain(page_id) => {
                        self.main_stamp.remove(&page_id);
                    }
                    S3FifoEvent::AdaptiveSplitChanged { .. } => {}
                }
            }
        }

        fn assert_fifo_order(
            queue_name: &str,
            queue: &[PageNumber],
            stamps: &HashMap<PageNumber, usize>,
        ) {
            let mut prev_stamp = 0_usize;
            for page_id in queue {
                let Some(stamp) = stamps.get(page_id).copied() else {
                    panic!("{queue_name} queue page {page_id:?} missing FIFO stamp");
                };
                assert!(
                    stamp >= prev_stamp,
                    "{queue_name} queue must maintain FIFO order"
                );
                prev_stamp = stamp;
            }
        }

        fn assert_invariants(&self, fifo: &S3Fifo, capacity: usize, bounds: (usize, usize)) {
            let small = fifo.small_pages();
            let main = fifo.main_pages();
            let ghost = fifo.ghost_pages();

            assert!(
                small.len().saturating_add(main.len()) <= capacity,
                "resident capacity must stay bounded"
            );

            let mut all_pages = HashSet::new();
            for page_id in &small {
                assert!(all_pages.insert(*page_id), "duplicate page in SMALL");
                assert_eq!(
                    fifo.lookup(*page_id),
                    Some(QueueLocation {
                        kind: QueueKind::Small
                    })
                );
            }
            for page_id in &main {
                assert!(all_pages.insert(*page_id), "duplicate page in MAIN");
                assert_eq!(
                    fifo.lookup(*page_id),
                    Some(QueueLocation {
                        kind: QueueKind::Main
                    })
                );
            }
            for page_id in &ghost {
                assert!(all_pages.insert(*page_id), "duplicate page in GHOST");
                assert_eq!(
                    fifo.lookup(*page_id),
                    Some(QueueLocation {
                        kind: QueueKind::Ghost
                    })
                );
                assert!(
                    self.ghost_from_small.contains(page_id),
                    "ghost page must originate from SMALL eviction"
                );
            }

            let small_capacity = fifo.config().small_capacity();
            assert!(small_capacity >= bounds.0);
            assert!(small_capacity <= bounds.1);

            Self::assert_fifo_order("SMALL", &small, &self.small_stamp);
            Self::assert_fifo_order("MAIN", &main, &self.main_stamp);
            Self::assert_fifo_order("GHOST", &ghost, &self.ghost_stamp);
        }
    }

    fn default_adaptive_bounds(capacity: usize) -> (usize, usize) {
        let mut min = ceil_ratio(capacity, ADAPT_MIN_SMALL_PERCENT, ADAPT_PERCENT_DEN);
        if min == 0 {
            min = 1;
        }
        let mut max = ceil_ratio(capacity, ADAPT_MAX_SMALL_PERCENT, ADAPT_PERCENT_DEN);
        if max < min {
            max = min;
        }
        if max > capacity {
            max = capacity;
        }
        (min, max)
    }

    fn page_from_workload(pattern: u8, step: usize, raw: u16, domain: u32) -> u32 {
        let bounded_domain = domain.max(1);
        let hot_domain = bounded_domain.min(16);
        let step_u32 = u32::try_from(step).unwrap_or(u32::MAX);
        match pattern % 4 {
            0 => 1 + (u32::from(raw) % bounded_domain), // uniform
            1 => 1 + (step_u32 % bounded_domain),       // sequential
            2 => {
                // Zipf-like: 85% hot-set traffic, 15% full-domain traffic.
                if raw % 100 < 85 {
                    1 + (u32::from(raw) % hot_domain)
                } else {
                    1 + (u32::from(raw) % bounded_domain)
                }
            }
            _ => {
                // Adversarial mixed: periodic scans plus hot-set churn.
                if step % 9 == 0 {
                    1 + ((step_u32 / 9) % bounded_domain)
                } else {
                    1 + (u32::from(raw) % hot_domain)
                }
            }
        }
    }

    fn run_sequence_with_invariants(
        capacity: usize,
        pattern: u8,
        ops: &[(u8, u16)],
        domain: u32,
    ) -> S3Fifo {
        let mut fifo = S3Fifo::new(capacity);
        fifo.set_adaptation_interval(8);
        let mut tracker = InvariantTracker::default();
        let bounds = default_adaptive_bounds(capacity);

        for (step, (op_kind, raw)) in ops.iter().copied().enumerate() {
            let page_id = pg(page_from_workload(pattern, step, raw, domain));
            if op_kind % 3 == 0 {
                tracker.note_access(&fifo, page_id);
                let _ = fifo.access(page_id);
            } else {
                let events = fifo.insert(page_id);
                tracker.observe_events(&events);
            }
            tracker.assert_invariants(&fifo, capacity, bounds);
        }

        fifo
    }

    fn resident_set(fifo: &S3Fifo) -> HashSet<PageNumber> {
        fifo.small_pages()
            .into_iter()
            .chain(fifo.main_pages())
            .collect()
    }

    fn run_increasing_stream(capacity: usize, touches: &[bool]) -> S3Fifo {
        let mut fifo = S3Fifo::new(capacity);
        fifo.set_adaptation_interval(usize::MAX);
        for (index, touch) in touches.iter().copied().enumerate() {
            let page_u32 = u32::try_from(index.saturating_add(1)).unwrap_or(u32::MAX);
            let page_id = pg(page_u32.max(1));
            let _ = fifo.insert(page_id);
            if touch {
                let _ = fifo.access(page_id);
            }
        }
        fifo
    }

    fn next_lcg(state: &mut u64) -> u64 {
        const LCG_A: u64 = 6_364_136_223_846_793_005;
        const LCG_C: u64 = 1_442_695_040_888_963_407;
        *state = state.wrapping_mul(LCG_A).wrapping_add(LCG_C);
        *state
    }

    fn seeded_ops(seed: u64, len: usize) -> Vec<(u8, u16)> {
        let mut state = seed;
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            let op = u8::try_from(next_lcg(&mut state) & 0xFF).unwrap_or(u8::MAX);
            let raw = u16::try_from(next_lcg(&mut state) & 0xFFFF).unwrap_or(u16::MAX);
            out.push((op, raw));
        }
        out
    }

    const REGRESSION_SEEDS: [u64; 4] = [
        0x5EED_0000_0000_0001,
        0x5EED_0000_0000_0002,
        0x5EED_0000_0000_0011,
        0x5EED_0000_0000_0101,
    ];

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn prop_s3fifo_invariants_hold_for_random_sequences(
            capacity in 8_usize..64,
            pattern in 0_u8..4_u8,
            ops in vec(any::<(u8, u16)>(), 1..96),
        ) {
            let domain = u32::try_from(capacity.saturating_mul(8)).unwrap_or(u32::MAX);
            let _ = run_sequence_with_invariants(capacity, pattern, &ops, domain);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2_000))]

        #[test]
        fn prop_capacity_monotonicity_for_increasing_streams(
            capacity in 8_usize..63,
            touches in vec(any::<bool>(), 64..256),
        ) {
            let smaller = run_increasing_stream(capacity, &touches);
            let larger = run_increasing_stream(capacity + 1, &touches);
            let smaller_resident = resident_set(&smaller);
            let larger_resident = resident_set(&larger);
            assert!(
                smaller_resident.is_subset(&larger_resident),
                "resident set must be monotone for C -> C+1 on increasing streams"
            );
        }
    }

    #[test]
    fn adversarial_scan_pollution_sequence_preserves_invariants() {
        let capacity = 16_usize;
        let bounds = default_adaptive_bounds(capacity);
        let mut fifo = S3Fifo::new(capacity);
        fifo.set_adaptation_interval(4);
        let mut tracker = InvariantTracker::default();

        // Hot-set loop with periodic full scans to stress scan resistance.
        for round in 0_u32..200_u32 {
            for hot in 1_u32..=8_u32 {
                let page_id = pg(hot);
                tracker.note_access(&fifo, page_id);
                let _ = fifo.access(page_id);
                let events = fifo.insert(page_id);
                tracker.observe_events(&events);
                tracker.assert_invariants(&fifo, capacity, bounds);
            }
            let scan_page = pg(100_u32.saturating_add(round));
            let events = fifo.insert(scan_page);
            tracker.observe_events(&events);
            tracker.assert_invariants(&fifo, capacity, bounds);
        }
    }

    #[test]
    fn regression_seed_sequences_preserve_invariants() {
        for seed in REGRESSION_SEEDS {
            let ops = seeded_ops(seed, 256);
            let _ = run_sequence_with_invariants(24, 3, &ops, 256);
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum WorkloadKind {
        SequentialScan,
        ZipfianRandom,
        MixedInterleaved,
        ScanPollution,
    }

    impl WorkloadKind {
        const ALL: [Self; 4] = [
            Self::SequentialScan,
            Self::ZipfianRandom,
            Self::MixedInterleaved,
            Self::ScanPollution,
        ];

        const fn as_str(self) -> &'static str {
            match self {
                Self::SequentialScan => "sequential_scan_x3",
                Self::ZipfianRandom => "zipfian_random_skew1",
                Self::MixedInterleaved => "mixed_50_50",
                Self::ScanPollution => "scan_pollution",
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct AccessOutcome {
        hit: bool,
        evicted: bool,
        metadata_ops: usize,
        adaptive_split_changed: bool,
    }

    impl AccessOutcome {
        const fn hit(metadata_ops: usize) -> Self {
            Self {
                hit: true,
                evicted: false,
                metadata_ops,
                adaptive_split_changed: false,
            }
        }

        const fn miss(evicted: bool, metadata_ops: usize, adaptive_split_changed: bool) -> Self {
            Self {
                hit: false,
                evicted,
                metadata_ops,
                adaptive_split_changed,
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    struct CacheStats {
        accesses: usize,
        hits: usize,
        misses: usize,
        compulsory_misses: usize,
        capacity_misses: usize,
        conflict_misses: usize,
        evictions: usize,
        metadata_ops: usize,
        elapsed_nanos: u128,
        max_metadata_entries: usize,
        adaptation_changes: usize,
        adaptation_last_change_access: Option<usize>,
    }

    impl CacheStats {
        fn note_access(
            &mut self,
            access_index: usize,
            outcome: AccessOutcome,
            metadata_entries: usize,
        ) {
            self.accesses = self.accesses.saturating_add(1);
            self.metadata_ops = self.metadata_ops.saturating_add(outcome.metadata_ops);
            self.max_metadata_entries = self.max_metadata_entries.max(metadata_entries);

            if outcome.hit {
                self.hits = self.hits.saturating_add(1);
            } else {
                self.misses = self.misses.saturating_add(1);
            }

            if outcome.evicted {
                self.evictions = self.evictions.saturating_add(1);
            }

            if outcome.adaptive_split_changed {
                self.adaptation_changes = self.adaptation_changes.saturating_add(1);
                self.adaptation_last_change_access = Some(access_index.saturating_add(1));
            }
        }

        #[inline]
        fn hit_rate(self) -> f64 {
            if self.accesses == 0 {
                return 0.0;
            }
            self.hits as f64 / self.accesses as f64
        }

        #[inline]
        fn metadata_ops_per_eviction(self) -> f64 {
            if self.evictions == 0 {
                return 0.0;
            }
            self.metadata_ops as f64 / self.evictions as f64
        }

        #[inline]
        fn nanos_per_eviction(self) -> f64 {
            if self.evictions == 0 {
                return 0.0;
            }
            self.elapsed_nanos as f64 / self.evictions as f64
        }

        #[inline]
        fn metadata_entries_per_capacity(self, capacity: usize) -> f64 {
            if capacity == 0 {
                return 0.0;
            }
            self.max_metadata_entries as f64 / capacity as f64
        }
    }

    #[derive(Debug, Clone)]
    struct BenchmarkRow {
        workload: WorkloadKind,
        capacity_percent: usize,
        capacity_pages: usize,
        s3_fifo: CacheStats,
        lru: CacheStats,
        arc: CacheStats,
    }

    #[derive(Debug, Clone)]
    struct ZipfSampler {
        cumulative_weights: Vec<u64>,
        total_weight: u64,
    }

    impl ZipfSampler {
        fn new(domain_pages: u32) -> Self {
            let page_count = usize::try_from(domain_pages).unwrap_or(usize::MAX).max(1);
            let mut cumulative_weights = Vec::with_capacity(page_count);
            let mut total_weight = 0_u64;
            for rank in 1..=page_count {
                let rank_u64 = u64::try_from(rank).unwrap_or(u64::MAX);
                let weight = (1_000_000_u64 / rank_u64).max(1);
                total_weight = total_weight.saturating_add(weight);
                cumulative_weights.push(total_weight);
            }
            Self {
                cumulative_weights,
                total_weight,
            }
        }

        fn sample(&self, state: &mut u64) -> u32 {
            if self.total_weight == 0 {
                return 1;
            }
            let pick = next_lcg(state) % self.total_weight;
            let index = self
                .cumulative_weights
                .partition_point(|prefix| *prefix <= pick);
            u32::try_from(index.saturating_add(1))
                .unwrap_or(u32::MAX)
                .max(1)
        }
    }

    #[derive(Debug, Clone)]
    struct LruSimulator {
        capacity: usize,
        queue: VecDeque<PageNumber>,
        members: HashSet<PageNumber>,
    }

    impl LruSimulator {
        fn new(capacity: usize) -> Self {
            Self {
                capacity: capacity.max(1),
                queue: VecDeque::new(),
                members: HashSet::new(),
            }
        }

        fn metadata_entries(&self) -> usize {
            self.queue.len()
        }

        fn access(&mut self, page_id: PageNumber) -> AccessOutcome {
            if self.members.contains(&page_id) {
                if let Some(position) = self
                    .queue
                    .iter()
                    .position(|candidate| *candidate == page_id)
                {
                    let _ = self.queue.remove(position);
                    self.queue.push_back(page_id);
                    return AccessOutcome::hit(position.saturating_add(2));
                }
                return AccessOutcome::hit(1);
            }

            let mut metadata_ops = 1_usize;
            let mut evicted = false;
            if self.queue.len() >= self.capacity {
                if let Some(victim) = self.queue.pop_front() {
                    let _ = self.members.remove(&victim);
                    metadata_ops = metadata_ops.saturating_add(2);
                    evicted = true;
                }
            }
            self.queue.push_back(page_id);
            let _ = self.members.insert(page_id);
            metadata_ops = metadata_ops.saturating_add(2);
            AccessOutcome::miss(evicted, metadata_ops, false)
        }
    }

    #[derive(Debug, Clone)]
    struct ArcSimulator {
        capacity: usize,
        target_t1: usize,
        t1: VecDeque<PageNumber>,
        t2: VecDeque<PageNumber>,
        b1: VecDeque<PageNumber>,
        b2: VecDeque<PageNumber>,
    }

    impl ArcSimulator {
        fn new(capacity: usize) -> Self {
            Self {
                capacity: capacity.max(1),
                target_t1: 0,
                t1: VecDeque::new(),
                t2: VecDeque::new(),
                b1: VecDeque::new(),
                b2: VecDeque::new(),
            }
        }

        fn metadata_entries(&self) -> usize {
            self.t1
                .len()
                .saturating_add(self.t2.len())
                .saturating_add(self.b1.len())
                .saturating_add(self.b2.len())
        }

        fn access(&mut self, page_id: PageNumber) -> AccessOutcome {
            let mut metadata_ops = 0_usize;

            if let Some(position) = self.t1.iter().position(|candidate| *candidate == page_id) {
                let _ = self.t1.remove(position);
                self.t2.push_back(page_id);
                metadata_ops = metadata_ops.saturating_add(position.saturating_add(2));
                return AccessOutcome::hit(metadata_ops);
            }

            if let Some(position) = self.t2.iter().position(|candidate| *candidate == page_id) {
                let _ = self.t2.remove(position);
                self.t2.push_back(page_id);
                metadata_ops = metadata_ops.saturating_add(position.saturating_add(2));
                return AccessOutcome::hit(metadata_ops);
            }

            if let Some(position) = self.b1.iter().position(|candidate| *candidate == page_id) {
                let b1_len = self.b1.len().max(1);
                let delta = (self.b2.len() / b1_len).max(1);
                self.target_t1 = self.target_t1.saturating_add(delta).min(self.capacity);
                let (evicted, replace_ops) = self.replace(false);
                metadata_ops = metadata_ops.saturating_add(position.saturating_add(1));
                metadata_ops = metadata_ops.saturating_add(replace_ops);
                let _ = self.b1.remove(position);
                self.t2.push_back(page_id);
                metadata_ops = metadata_ops.saturating_add(2);
                return AccessOutcome::miss(evicted, metadata_ops, false);
            }

            if let Some(position) = self.b2.iter().position(|candidate| *candidate == page_id) {
                let b2_len = self.b2.len().max(1);
                let delta = (self.b1.len() / b2_len).max(1);
                self.target_t1 = self.target_t1.saturating_sub(delta);
                let (evicted, replace_ops) = self.replace(true);
                metadata_ops = metadata_ops.saturating_add(position.saturating_add(1));
                metadata_ops = metadata_ops.saturating_add(replace_ops);
                let _ = self.b2.remove(position);
                self.t2.push_back(page_id);
                metadata_ops = metadata_ops.saturating_add(2);
                return AccessOutcome::miss(evicted, metadata_ops, false);
            }

            let mut evicted = false;
            let t1_plus_b1 = self.t1.len().saturating_add(self.b1.len());
            if t1_plus_b1 == self.capacity {
                if self.t1.len() < self.capacity {
                    if self.b1.pop_front().is_some() {
                        metadata_ops = metadata_ops.saturating_add(1);
                    }
                    let (did_evict, replace_ops) = self.replace(false);
                    metadata_ops = metadata_ops.saturating_add(replace_ops);
                    evicted |= did_evict;
                } else if self.t1.pop_front().is_some() {
                    metadata_ops = metadata_ops.saturating_add(1);
                    evicted = true;
                }
            } else {
                let total = self.metadata_entries();
                if total >= self.capacity {
                    if total >= self.capacity.saturating_mul(2) && self.b2.pop_front().is_some() {
                        metadata_ops = metadata_ops.saturating_add(1);
                    }
                    let (did_evict, replace_ops) = self.replace(false);
                    metadata_ops = metadata_ops.saturating_add(replace_ops);
                    evicted |= did_evict;
                }
            }

            self.t1.push_back(page_id);
            metadata_ops = metadata_ops.saturating_add(1);
            AccessOutcome::miss(evicted, metadata_ops, false)
        }

        fn replace(&mut self, request_from_b2: bool) -> (bool, usize) {
            let mut metadata_ops = 0_usize;
            let choose_t1 = !self.t1.is_empty()
                && (self.t1.len() > self.target_t1
                    || (request_from_b2 && self.t1.len() == self.target_t1));

            if choose_t1 {
                if let Some(victim) = self.t1.pop_front() {
                    self.b1.push_back(victim);
                    metadata_ops = metadata_ops.saturating_add(2);
                    if self.b1.len() > self.capacity {
                        let _ = self.b1.pop_front();
                        metadata_ops = metadata_ops.saturating_add(1);
                    }
                    return (true, metadata_ops);
                }
                return (false, metadata_ops);
            }

            if let Some(victim) = self.t2.pop_front() {
                self.b2.push_back(victim);
                metadata_ops = metadata_ops.saturating_add(2);
                if self.b2.len() > self.capacity {
                    let _ = self.b2.pop_front();
                    metadata_ops = metadata_ops.saturating_add(1);
                }
                return (true, metadata_ops);
            }

            (false, metadata_ops)
        }
    }

    fn sweep_capacity_pages(domain_pages: usize, percent: usize) -> usize {
        let scaled = domain_pages.saturating_mul(percent);
        let mut pages = scaled / 100;
        if scaled % 100 != 0 {
            pages = pages.saturating_add(1);
        }
        pages.max(1)
    }

    fn make_workload_trace(kind: WorkloadKind, domain_pages: u32, seed: u64) -> Vec<PageNumber> {
        let mut state = seed;
        let mut out = Vec::new();
        let domain_usize = usize::try_from(domain_pages).unwrap_or(usize::MAX).max(1);
        match kind {
            WorkloadKind::SequentialScan => {
                out.reserve(domain_usize.saturating_mul(3));
                for _ in 0..3 {
                    for page in 1..=domain_pages {
                        out.push(pg(page));
                    }
                }
            }
            WorkloadKind::ZipfianRandom => {
                let sampler = ZipfSampler::new(domain_pages);
                let draws = domain_usize.saturating_mul(30);
                out.reserve(draws);
                for _ in 0..draws {
                    out.push(pg(sampler.sample(&mut state)));
                }
            }
            WorkloadKind::MixedInterleaved => {
                let sampler = ZipfSampler::new(domain_pages);
                let draws = domain_usize.saturating_mul(30);
                out.reserve(draws);
                let mut scan_cursor = 1_u32;
                for index in 0..draws {
                    if index % 2 == 0 {
                        out.push(pg(scan_cursor));
                        scan_cursor = if scan_cursor >= domain_pages {
                            1
                        } else {
                            scan_cursor.saturating_add(1)
                        };
                    } else {
                        out.push(pg(sampler.sample(&mut state)));
                    }
                }
            }
            WorkloadKind::ScanPollution => {
                let hot_set = (domain_pages / 32).clamp(8, 32);
                let rounds = domain_usize.saturating_mul(4);
                let hot_burst = 128_usize;
                out.reserve(
                    rounds
                        .saturating_mul(hot_burst)
                        .saturating_add(domain_usize.saturating_mul(4)),
                );
                for round in 0..rounds {
                    for _ in 0..hot_burst {
                        let offset =
                            u32::try_from(next_lcg(&mut state) % u64::from(hot_set)).unwrap_or(0);
                        out.push(pg(offset.saturating_add(1)));
                    }
                    if round % 10 == 0 {
                        for page in 1..=domain_pages {
                            out.push(pg(page));
                        }
                    }
                }
            }
        }
        out
    }

    fn run_s3_fifo_trace(trace: &[PageNumber], capacity: usize) -> CacheStats {
        let mut stats = CacheStats::default();
        let mut seen_pages = HashSet::new();
        let mut cache = S3Fifo::new(capacity.max(1));
        cache.set_adaptation_interval(64);
        let started_at = std::time::Instant::now();

        for (access_index, page_id) in trace.iter().copied().enumerate() {
            if cache.access(page_id) {
                let metadata_entries = cache.resident_len().saturating_add(cache.ghost_len());
                stats.note_access(access_index, AccessOutcome::hit(1), metadata_entries);
                continue;
            }

            if seen_pages.insert(page_id) {
                stats.compulsory_misses = stats.compulsory_misses.saturating_add(1);
            } else {
                stats.capacity_misses = stats.capacity_misses.saturating_add(1);
            }
            let events = cache.insert(page_id);
            let mut evicted = false;
            let mut adaptive_split_changed = false;
            for event in &events {
                if matches!(
                    event,
                    S3FifoEvent::EvictedFromSmallToGhost(_)
                        | S3FifoEvent::EvictedFromMain(_)
                        | S3FifoEvent::GhostTrimmed(_)
                ) {
                    evicted = true;
                }
                if matches!(event, S3FifoEvent::AdaptiveSplitChanged { .. }) {
                    adaptive_split_changed = true;
                }
            }
            let metadata_entries = cache.resident_len().saturating_add(cache.ghost_len());
            stats.note_access(
                access_index,
                AccessOutcome::miss(evicted, events.len().max(1), adaptive_split_changed),
                metadata_entries,
            );
        }

        stats.elapsed_nanos = started_at.elapsed().as_nanos();
        stats
    }

    fn run_lru_trace(trace: &[PageNumber], capacity: usize) -> CacheStats {
        let mut stats = CacheStats::default();
        let mut seen_pages = HashSet::new();
        let mut cache = LruSimulator::new(capacity.max(1));
        let started_at = std::time::Instant::now();

        for (access_index, page_id) in trace.iter().copied().enumerate() {
            let outcome = cache.access(page_id);
            if !outcome.hit {
                if seen_pages.insert(page_id) {
                    stats.compulsory_misses = stats.compulsory_misses.saturating_add(1);
                } else {
                    stats.capacity_misses = stats.capacity_misses.saturating_add(1);
                }
            }
            stats.note_access(access_index, outcome, cache.metadata_entries());
        }

        stats.elapsed_nanos = started_at.elapsed().as_nanos();
        stats
    }

    fn run_arc_trace(trace: &[PageNumber], capacity: usize) -> CacheStats {
        let mut stats = CacheStats::default();
        let mut seen_pages = HashSet::new();
        let mut cache = ArcSimulator::new(capacity.max(1));
        let started_at = std::time::Instant::now();

        for (access_index, page_id) in trace.iter().copied().enumerate() {
            let outcome = cache.access(page_id);
            if !outcome.hit {
                if seen_pages.insert(page_id) {
                    stats.compulsory_misses = stats.compulsory_misses.saturating_add(1);
                } else {
                    stats.capacity_misses = stats.capacity_misses.saturating_add(1);
                }
            }
            stats.note_access(access_index, outcome, cache.metadata_entries());
        }

        stats.elapsed_nanos = started_at.elapsed().as_nanos();
        stats
    }

    fn format_percent(value: f64) -> String {
        format!("{:.2}%", value * 100.0)
    }

    fn render_benchmark_report(rows: &[BenchmarkRow], domain_pages: u32) -> String {
        let mut report = String::new();
        report.push_str("# S3-FIFO vs LRU vs ARC Benchmark Report\n\n");
        report.push_str("Generated by `s3_fifo_benchmark_matrix_meets_acceptance_contract`.\n\n");
        let line = format!("- Domain pages: {domain_pages}\n");
        report.push_str(&line);
        report.push_str("- Buffer sizes: 10%, 25%, 50%, 75%\n");
        report.push_str(
            "- Workloads: sequential scan x3, zipfian random, mixed 50/50, scan pollution\n\n",
        );
        report.push_str("## Hit-Rate Matrix\n\n");
        report.push_str("| Workload | Capacity | S3-FIFO Hit | LRU Hit | ARC Hit |\n");
        report.push_str("|---|---:|---:|---:|---:|\n");
        for row in rows {
            let line = format!(
                "| {} | {}% ({}) | {} | {} | {} |\n",
                row.workload.as_str(),
                row.capacity_percent,
                row.capacity_pages,
                format_percent(row.s3_fifo.hit_rate()),
                format_percent(row.lru.hit_rate()),
                format_percent(row.arc.hit_rate()),
            );
            report.push_str(&line);
        }

        report.push_str("\n## Overhead Metrics\n\n");
        report.push_str("| Workload | Capacity | S3 ops/evict | LRU ops/evict | ARC ops/evict | S3 ns/evict | LRU ns/evict | ARC ns/evict |\n");
        report.push_str("|---|---:|---:|---:|---:|---:|---:|---:|\n");
        for row in rows {
            let line = format!(
                "| {} | {}% | {:.2} | {:.2} | {:.2} | {:.1} | {:.1} | {:.1} |\n",
                row.workload.as_str(),
                row.capacity_percent,
                row.s3_fifo.metadata_ops_per_eviction(),
                row.lru.metadata_ops_per_eviction(),
                row.arc.metadata_ops_per_eviction(),
                row.s3_fifo.nanos_per_eviction(),
                row.lru.nanos_per_eviction(),
                row.arc.nanos_per_eviction(),
            );
            report.push_str(&line);
        }

        report.push_str("\n## Metadata Overhead Per Resident Capacity\n\n");
        report.push_str(
            "| Workload | Capacity | S3 metadata xC | LRU metadata xC | ARC metadata xC |\n",
        );
        report.push_str("|---|---:|---:|---:|---:|\n");
        for row in rows {
            let line = format!(
                "| {} | {}% | {:.2} | {:.2} | {:.2} |\n",
                row.workload.as_str(),
                row.capacity_percent,
                row.s3_fifo
                    .metadata_entries_per_capacity(row.capacity_pages),
                row.lru.metadata_entries_per_capacity(row.capacity_pages),
                row.arc.metadata_entries_per_capacity(row.capacity_pages),
            );
            report.push_str(&line);
        }

        report.push_str("\n## Adaptive Convergence (S3-FIFO)\n\n");
        report.push_str("| Workload | Capacity | Adaptation changes | Last change access |\n");
        report.push_str("|---|---:|---:|---:|\n");
        for row in rows {
            let last_change = row
                .s3_fifo
                .adaptation_last_change_access
                .map_or_else(|| String::from("none"), |value| value.to_string());
            let line = format!(
                "| {} | {}% | {} | {} |\n",
                row.workload.as_str(),
                row.capacity_percent,
                row.s3_fifo.adaptation_changes,
                last_change,
            );
            report.push_str(&line);
        }

        report
    }

    fn write_benchmark_report(report: &str) -> std::path::PathBuf {
        let target_dir =
            std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| String::from("target"));
        let mut path = std::path::PathBuf::from(target_dir);
        path.push("s3_fifo_benchmark_report.md");
        if let Some(parent) = path.parent() {
            if let Err(error) = std::fs::create_dir_all(parent) {
                panic!("failed to create benchmark report directory: {error}");
            }
        }
        if let Err(error) = std::fs::write(&path, report) {
            panic!("failed to write benchmark report: {error}");
        }
        path
    }

    fn collect_benchmark_rows(domain_pages: u32, capacity_percents: &[usize]) -> Vec<BenchmarkRow> {
        let domain_usize = usize::try_from(domain_pages).unwrap_or(usize::MAX);
        let mut rows = Vec::new();

        for (workload_index, workload) in WorkloadKind::ALL.iter().copied().enumerate() {
            let seed = 0x51F0_0000_0000_0000_u64
                .saturating_add(u64::try_from(workload_index).unwrap_or(u64::MAX));
            let trace = make_workload_trace(workload, domain_pages, seed);
            for capacity_percent in capacity_percents {
                let capacity_pages = sweep_capacity_pages(domain_usize, *capacity_percent);
                let s3_fifo = run_s3_fifo_trace(&trace, capacity_pages);
                let lru = run_lru_trace(&trace, capacity_pages);
                let arc = run_arc_trace(&trace, capacity_pages);
                rows.push(BenchmarkRow {
                    workload,
                    capacity_percent: *capacity_percent,
                    capacity_pages,
                    s3_fifo,
                    lru,
                    arc,
                });
            }
        }

        rows
    }

    fn assert_miss_breakdown(stats: CacheStats) {
        assert_eq!(
            stats.misses,
            stats
                .compulsory_misses
                .saturating_add(stats.capacity_misses)
                .saturating_add(stats.conflict_misses)
        );
    }

    fn assert_hit_rate_contract(row: &BenchmarkRow) {
        match row.workload {
            WorkloadKind::ZipfianRandom => {
                assert!(
                    row.s3_fifo.hit_rate() + 0.08 >= row.arc.hit_rate(),
                    "S3-FIFO hit rate {:.4} too far below ARC {:.4} on Zipfian workload at {}%",
                    row.s3_fifo.hit_rate(),
                    row.arc.hit_rate(),
                    row.capacity_percent
                );
            }
            WorkloadKind::MixedInterleaved => {
                assert!(
                    row.s3_fifo.hit_rate() + 0.05 >= row.arc.hit_rate(),
                    "S3-FIFO hit rate {:.4} too far below ARC {:.4} on mixed workload at {}%",
                    row.s3_fifo.hit_rate(),
                    row.arc.hit_rate(),
                    row.capacity_percent
                );
            }
            WorkloadKind::ScanPollution => {
                assert!(
                    row.s3_fifo.hit_rate() >= row.lru.hit_rate() + 0.005,
                    "S3-FIFO hit rate {:.4} must exceed LRU {:.4} by >=0.5% on scan-pollution workload at {}%",
                    row.s3_fifo.hit_rate(),
                    row.lru.hit_rate(),
                    row.capacity_percent
                );
            }
            WorkloadKind::SequentialScan => {
                assert!(
                    row.s3_fifo.hit_rate() + 0.10 >= row.arc.hit_rate(),
                    "S3-FIFO hit rate {:.4} too far below ARC {:.4} on sequential workload at {}%",
                    row.s3_fifo.hit_rate(),
                    row.arc.hit_rate(),
                    row.capacity_percent
                );
            }
        }
    }

    fn assert_overhead_contract(row: &BenchmarkRow) {
        assert!(
            row.s3_fifo.metadata_ops_per_eviction() <= row.arc.metadata_ops_per_eviction() + 6.0,
            "S3-FIFO eviction metadata overhead {:.2} too high vs ARC {:.2} at {}% {}",
            row.s3_fifo.metadata_ops_per_eviction(),
            row.arc.metadata_ops_per_eviction(),
            row.capacity_percent,
            row.workload.as_str()
        );
    }

    #[test]
    fn s3_fifo_benchmark_matrix_meets_acceptance_contract() {
        let domain_pages = 512_u32;
        let capacity_percents = [10_usize, 25, 50, 75];
        let rows = collect_benchmark_rows(domain_pages, &capacity_percents);

        assert_eq!(
            rows.len(),
            WorkloadKind::ALL
                .len()
                .saturating_mul(capacity_percents.len())
        );

        for row in &rows {
            assert_miss_breakdown(row.s3_fifo);
            assert_miss_breakdown(row.lru);
            assert_miss_breakdown(row.arc);
            assert_hit_rate_contract(row);
            assert_overhead_contract(row);
        }

        let report = render_benchmark_report(&rows, domain_pages);
        let report_path = write_benchmark_report(&report);
        assert!(report.contains("| Workload | Capacity | S3-FIFO Hit | LRU Hit | ARC Hit |"));
        assert!(report_path.exists());
    }

    #[derive(Debug, Clone)]
    struct IntegrityStore {
        values: HashMap<PageNumber, u64>,
        checksums: HashMap<PageNumber, blake3::Hash>,
    }

    impl IntegrityStore {
        fn new(page_count: u32) -> Self {
            let mut values = HashMap::new();
            let mut checksums = HashMap::new();
            for page in 1..=page_count {
                let page_id = pg(page);
                let value = u64::from(page).saturating_mul(1_000_003);
                values.insert(page_id, value);
                checksums.insert(page_id, Self::checksum(value));
            }
            Self { values, checksums }
        }

        fn read(&self, page_id: PageNumber) -> u64 {
            match self.values.get(&page_id).copied() {
                Some(value) => value,
                None => panic!("missing page value for {page_id:?}"),
            }
        }

        fn write(&mut self, page_id: PageNumber, value: u64) {
            self.values.insert(page_id, value);
            self.checksums.insert(page_id, Self::checksum(value));
        }

        fn verify(&self, page_id: PageNumber) -> bool {
            let Some(value) = self.values.get(&page_id).copied() else {
                return false;
            };
            let Some(expected) = self.checksums.get(&page_id).copied() else {
                return false;
            };
            Self::checksum(value) == expected
        }

        fn verify_all(&self) -> bool {
            self.values.keys().all(|page_id| self.verify(*page_id))
        }

        fn checksum(value: u64) -> blake3::Hash {
            blake3::hash(&value.to_le_bytes())
        }
    }

    fn page_range(start: u32, end_inclusive: u32) -> Vec<PageNumber> {
        let mut out = Vec::new();
        for page in start..=end_inclusive {
            out.push(pg(page));
        }
        out
    }

    fn select_page(seed: &mut u64, pages: &[PageNumber]) -> PageNumber {
        assert!(!pages.is_empty(), "page selection set must be non-empty");
        let page_count = u64::try_from(pages.len()).unwrap_or(u64::MAX).max(1);
        let index_u64 = next_lcg(seed) % page_count;
        let index = usize::try_from(index_u64).unwrap_or(0);
        pages[index]
    }

    fn access_with_integrity(
        fifo: &mut S3Fifo,
        store: &IntegrityStore,
        page_id: PageNumber,
    ) -> (bool, Vec<S3FifoEvent>) {
        assert!(store.verify(page_id), "checksum mismatch before access");
        if fifo.access(page_id) {
            assert!(store.verify(page_id), "checksum mismatch after hit access");
            return (true, Vec::new());
        }
        let events = fifo.insert(page_id);
        assert!(
            store.verify(page_id),
            "checksum mismatch after miss insertion"
        );
        (false, events)
    }

    fn write_with_integrity(
        fifo: &mut S3Fifo,
        store: &mut IntegrityStore,
        page_id: PageNumber,
        delta: u64,
    ) -> (bool, Vec<S3FifoEvent>) {
        let next_value = store.read(page_id).saturating_add(delta);
        store.write(page_id, next_value);
        access_with_integrity(fifo, store, page_id)
    }

    #[test]
    fn e2e_scan_pollution_preserves_hot_set_and_recovers_hit_rate() {
        let mut fifo = S3Fifo::new(64);
        fifo.set_adaptation_interval(32);
        let store = IntegrityStore::new(640);
        let hot_set = page_range(1, 32);
        let mut seed = 0xABCD_EE01_u64;

        for _ in 0..4_000 {
            let page_id = select_page(&mut seed, &hot_set);
            let _ = access_with_integrity(&mut fifo, &store, page_id);
        }

        for page in 1_u32..=640_u32 {
            let page_id = pg(page);
            let _ = access_with_integrity(&mut fifo, &store, page_id);
        }

        let mut recovery_hits = 0_usize;
        let recovery_window = 1_000_usize;
        for _ in 0..recovery_window {
            let page_id = select_page(&mut seed, &hot_set);
            let (hit, _) = access_with_integrity(&mut fifo, &store, page_id);
            if hit {
                recovery_hits = recovery_hits.saturating_add(1);
            }
        }
        let recovery_rate = recovery_hits as f64 / recovery_window as f64;
        assert!(
            recovery_rate >= 0.01,
            "hit rate should recover within 1000 accesses after scan (observed {:.4})",
            recovery_rate
        );
    }

    #[test]
    fn e2e_working_set_shift_adapts_and_reaches_steady_state() {
        let mut fifo = S3Fifo::new(80);
        fifo.set_adaptive_bounds(8, 40);
        fifo.set_adaptation_interval(16);
        let store = IntegrityStore::new(1_000);
        let working_set_a = page_range(1, 40);
        let working_set_b = page_range(81, 120);
        let mut seed = 0xAA55_BEEF_u64;

        for _ in 0..4_000 {
            let page_id = select_page(&mut seed, &working_set_a);
            let _ = access_with_integrity(&mut fifo, &store, page_id);
        }

        let mut adaptation_changes = 0_usize;
        let mut steady_state_at = None;
        let mut window_hits = 0_usize;
        let mut window = VecDeque::new();
        for step in 0..5_000_usize {
            let page_id = select_page(&mut seed, &working_set_b);
            let (hit, events) = access_with_integrity(&mut fifo, &store, page_id);
            if hit {
                window_hits = window_hits.saturating_add(1);
            }
            window.push_back(hit);
            if window.len() > 500 {
                let removed = window.pop_front().unwrap_or(false);
                if removed {
                    window_hits = window_hits.saturating_sub(1);
                }
            }
            if events
                .iter()
                .any(|event| matches!(event, S3FifoEvent::AdaptiveSplitChanged { .. }))
            {
                adaptation_changes = adaptation_changes.saturating_add(1);
            }
            if steady_state_at.is_none() && window.len() == 500 {
                let rate = window_hits as f64 / 500.0;
                if rate >= 0.75 {
                    steady_state_at = Some(step.saturating_add(1));
                }
            }
        }

        assert!(
            adaptation_changes > 0,
            "working-set shift should trigger adaptive split changes"
        );
        let Some(steady_state_access) = steady_state_at else {
            panic!("working-set B did not reach steady-state hit-rate");
        };
        assert!(
            steady_state_access <= 5_000,
            "working set B should converge within 5000 accesses"
        );
    }

    #[test]
    fn e2e_memory_pressure_preserves_checksum_integrity() {
        let mut fifo = S3Fifo::new(100); // 10% of DB pages.
        fifo.set_adaptation_interval(32);
        let mut store = IntegrityStore::new(1_000);
        let mut seed = 0xDD44_99AA_u64;

        for step in 0_u32..12_000_u32 {
            let page_id = if step % 3 == 0 {
                pg(step % 1_000 + 1)
            } else {
                let candidate = u32::try_from(next_lcg(&mut seed) % 1_000).unwrap_or(0) + 1;
                pg(candidate)
            };
            let _ = access_with_integrity(&mut fifo, &store, page_id);
            if step % 17 == 0 {
                let _ = write_with_integrity(&mut fifo, &mut store, page_id, 1);
            }
        }

        assert!(
            store.verify_all(),
            "BLAKE3 checksums must remain valid after eviction/reload cycles"
        );
        assert!(fifo.resident_len() <= fifo.config().capacity());
    }

    #[derive(Debug)]
    struct SharedHarness {
        fifo: S3Fifo,
        store: IntegrityStore,
    }

    impl SharedHarness {
        fn read_access(&mut self, page_id: PageNumber) -> (bool, Vec<S3FifoEvent>) {
            let store_ref = &self.store;
            let fifo_ref = &mut self.fifo;
            access_with_integrity(fifo_ref, store_ref, page_id)
        }

        fn write_access(&mut self, page_id: PageNumber, delta: u64) -> (bool, Vec<S3FifoEvent>) {
            let Self { fifo, store } = self;
            write_with_integrity(fifo, store, page_id, delta)
        }
    }

    #[test]
    fn e2e_concurrent_readers_writers_preserve_integrity() {
        let shared = Arc::new(Mutex::new(SharedHarness {
            fifo: S3Fifo::new(96),
            store: IntegrityStore::new(512),
        }));

        std::thread::scope(|scope| {
            for reader_id in 0_u64..16_u64 {
                let shared_clone = Arc::clone(&shared);
                scope.spawn(move || {
                    let mut seed = 0x1000_0000_u64.saturating_add(reader_id);
                    for _ in 0..400 {
                        let page_u32 = u32::try_from(next_lcg(&mut seed) % 512).unwrap_or(0) + 1;
                        let page_id = pg(page_u32);
                        let mut guard = match shared_clone.lock() {
                            Ok(guard) => guard,
                            Err(error) => panic!("reader mutex poisoned: {error}"),
                        };
                        let _ = guard.read_access(page_id);
                    }
                });
            }

            for writer_id in 0_u64..4_u64 {
                let shared_clone = Arc::clone(&shared);
                scope.spawn(move || {
                    let mut seed = 0x2000_0000_u64.saturating_add(writer_id);
                    for _ in 0..250 {
                        let page_u32 = u32::try_from(next_lcg(&mut seed) % 128).unwrap_or(0) + 1;
                        let page_id = pg(page_u32);
                        let mut guard = match shared_clone.lock() {
                            Ok(guard) => guard,
                            Err(error) => panic!("writer mutex poisoned: {error}"),
                        };
                        let _ = guard.write_access(page_id, 1_u64.saturating_add(writer_id));
                    }
                });
            }
        });

        let guard = match shared.lock() {
            Ok(guard) => guard,
            Err(error) => panic!("final mutex poisoned: {error}"),
        };
        assert!(guard.store.verify_all());
        assert!(guard.fifo.resident_len() <= guard.fifo.config().capacity());
        drop(guard);
    }

    #[test]
    fn e2e_ghost_queue_readmission_drives_adaptation_and_promotion() {
        let mut fifo = S3Fifo::new(32);
        fifo.set_adaptive_bounds(4, 16);
        fifo.set_adaptation_interval(8);
        let store = IntegrityStore::new(1_000);

        for page in 1_u32..=96_u32 {
            let page_id = pg(page);
            let _ = access_with_integrity(&mut fifo, &store, page_id);
        }
        let ghost_candidates: Vec<PageNumber> = fifo.ghost_pages().into_iter().take(8).collect();
        assert!(
            !ghost_candidates.is_empty(),
            "ghost queue should be populated before readmission test"
        );

        let mut ghost_readmissions = 0_usize;
        let mut adaptive_changes = 0_usize;
        let mut promoted_to_main = false;
        let mut filler = 200_u32;
        for candidate in ghost_candidates {
            let (_, readmit_events) = access_with_integrity(&mut fifo, &store, candidate);
            for event in &readmit_events {
                if matches!(event, S3FifoEvent::GhostReadmission(page_id) if *page_id == candidate)
                {
                    ghost_readmissions = ghost_readmissions.saturating_add(1);
                }
            }
            let _ = fifo.access(candidate);
            for _ in 0..32 {
                filler = filler.saturating_add(1);
                let (_, events) = access_with_integrity(&mut fifo, &store, pg(filler));
                for event in &events {
                    if matches!(event, S3FifoEvent::AdaptiveSplitChanged { .. }) {
                        adaptive_changes = adaptive_changes.saturating_add(1);
                    }
                }
            }
            if matches!(
                fifo.lookup(candidate),
                Some(QueueLocation {
                    kind: QueueKind::Main
                })
            ) {
                promoted_to_main = true;
            }
        }

        assert!(
            ghost_readmissions > 0,
            "ghost queue should produce readmissions"
        );
        assert!(
            adaptive_changes > 0,
            "ghost readmissions should contribute adaptation signal"
        );
        assert!(
            promoted_to_main,
            "re-accessed pages should be promotable to MAIN after readmission"
        );
    }
}
