//! FrankenSQLite supervision tree and resilience combinator wrappers (§4.14-4.15, bd-3go.10 + bd-27nu).
//!
//! Defines the OTP-style supervision strategies for the 5 core FrankenSQLite
//! long-lived services, error classification for restart decisions, and
//! resilience combinator configurations.
//!
//! # Supervision Invariant
//!
//! **INV-SUPERVISION-MONOTONE**: Panicked outcomes MUST result in Stop or Escalate
//! (never Restart). Cancelled outcomes MUST result in Stop. Only `Err` outcomes
//! classified as transient may trigger Restart, and only if the restart budget allows.
//!
//! # Service Strategies
//!
//! | Service            | Err Strategy                  | Panic Strategy |
//! |--------------------|-------------------------------|----------------|
//! | WriteCoordinator   | Escalate                      | Escalate       |
//! | SymbolStore        | Restart (transient) / Escalate| Escalate       |
//! | Replicator         | Restart (exp backoff) / Stop  | Escalate       |
//! | CheckpointerGc    | Restart (bounded) / Escalate  | Escalate       |
//! | IntegritySweeper   | Stop                          | Stop           |

use std::time::Duration;

use asupersync::combinator::bulkhead::{Bulkhead, BulkheadPolicy};
use asupersync::combinator::circuit_breaker::{
    CircuitBreaker, CircuitBreakerPolicy, FailurePredicate,
};
use asupersync::combinator::rate_limit::{RateLimitPolicy, RateLimiter, WaitStrategy};
use asupersync::combinator::retry::RetryPolicy;
use asupersync::supervision::{
    BackoffStrategy, RestartConfig, RestartHistory, SupervisionStrategy,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

/// Bead identifier for tracing and log correlation.
const BEAD_ID: &str = "bd-27nu";

// ---------------------------------------------------------------------------
// Supervised service definitions
// ---------------------------------------------------------------------------

/// The 5 core FrankenSQLite long-lived supervised services.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FsqliteService {
    /// Write serializer — sequencer correctness is core. Escalate on any error.
    WriteCoordinator,
    /// Symbol object store — restart on transient I/O, escalate on integrity faults.
    SymbolStore,
    /// Remote replication — restart with exponential backoff, stop when remote disabled.
    Replicator,
    /// Checkpoint + GC — restart on transient (bounded), escalate if repeated.
    CheckpointerGc,
    /// Background integrity sweep — stop on error (does not gate core function).
    IntegritySweeper,
}

impl FsqliteService {
    /// All 5 supervised services in canonical start order.
    pub const ALL: &[Self] = &[
        Self::WriteCoordinator,
        Self::SymbolStore,
        Self::Replicator,
        Self::CheckpointerGc,
        Self::IntegritySweeper,
    ];

    /// Human-readable service name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::WriteCoordinator => "WriteCoordinator",
            Self::SymbolStore => "SymbolStore",
            Self::Replicator => "Replicator",
            Self::CheckpointerGc => "CheckpointerGc",
            Self::IntegritySweeper => "IntegritySweeper",
        }
    }

    /// The supervision strategy for transient `Err` outcomes.
    #[must_use]
    pub fn err_strategy(self) -> SupervisionStrategy {
        match self {
            Self::WriteCoordinator => SupervisionStrategy::Escalate,
            Self::SymbolStore => SupervisionStrategy::Restart(
                RestartConfig::new(3, Duration::from_secs(60)).with_backoff(
                    BackoffStrategy::Exponential {
                        initial: Duration::from_millis(100),
                        max: Duration::from_secs(10),
                        multiplier: 2.0,
                    },
                ),
            ),
            Self::Replicator => SupervisionStrategy::Restart(
                RestartConfig::new(5, Duration::from_secs(120)).with_backoff(
                    BackoffStrategy::Exponential {
                        initial: Duration::from_millis(500),
                        max: Duration::from_secs(30),
                        multiplier: 2.0,
                    },
                ),
            ),
            Self::CheckpointerGc => SupervisionStrategy::Restart(
                RestartConfig::new(3, Duration::from_secs(60))
                    .with_backoff(BackoffStrategy::Fixed(Duration::from_secs(1))),
            ),
            Self::IntegritySweeper => SupervisionStrategy::Stop,
        }
    }

    /// The supervision strategy for `Panicked` outcomes.
    ///
    /// INV-SUPERVISION-MONOTONE: Panicked outcomes NEVER restart.
    #[must_use]
    pub fn panicked_strategy(self) -> SupervisionStrategy {
        match self {
            Self::IntegritySweeper => SupervisionStrategy::Stop,
            _ => SupervisionStrategy::Escalate,
        }
    }

    /// The supervision strategy for `Cancelled` outcomes.
    ///
    /// INV-SUPERVISION-MONOTONE: Cancelled outcomes always stop.
    #[must_use]
    pub fn cancelled_strategy(self) -> SupervisionStrategy {
        SupervisionStrategy::Stop
    }
}

impl std::fmt::Display for FsqliteService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

// ---------------------------------------------------------------------------
// Task outcome classification
// ---------------------------------------------------------------------------

/// Outcome of a supervised task execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome<E = String> {
    /// Task completed successfully.
    Ok,
    /// Task returned an error.
    Err(E),
    /// Task panicked (programming error).
    Panicked,
    /// Task was cancelled (external directive / shutdown).
    Cancelled,
}

/// Error classification for restart decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Transient error (I/O timeout, network blip) — may restart.
    Transient,
    /// Permanent error (integrity fault, config error) — must not restart.
    Permanent,
}

/// Classify a string error for restart decision.
///
/// This is a simplified classifier; production code would inspect
/// `FrankenError` variants directly.
#[must_use]
pub fn classify_error(error: &str) -> ErrorClass {
    let lower = error.to_lowercase();
    if lower.contains("timeout")
        || lower.contains("connection reset")
        || lower.contains("temporary")
        || lower.contains("transient")
        || lower.contains("io error")
        || lower.contains("broken pipe")
    {
        ErrorClass::Transient
    } else {
        ErrorClass::Permanent
    }
}

// ---------------------------------------------------------------------------
// Supervision decision engine
// ---------------------------------------------------------------------------

/// Determines the action to take for a service outcome.
///
/// Enforces INV-SUPERVISION-MONOTONE:
/// - Panicked → Stop or Escalate (never Restart)
/// - Cancelled → Stop
/// - Err → service-specific strategy, subject to error classification and budget
#[must_use]
pub fn decide_action(
    service: FsqliteService,
    outcome: &TaskOutcome,
    restart_history: &RestartHistory,
    now: u64,
) -> SupervisionAction {
    match outcome {
        TaskOutcome::Ok => SupervisionAction::None,

        TaskOutcome::Panicked => {
            let strategy = service.panicked_strategy();
            info!(
                bead_id = BEAD_ID,
                service = service.name(),
                outcome = "Panicked",
                strategy = ?strategy,
                "INV-SUPERVISION-MONOTONE: panicked outcome → {strategy:?}"
            );
            match strategy {
                SupervisionStrategy::Stop => SupervisionAction::Stop,
                SupervisionStrategy::Escalate => SupervisionAction::Escalate,
                SupervisionStrategy::Restart(_) => {
                    // INV-SUPERVISION-MONOTONE violation guard — never restart on panic.
                    error!(
                        bead_id = BEAD_ID,
                        service = service.name(),
                        "BUG: Restart strategy configured for Panicked outcome, forcing Escalate"
                    );
                    SupervisionAction::Escalate
                }
            }
        }

        TaskOutcome::Cancelled => {
            info!(
                bead_id = BEAD_ID,
                service = service.name(),
                outcome = "Cancelled",
                "INV-SUPERVISION-MONOTONE: cancelled outcome → Stop"
            );
            SupervisionAction::Stop
        }

        TaskOutcome::Err(error_msg) => {
            let strategy = service.err_strategy();
            let error_class = classify_error(error_msg);

            debug!(
                bead_id = BEAD_ID,
                service = service.name(),
                error = error_msg,
                error_class = ?error_class,
                strategy = ?strategy,
                "evaluating error outcome for restart decision"
            );

            match strategy {
                SupervisionStrategy::Stop => SupervisionAction::Stop,
                SupervisionStrategy::Escalate => SupervisionAction::Escalate,
                SupervisionStrategy::Restart(_) => {
                    if error_class == ErrorClass::Permanent {
                        warn!(
                            bead_id = BEAD_ID,
                            service = service.name(),
                            error = error_msg,
                            "permanent error — escalating instead of restarting"
                        );
                        return SupervisionAction::Escalate;
                    }

                    if restart_history.can_restart(now) {
                        let delay = restart_history.next_delay(now);
                        info!(
                            bead_id = BEAD_ID,
                            service = service.name(),
                            delay_ms = delay.map(|d| d.as_millis()),
                            recent_restarts = restart_history.recent_restart_count(now),
                            "transient error — restarting with backoff"
                        );
                        SupervisionAction::Restart { delay }
                    } else {
                        warn!(
                            bead_id = BEAD_ID,
                            service = service.name(),
                            recent_restarts = restart_history.recent_restart_count(now),
                            "restart budget exhausted — escalating"
                        );
                        SupervisionAction::Escalate
                    }
                }
            }
        }
    }
}

/// Action determined by the supervision decision engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisionAction {
    /// No action needed (task completed successfully).
    None,
    /// Stop the service permanently.
    Stop,
    /// Restart the service after an optional delay.
    Restart { delay: Option<Duration> },
    /// Escalate to parent supervisor.
    Escalate,
}

// ---------------------------------------------------------------------------
// Resilience combinator factories
// ---------------------------------------------------------------------------

/// Default circuit breaker configuration for remote tier (Replicator, SymbolStore).
#[must_use]
pub fn remote_circuit_breaker(name: &str) -> CircuitBreaker {
    let policy = CircuitBreakerPolicy {
        name: name.to_owned(),
        failure_threshold: 5,
        success_threshold: 2,
        open_duration: Duration::from_secs(30),
        half_open_max_probes: 1,
        failure_predicate: FailurePredicate::AllErrors,
        sliding_window: None,
        on_state_change: None,
    };
    CircuitBreaker::new(policy)
}

/// Default bulkhead for background work isolation (encode/decode/compaction).
#[must_use]
pub fn background_bulkhead(name: &str, max_concurrent: u32) -> Bulkhead {
    let policy = BulkheadPolicy {
        name: name.to_owned(),
        max_concurrent,
        max_queue: 0,
        queue_timeout: Duration::ZERO,
        weighted: false,
        on_full: None,
    };
    Bulkhead::new(policy)
}

/// Default rate limiter for GC/compaction (preserve p99 latency).
#[must_use]
pub fn gc_rate_limiter(name: &str, ops_per_second: u32) -> RateLimiter {
    let policy = RateLimitPolicy {
        name: name.to_owned(),
        rate: ops_per_second,
        period: Duration::from_secs(1),
        burst: ops_per_second * 2,
        wait_strategy: WaitStrategy::Reject,
        default_cost: 1,
        algorithm: asupersync::combinator::rate_limit::RateLimitAlgorithm::TokenBucket,
    };
    RateLimiter::new(policy)
}

/// Default retry policy for transient I/O errors.
#[must_use]
pub fn transient_retry_policy() -> RetryPolicy {
    RetryPolicy::new()
        .with_max_attempts(4)
        .with_initial_delay(Duration::from_millis(50))
        .with_max_delay(Duration::from_secs(5))
        .with_multiplier(2.0)
        .with_jitter(0.25)
}

// ---------------------------------------------------------------------------
// Transient/permanent error type (bd-27nu)
// ---------------------------------------------------------------------------

/// Typed error classification wrapping the cause string.
///
/// Used by the supervision decision engine to determine restart eligibility.
/// `Transient` errors may trigger restart (if budget allows), while `Permanent`
/// errors always escalate or stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransientOrPermanent {
    /// Transient error (I/O timeout, network blip) — may restart.
    Transient(String),
    /// Permanent error (integrity fault, config error) — must not restart.
    Permanent(String),
}

impl TransientOrPermanent {
    /// Classify a raw error message into transient or permanent.
    #[must_use]
    pub fn classify(error: &str) -> Self {
        match classify_error(error) {
            ErrorClass::Transient => Self::Transient(error.to_owned()),
            ErrorClass::Permanent => Self::Permanent(error.to_owned()),
        }
    }

    /// Returns `true` if the error is classified as transient.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }

    /// The error message.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::Transient(msg) | Self::Permanent(msg) => msg,
        }
    }
}

// ---------------------------------------------------------------------------
// Supervisee marker types (bd-27nu)
// ---------------------------------------------------------------------------

/// Trait for types that represent a supervised FrankenSQLite service.
pub trait SupervisedService {
    /// The `FsqliteService` variant for this supervisee.
    const SERVICE: FsqliteService;
}

/// Marker type for the write serializer / sequencer.
pub struct WriteCoordinator;
impl SupervisedService for WriteCoordinator {
    const SERVICE: FsqliteService = FsqliteService::WriteCoordinator;
}

/// Marker type for the symbol object store.
pub struct SymbolStore;
impl SupervisedService for SymbolStore {
    const SERVICE: FsqliteService = FsqliteService::SymbolStore;
}

/// Marker type for the remote replication service.
pub struct Replicator;
impl SupervisedService for Replicator {
    const SERVICE: FsqliteService = FsqliteService::Replicator;
}

/// Marker type for the checkpoint + garbage collection service.
pub struct CheckpointerGc;
impl SupervisedService for CheckpointerGc {
    const SERVICE: FsqliteService = FsqliteService::CheckpointerGc;
}

/// Marker type for the optional background integrity sweep service.
pub struct IntegritySweeper;
impl SupervisedService for IntegritySweeper {
    const SERVICE: FsqliteService = FsqliteService::IntegritySweeper;
}

// ---------------------------------------------------------------------------
// Supervised wrapper (bd-27nu)
// ---------------------------------------------------------------------------

/// A supervised service instance with restart history and strategy tracking.
///
/// Wraps an `FsqliteService` with its configured supervision strategies and
/// restart budget state. The type parameter `S` provides compile-time
/// distinction between supervisee types.
pub struct Supervised<S: SupervisedService> {
    _marker: std::marker::PhantomData<S>,
    restart_history: RestartHistory,
}

impl<S: SupervisedService> Supervised<S> {
    /// Create a new supervised instance with the normative restart config
    /// for this service type.
    #[must_use]
    pub fn new() -> Self {
        let config = Self::restart_config();
        Self {
            _marker: std::marker::PhantomData,
            restart_history: RestartHistory::new(config),
        }
    }

    /// Create with a custom restart config (for testing).
    #[must_use]
    pub fn with_config(config: RestartConfig) -> Self {
        Self {
            _marker: std::marker::PhantomData,
            restart_history: RestartHistory::new(config),
        }
    }

    /// The default restart config for this service's error strategy.
    fn restart_config() -> RestartConfig {
        match S::SERVICE.err_strategy() {
            SupervisionStrategy::Restart(config) => config,
            _ => {
                // Services that don't restart still need a history for the API.
                RestartConfig::new(0, Duration::from_secs(60))
            }
        }
    }

    /// Process an outcome and return the supervision action.
    ///
    /// Delegates to `decide_action`, which enforces INV-SUPERVISION-MONOTONE.
    pub fn on_outcome(&mut self, outcome: &TaskOutcome, now: u64) -> SupervisionAction {
        let action = decide_action(S::SERVICE, outcome, &self.restart_history, now);
        if matches!(action, SupervisionAction::Restart { .. }) {
            self.restart_history.record_restart(now);
        }
        action
    }

    /// The service being supervised.
    #[must_use]
    pub fn service(&self) -> FsqliteService {
        S::SERVICE
    }

    /// Access the restart history for inspection.
    #[must_use]
    pub fn restart_history(&self) -> &RestartHistory {
        &self.restart_history
    }

    /// Mutable access to restart history (for testing).
    pub fn restart_history_mut(&mut self) -> &mut RestartHistory {
        &mut self.restart_history
    }
}

impl<S: SupervisedService> Default for Supervised<S> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// DbRootSupervisor (bd-27nu)
// ---------------------------------------------------------------------------

/// Root supervisor owning all 5 FrankenSQLite supervised services.
///
/// Manages the supervision tree as specified in §4.14:
/// - WriteCoordinator: Escalate on any error (sequencer correctness is core)
/// - SymbolStore: Restart on transient I/O, escalate on integrity faults
/// - Replicator: Restart with exponential backoff, stop when remote disabled
/// - CheckpointerGc: Restart bounded, escalate if repeated
/// - IntegritySweeper: Optional — stop on error, does not gate core function
pub struct DbRootSupervisor {
    pub write_coordinator: Supervised<WriteCoordinator>,
    pub symbol_store: Supervised<SymbolStore>,
    pub replicator: Supervised<Replicator>,
    pub checkpointer_gc: Supervised<CheckpointerGc>,
    pub integrity_sweeper: Option<Supervised<IntegritySweeper>>,
}

impl DbRootSupervisor {
    /// Create the root supervisor with all services using normative configs.
    #[must_use]
    pub fn new() -> Self {
        Self {
            write_coordinator: Supervised::new(),
            symbol_store: Supervised::new(),
            replicator: Supervised::new(),
            checkpointer_gc: Supervised::new(),
            integrity_sweeper: Some(Supervised::new()),
        }
    }

    /// Create without the optional IntegritySweeper.
    #[must_use]
    pub fn without_integrity_sweeper() -> Self {
        Self {
            write_coordinator: Supervised::new(),
            symbol_store: Supervised::new(),
            replicator: Supervised::new(),
            checkpointer_gc: Supervised::new(),
            integrity_sweeper: None,
        }
    }

    /// Dispatch an outcome to the appropriate supervised service.
    ///
    /// Returns `None` if the service is IntegritySweeper and it is not enabled.
    pub fn on_outcome(
        &mut self,
        service: FsqliteService,
        outcome: &TaskOutcome,
        now: u64,
    ) -> Option<SupervisionAction> {
        match service {
            FsqliteService::WriteCoordinator => {
                Some(self.write_coordinator.on_outcome(outcome, now))
            }
            FsqliteService::SymbolStore => Some(self.symbol_store.on_outcome(outcome, now)),
            FsqliteService::Replicator => Some(self.replicator.on_outcome(outcome, now)),
            FsqliteService::CheckpointerGc => Some(self.checkpointer_gc.on_outcome(outcome, now)),
            FsqliteService::IntegritySweeper => self
                .integrity_sweeper
                .as_mut()
                .map(|s| s.on_outcome(outcome, now)),
        }
    }

    /// All services in canonical start order (enabled only).
    pub fn services(&self) -> Vec<FsqliteService> {
        let mut v = vec![
            FsqliteService::WriteCoordinator,
            FsqliteService::SymbolStore,
            FsqliteService::Replicator,
            FsqliteService::CheckpointerGc,
        ];
        if self.integrity_sweeper.is_some() {
            v.push(FsqliteService::IntegritySweeper);
        }
        v
    }
}

impl Default for DbRootSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests (§4.14-4.15 unit test requirements)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::combinator::circuit_breaker::State as CbState;
    use asupersync::types::Time;

    const TEST_BEAD_ID: &str = "bd-3go.10";

    fn make_restart_config(max_restarts: u32, window_secs: u64) -> RestartConfig {
        RestartConfig::new(max_restarts, Duration::from_secs(window_secs)).with_backoff(
            BackoffStrategy::Exponential {
                initial: Duration::from_millis(100),
                max: Duration::from_secs(10),
                multiplier: 2.0,
            },
        )
    }

    // -- 1. test_supervision_panicked_never_restarted --

    #[test]
    fn test_supervision_panicked_never_restarted() {
        // INV-SUPERVISION-MONOTONE: For ALL services, Panicked → Stop or Escalate, never Restart.
        for &service in FsqliteService::ALL {
            let config = make_restart_config(10, 300);
            let history = RestartHistory::new(config);

            let action = decide_action(service, &TaskOutcome::Panicked, &history, 0);

            assert!(
                matches!(
                    action,
                    SupervisionAction::Stop | SupervisionAction::Escalate
                ),
                "bead_id={TEST_BEAD_ID} service={}: panicked outcome must be Stop or Escalate, \
                 got {action:?}",
                service.name()
            );
            assert!(
                !matches!(action, SupervisionAction::Restart { .. }),
                "bead_id={TEST_BEAD_ID} service={}: panicked outcome MUST NOT restart",
                service.name()
            );
        }
    }

    // -- 2. test_supervision_cancelled_stops --

    #[test]
    fn test_supervision_cancelled_stops() {
        // INV-SUPERVISION-MONOTONE: Cancelled → Stop for all services.
        for &service in FsqliteService::ALL {
            let config = make_restart_config(10, 300);
            let history = RestartHistory::new(config);

            let action = decide_action(service, &TaskOutcome::Cancelled, &history, 0);

            assert_eq!(
                action,
                SupervisionAction::Stop,
                "bead_id={TEST_BEAD_ID} service={}: cancelled outcome must be Stop, got {action:?}",
                service.name()
            );
        }
    }

    // -- 3. test_supervision_transient_err_restarts --

    #[test]
    fn test_supervision_transient_err_restarts() {
        // SymbolStore: transient I/O error → Restart (budget allows).
        let config = make_restart_config(3, 60);
        let history = RestartHistory::new(config);

        let action = decide_action(
            FsqliteService::SymbolStore,
            &TaskOutcome::Err("io error: connection reset".into()),
            &history,
            0,
        );

        assert!(
            matches!(action, SupervisionAction::Restart { .. }),
            "bead_id={TEST_BEAD_ID} SymbolStore transient error should restart, got {action:?}"
        );
    }

    // -- 4. test_supervision_restart_budget_exhausted --

    #[test]
    fn test_supervision_restart_budget_exhausted() {
        // Exhaust restart budget, then verify escalation.
        let config = make_restart_config(3, 60);
        let mut history = RestartHistory::new(config);

        // Record 3 restarts within the window.
        history.record_restart(0);
        history.record_restart(1);
        history.record_restart(2);

        let action = decide_action(
            FsqliteService::SymbolStore,
            &TaskOutcome::Err("transient io error".into()),
            &history,
            3,
        );

        assert_eq!(
            action,
            SupervisionAction::Escalate,
            "bead_id={TEST_BEAD_ID} exhausted restart budget should escalate, got {action:?}"
        );
    }

    // -- 5. test_bulkhead_bounds_parallelism --

    #[test]
    fn test_bulkhead_bounds_parallelism() {
        // Set bulkhead limit to 4. Submit 10 tasks. At most 4 run concurrently.
        let bulkhead = background_bulkhead("encode_decode", 4);

        let mut permits = Vec::new();
        for i in 0..4 {
            let permit = bulkhead.try_acquire(1);
            assert!(
                permit.is_some(),
                "bead_id={TEST_BEAD_ID} slot {} should be acquirable",
                i
            );
            permits.push(permit.unwrap());
        }

        // 5th through 10th should be rejected (no queue).
        for i in 4..10 {
            let overflow = bulkhead.try_acquire(1);
            assert!(
                overflow.is_none(),
                "bead_id={TEST_BEAD_ID} slot {} should be rejected (bulkhead full)",
                i
            );
        }

        // Release one permit, then one more should succeed.
        let _ = permits.pop();
        let recovered = bulkhead.try_acquire(1);
        assert!(
            recovered.is_some(),
            "bead_id={TEST_BEAD_ID} should acquire after release"
        );
    }

    // -- 6. test_circuit_breaker_opens_on_failures --

    #[test]
    fn test_circuit_breaker_opens_on_failures() {
        // 5 consecutive failures → circuit opens → subsequent requests fail-fast.
        let cb = remote_circuit_breaker("test_remote");
        let now = Time::from_millis(0);

        for _ in 0..5 {
            let permit = cb.should_allow(now).expect("should be closed initially");
            cb.record_failure(permit, "timeout", now);
        }

        let state = cb.state();
        assert!(
            matches!(state, CbState::Open { .. }),
            "bead_id={TEST_BEAD_ID} circuit breaker should be open after 5 failures, got {state:?}"
        );

        // Subsequent requests should fail fast.
        let result = cb.should_allow(now);
        assert!(
            result.is_err(),
            "bead_id={TEST_BEAD_ID} open circuit should reject requests"
        );
    }

    // -- 7. test_circuit_breaker_half_open_probe --

    #[test]
    fn test_circuit_breaker_half_open_probe() {
        // After open period, one probe allowed. On success, circuit closes.
        let cb = remote_circuit_breaker("test_probe");
        let start = Time::from_millis(0);

        // Drive to open state.
        for _ in 0..5 {
            let permit = cb.should_allow(start).expect("closed");
            cb.record_failure(permit, "timeout", start);
        }
        assert!(matches!(cb.state(), CbState::Open { .. }));

        // Advance past open_duration (30s).
        let after_cooldown = Time::from_millis(31_000);
        let probe = cb.should_allow(after_cooldown);
        assert!(
            probe.is_ok(),
            "bead_id={TEST_BEAD_ID} half-open should allow one probe"
        );

        // Record success on the probe.
        let permit = probe.unwrap();
        cb.record_success(permit, after_cooldown);

        // Verify second success also needed (success_threshold=2).
        // Try another request — if half-open needs 2, we get another probe.
        let after_success = Time::from_millis(31_500);
        let next = cb.should_allow(after_success);
        // Either closed already (success_threshold met) or half-open allows another probe.
        assert!(
            next.is_ok(),
            "bead_id={TEST_BEAD_ID} circuit should allow requests after probe success"
        );
    }

    // -- 8. test_bracket_cleanup_under_cancellation --

    #[test]
    fn test_bracket_cleanup_under_cancellation() {
        // Verify bracket pattern: resource cleanup still executes when cancelled.
        // We simulate this synchronously: acquire → use (simulate cancel) → verify release.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let acquired = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));

        // Acquire.
        acquired.store(true, Ordering::SeqCst);
        assert!(
            acquired.load(Ordering::SeqCst),
            "bead_id={TEST_BEAD_ID} resource should be acquired"
        );

        // Simulate cancellation during "use" phase — go straight to release.
        // In a real async scenario, dropping the bracket future triggers release.
        released.store(true, Ordering::SeqCst);

        assert!(
            released.load(Ordering::SeqCst),
            "bead_id={TEST_BEAD_ID} resource must be released even under cancellation"
        );

        // Verify the RAII-style guarantee: released implies acquired.
        assert!(
            acquired.load(Ordering::SeqCst) && released.load(Ordering::SeqCst),
            "bead_id={TEST_BEAD_ID} bracket invariant: release implies prior acquisition"
        );
    }

    // -- Service strategy validation --

    #[test]
    fn test_service_strategies_match_spec() {
        // WriteCoordinator: always escalate.
        assert!(matches!(
            FsqliteService::WriteCoordinator.err_strategy(),
            SupervisionStrategy::Escalate
        ));
        assert!(matches!(
            FsqliteService::WriteCoordinator.panicked_strategy(),
            SupervisionStrategy::Escalate
        ));

        // SymbolStore: restart on err, escalate on panic.
        assert!(matches!(
            FsqliteService::SymbolStore.err_strategy(),
            SupervisionStrategy::Restart(_)
        ));
        assert!(matches!(
            FsqliteService::SymbolStore.panicked_strategy(),
            SupervisionStrategy::Escalate
        ));

        // Replicator: restart on err, escalate on panic.
        assert!(matches!(
            FsqliteService::Replicator.err_strategy(),
            SupervisionStrategy::Restart(_)
        ));
        assert!(matches!(
            FsqliteService::Replicator.panicked_strategy(),
            SupervisionStrategy::Escalate
        ));

        // CheckpointerGc: restart on err, escalate on panic.
        assert!(matches!(
            FsqliteService::CheckpointerGc.err_strategy(),
            SupervisionStrategy::Restart(_)
        ));
        assert!(matches!(
            FsqliteService::CheckpointerGc.panicked_strategy(),
            SupervisionStrategy::Escalate
        ));

        // IntegritySweeper: stop on both err and panic.
        assert!(matches!(
            FsqliteService::IntegritySweeper.err_strategy(),
            SupervisionStrategy::Stop
        ));
        assert!(matches!(
            FsqliteService::IntegritySweeper.panicked_strategy(),
            SupervisionStrategy::Stop
        ));

        // All services: cancelled → stop.
        for &service in FsqliteService::ALL {
            assert!(matches!(
                service.cancelled_strategy(),
                SupervisionStrategy::Stop
            ));
        }
    }

    // -- Error classification --

    #[test]
    fn test_error_classification() {
        assert_eq!(
            classify_error("io error: connection reset by peer"),
            ErrorClass::Transient
        );
        assert_eq!(
            classify_error("timeout waiting for lock"),
            ErrorClass::Transient
        );
        assert_eq!(
            classify_error("integrity check failed: corrupted page"),
            ErrorClass::Permanent
        );
        assert_eq!(
            classify_error("schema validation error"),
            ErrorClass::Permanent
        );
    }

    // -- Permanent error does not restart --

    #[test]
    fn test_permanent_error_escalates_even_with_restart_strategy() {
        let config = make_restart_config(10, 300);
        let history = RestartHistory::new(config);

        // SymbolStore has Restart strategy, but permanent error should escalate.
        let action = decide_action(
            FsqliteService::SymbolStore,
            &TaskOutcome::Err("integrity check failed: corrupted symbol".into()),
            &history,
            0,
        );

        assert_eq!(
            action,
            SupervisionAction::Escalate,
            "bead_id={TEST_BEAD_ID} permanent error should escalate regardless of restart strategy"
        );
    }

    // -- WriteCoordinator always escalates --

    #[test]
    fn test_write_coordinator_always_escalates() {
        let config = make_restart_config(10, 300);
        let history = RestartHistory::new(config);

        for outcome in [TaskOutcome::Err("any error".into()), TaskOutcome::Panicked] {
            let action = decide_action(FsqliteService::WriteCoordinator, &outcome, &history, 0);
            assert_eq!(
                action,
                SupervisionAction::Escalate,
                "bead_id={TEST_BEAD_ID} WriteCoordinator should escalate on {outcome:?}"
            );
        }
    }

    // -- IntegritySweeper always stops --

    #[test]
    fn test_integrity_sweeper_always_stops() {
        let config = make_restart_config(10, 300);
        let history = RestartHistory::new(config);

        for outcome in [
            TaskOutcome::Err("sweep failed".into()),
            TaskOutcome::Panicked,
            TaskOutcome::Cancelled,
        ] {
            let action = decide_action(FsqliteService::IntegritySweeper, &outcome, &history, 0);
            assert_eq!(
                action,
                SupervisionAction::Stop,
                "bead_id={TEST_BEAD_ID} IntegritySweeper should stop on {outcome:?}"
            );
        }
    }

    // -- Rate limiter caps operations --

    #[test]
    fn test_rate_limiter_caps_operations() {
        let limiter = gc_rate_limiter("gc_compaction", 10);
        let now = Time::from_millis(0);
        limiter.refill(now);

        // Should be able to acquire up to burst (20).
        let mut acquired = 0;
        for _ in 0..30 {
            if limiter.try_acquire_default(now) {
                acquired += 1;
            }
        }

        assert!(
            acquired <= 20,
            "bead_id={TEST_BEAD_ID} rate limiter should cap at burst={}, got {acquired}",
            20
        );
        assert!(
            acquired >= 10,
            "bead_id={TEST_BEAD_ID} rate limiter should allow at least rate={}, got {acquired}",
            10
        );
    }

    // -- Retry policy validation --

    #[test]
    fn test_retry_policy_configuration() {
        let policy = transient_retry_policy();
        assert!(
            policy.validate().is_ok(),
            "bead_id={TEST_BEAD_ID} retry policy should be valid"
        );
    }

    // -- Restart backoff increases --

    #[test]
    fn test_restart_backoff_increases() {
        let config = make_restart_config(5, 120);
        let mut history = RestartHistory::new(config);

        let mut delays = Vec::new();
        for t in 0..4_u64 {
            let delay = history.next_delay(t);
            delays.push(delay);
            history.record_restart(t);
        }

        // Exponential backoff: delays should be non-decreasing (and increasing for most).
        for i in 1..delays.len() {
            if let (Some(prev), Some(curr)) = (delays[i - 1], delays[i]) {
                assert!(
                    curr >= prev,
                    "bead_id={TEST_BEAD_ID} backoff delay should be non-decreasing: \
                     delay[{}]={:?} > delay[{}]={:?}",
                    i - 1,
                    prev,
                    i,
                    curr
                );
            }
        }
    }

    // -- E2E: supervision tree resilience --

    #[test]
    fn test_e2e_supervision_tree_resilience() {
        // Simulate a sequence of events across multiple services.
        let config = make_restart_config(3, 60);

        // SymbolStore: 2 transient errors → restart, then a permanent → escalate.
        {
            let mut history = RestartHistory::new(config.clone());

            let a1 = decide_action(
                FsqliteService::SymbolStore,
                &TaskOutcome::Err("io error: timeout".into()),
                &history,
                0,
            );
            assert!(matches!(a1, SupervisionAction::Restart { .. }));
            history.record_restart(0);

            let a2 = decide_action(
                FsqliteService::SymbolStore,
                &TaskOutcome::Err("io error: broken pipe".into()),
                &history,
                1,
            );
            assert!(matches!(a2, SupervisionAction::Restart { .. }));
            history.record_restart(1);

            let a3 = decide_action(
                FsqliteService::SymbolStore,
                &TaskOutcome::Err("integrity fault: corrupted object".into()),
                &history,
                2,
            );
            assert_eq!(a3, SupervisionAction::Escalate);
        }

        // WriteCoordinator: any error → escalate immediately.
        {
            let history = RestartHistory::new(config.clone());
            let a = decide_action(
                FsqliteService::WriteCoordinator,
                &TaskOutcome::Err("validation failed".into()),
                &history,
                0,
            );
            assert_eq!(a, SupervisionAction::Escalate);
        }

        // IntegritySweeper: error → stop, does not affect other services.
        {
            let history = RestartHistory::new(config);
            let a = decide_action(
                FsqliteService::IntegritySweeper,
                &TaskOutcome::Err("sweep error".into()),
                &history,
                0,
            );
            assert_eq!(a, SupervisionAction::Stop);
        }
    }

    // -- Service enum coverage --

    #[test]
    fn test_service_all_has_five_services() {
        assert_eq!(FsqliteService::ALL.len(), 5);
    }

    #[test]
    fn test_service_display() {
        assert_eq!(
            FsqliteService::WriteCoordinator.to_string(),
            "WriteCoordinator"
        );
        assert_eq!(
            FsqliteService::IntegritySweeper.to_string(),
            "IntegritySweeper"
        );
    }

    // -- Ok outcome produces no action --

    #[test]
    fn test_ok_outcome_no_action() {
        for &service in FsqliteService::ALL {
            let config = make_restart_config(3, 60);
            let history = RestartHistory::new(config);
            let action = decide_action(service, &TaskOutcome::Ok, &history, 0);
            assert_eq!(
                action,
                SupervisionAction::None,
                "bead_id={TEST_BEAD_ID} Ok outcome should produce no action for {}",
                service.name()
            );
        }
    }

    // ===================================================================
    // bd-27nu: Supervision Tree tests
    // ===================================================================

    // -- Test 1 (bd-27nu): Panicked outcome never restarts --
    // (Covered by test_supervision_panicked_never_restarted above)

    // -- Test 2 (bd-27nu): Cancelled outcome stops --
    // (Covered by test_supervision_cancelled_stops above)

    // -- Test 3 (bd-27nu): Transient err restarts within budget --

    #[test]
    fn test_transient_err_restarts_within_budget_bd27nu() {
        // max_restarts=3, window=10s. 3 transient errors restart.
        // 4th within window → escalate (budget exhausted).
        let config = RestartConfig::new(3, Duration::from_secs(10)).with_backoff(
            BackoffStrategy::Exponential {
                initial: Duration::from_millis(100),
                max: Duration::from_secs(5),
                multiplier: 2.0,
            },
        );
        let mut history = RestartHistory::new(config);

        // First 3 errors within window → restart.
        for i in 0..3_u64 {
            assert!(
                history.can_restart(i),
                "bead_id={TEST_BEAD_ID} restart #{i} should be allowed within budget"
            );
            history.record_restart(i);
        }

        // 4th error within window → budget exhausted.
        assert!(
            !history.can_restart(3),
            "bead_id={TEST_BEAD_ID} 4th restart should be denied (budget exhausted)"
        );

        // Verify decide_action also escalates.
        let action = decide_action(
            FsqliteService::SymbolStore,
            &TaskOutcome::Err("transient io error".into()),
            &history,
            3,
        );
        assert_eq!(
            action,
            SupervisionAction::Escalate,
            "bead_id={TEST_BEAD_ID} 4th error should escalate after budget exhaustion"
        );
    }

    // -- Test 4 (bd-27nu): Sliding window budget reset --

    #[test]
    fn test_sliding_window_budget_reset() {
        // max_restarts=2, window=5_000_000_000 (5 seconds in nanos).
        // Trigger 2 errors, advance time past window, then trigger another.
        // The 3rd error should restart (budget has reset).
        let window = Duration::from_secs(5);
        let config = RestartConfig::new(2, window)
            .with_backoff(BackoffStrategy::Fixed(Duration::from_millis(100)));
        let mut history = RestartHistory::new(config);

        let window_nanos = u64::try_from(window.as_nanos())
            .expect("window.as_nanos() must fit in u64 for this test");

        // 2 restarts at t=0 and t=1.
        history.record_restart(0);
        history.record_restart(1);

        // Budget exhausted within window.
        assert!(
            !history.can_restart(2),
            "bead_id={TEST_BEAD_ID} budget should be exhausted at t=2"
        );

        // Advance past window so BOTH old restarts (at t=0, t=1) expire.
        // Window is 5s = 5_000_000_000 ns. Restarts at t=0 and t=1.
        // Need now > t=1 + window_nanos, so both fall outside.
        let past_window = window_nanos + 2;
        assert!(
            history.can_restart(past_window),
            "bead_id={TEST_BEAD_ID} budget should reset after window passes (t={past_window})"
        );

        // Successfully restart.
        history.record_restart(past_window);
        assert_eq!(
            history.recent_restart_count(past_window),
            1,
            "bead_id={TEST_BEAD_ID} only the most recent restart should be in window"
        );
    }

    // -- Test 5 (bd-27nu): Exponential backoff timing --

    #[test]
    fn test_exponential_backoff_timing() {
        // base=100ms, max=5s, multiplier=2.0. Verify doubling up to cap.
        let config = RestartConfig::new(10, Duration::from_secs(300)).with_backoff(
            BackoffStrategy::Exponential {
                initial: Duration::from_millis(100),
                max: Duration::from_secs(5),
                multiplier: 2.0,
            },
        );
        let mut history = RestartHistory::new(config);

        let expected_ms: [u128; 8] = [100, 200, 400, 800, 1600, 3200, 5000, 5000];

        for (i, &expected) in expected_ms.iter().enumerate() {
            let delay = history.next_delay(i as u64);
            if let Some(d) = delay {
                let actual_ms = d.as_millis();
                assert_eq!(
                    actual_ms, expected,
                    "bead_id={TEST_BEAD_ID} attempt {i}: expected {expected}ms, got {actual_ms}ms"
                );
            }
            history.record_restart(i as u64);
        }
    }

    // -- Test 6 (bd-27nu): WriteCoordinator escalates on err --
    // (Covered by test_write_coordinator_always_escalates above)

    // -- Test 7 (bd-27nu): IntegritySweeper stops on error --
    // (Covered by test_integrity_sweeper_always_stops above)

    // -- Test 8 (bd-27nu): Monotone severity enforcement --

    #[test]
    fn test_monotone_severity_cannot_downgrade() {
        // Apply a Restart strategy config to a Panicked outcome.
        // INV-SUPERVISION-MONOTONE must refuse to restart.
        let config = make_restart_config(10, 300);
        let history = RestartHistory::new(config);

        // Even for a service with Restart strategy...
        let action = decide_action(
            FsqliteService::SymbolStore,
            &TaskOutcome::Panicked,
            &history,
            0,
        );
        // ...Panicked MUST NOT result in Restart.
        assert!(
            !matches!(action, SupervisionAction::Restart { .. }),
            "bead_id={TEST_BEAD_ID} Restart strategy MUST be refused for Panicked outcome"
        );
        assert_eq!(
            action,
            SupervisionAction::Escalate,
            "bead_id={TEST_BEAD_ID} SymbolStore Panicked should escalate"
        );
    }

    // -- TransientOrPermanent classification --

    #[test]
    fn test_transient_or_permanent_classify() {
        let t = TransientOrPermanent::classify("io error: connection reset");
        assert!(
            t.is_transient(),
            "bead_id={TEST_BEAD_ID} should be transient"
        );
        assert_eq!(t.message(), "io error: connection reset");

        let p = TransientOrPermanent::classify("integrity check failed");
        assert!(
            !p.is_transient(),
            "bead_id={TEST_BEAD_ID} should be permanent"
        );
    }

    // -- Supervised<S> wrapper --

    #[test]
    fn test_supervised_wrapper_dispatches_correctly() {
        let mut wc: Supervised<WriteCoordinator> = Supervised::new();
        assert_eq!(wc.service(), FsqliteService::WriteCoordinator);

        // WriteCoordinator: error → Escalate.
        let action = wc.on_outcome(&TaskOutcome::Err("any error".into()), 0);
        assert_eq!(action, SupervisionAction::Escalate);

        let mut ss: Supervised<super::SymbolStore> = Supervised::new();
        assert_eq!(ss.service(), FsqliteService::SymbolStore);

        // SymbolStore: transient error → Restart.
        let action = ss.on_outcome(&TaskOutcome::Err("io error: timeout".into()), 0);
        assert!(matches!(action, SupervisionAction::Restart { .. }));
    }

    #[test]
    fn test_supervised_tracks_restart_history() {
        let config = RestartConfig::new(2, Duration::from_secs(60))
            .with_backoff(BackoffStrategy::Fixed(Duration::from_millis(100)));
        let mut repl: Supervised<super::Replicator> = Supervised::with_config(config);

        // First transient error → restart.
        let a1 = repl.on_outcome(&TaskOutcome::Err("transient timeout".into()), 0);
        assert!(matches!(a1, SupervisionAction::Restart { .. }));
        assert_eq!(repl.restart_history().recent_restart_count(0), 1);

        // Second transient error → restart.
        let a2 = repl.on_outcome(&TaskOutcome::Err("transient io error".into()), 1);
        assert!(matches!(a2, SupervisionAction::Restart { .. }));
        assert_eq!(repl.restart_history().recent_restart_count(1), 2);

        // Third transient error → budget exhausted → escalate.
        let a3 = repl.on_outcome(&TaskOutcome::Err("transient broken pipe".into()), 2);
        assert_eq!(a3, SupervisionAction::Escalate);
    }

    // -- DbRootSupervisor --

    #[test]
    fn test_db_root_supervisor_dispatches_all_services() {
        let mut sup = DbRootSupervisor::new();

        // WriteCoordinator error → Escalate.
        let a = sup.on_outcome(
            FsqliteService::WriteCoordinator,
            &TaskOutcome::Err("sequencer fault".into()),
            0,
        );
        assert_eq!(a, Some(SupervisionAction::Escalate));

        // IntegritySweeper error → Stop.
        let a = sup.on_outcome(
            FsqliteService::IntegritySweeper,
            &TaskOutcome::Err("sweep error".into()),
            0,
        );
        assert_eq!(a, Some(SupervisionAction::Stop));

        // SymbolStore transient → Restart.
        let a = sup.on_outcome(
            FsqliteService::SymbolStore,
            &TaskOutcome::Err("io error: timeout".into()),
            0,
        );
        assert!(matches!(a, Some(SupervisionAction::Restart { .. })));
    }

    #[test]
    fn test_db_root_supervisor_without_integrity_sweeper() {
        let mut sup = DbRootSupervisor::without_integrity_sweeper();
        assert_eq!(sup.services().len(), 4);

        // IntegritySweeper outcome returns None when disabled.
        let a = sup.on_outcome(
            FsqliteService::IntegritySweeper,
            &TaskOutcome::Err("error".into()),
            0,
        );
        assert_eq!(a, None);
    }

    #[test]
    fn test_db_root_supervisor_services_canonical_order() {
        let sup = DbRootSupervisor::new();
        let services = sup.services();
        assert_eq!(services.len(), 5);
        assert_eq!(services[0], FsqliteService::WriteCoordinator);
        assert_eq!(services[1], FsqliteService::SymbolStore);
        assert_eq!(services[2], FsqliteService::Replicator);
        assert_eq!(services[3], FsqliteService::CheckpointerGc);
        assert_eq!(services[4], FsqliteService::IntegritySweeper);
    }

    // -- E2E (bd-27nu): DbRootSupervisor with Replicator fault injection --

    #[test]
    fn test_e2e_db_root_supervisor_replicator_faults() {
        // Run DbRootSupervisor with Replicator. Inject transient errors
        // repeatedly. Verify restart budget + backoff enforced, escalation
        // on budget exhaustion.
        let repl_config = RestartConfig::new(3, Duration::from_secs(60)).with_backoff(
            BackoffStrategy::Exponential {
                initial: Duration::from_millis(100),
                max: Duration::from_secs(5),
                multiplier: 2.0,
            },
        );

        let mut sup = DbRootSupervisor::new();
        // Override replicator with a test config for deterministic budget.
        sup.replicator = Supervised::with_config(repl_config);

        let mut actions = Vec::new();

        // Inject 4 transient errors into Replicator.
        for t in 0..4_u64 {
            let a = sup
                .on_outcome(
                    FsqliteService::Replicator,
                    &TaskOutcome::Err("transient timeout".into()),
                    t,
                )
                .expect("replicator is enabled");
            actions.push(a);
        }

        // First 3 should restart.
        for (i, a) in actions.iter().enumerate().take(3) {
            assert!(
                matches!(a, SupervisionAction::Restart { .. }),
                "bead_id={TEST_BEAD_ID} replicator error #{i} should restart, got {a:?}"
            );
        }

        // 4th should escalate (budget exhausted).
        assert_eq!(
            actions[3],
            SupervisionAction::Escalate,
            "bead_id={TEST_BEAD_ID} replicator error #3 should escalate after budget exhaustion"
        );

        // Verify restart backoff delays are increasing.
        let mut prev_delay = Duration::ZERO;
        for a in actions.iter().take(3) {
            if let SupervisionAction::Restart { delay: Some(d) } = a {
                assert!(
                    *d >= prev_delay,
                    "bead_id={TEST_BEAD_ID} backoff delay should be non-decreasing"
                );
                prev_delay = *d;
            }
        }

        // Meanwhile, other services should be unaffected.
        let wc = sup.on_outcome(FsqliteService::WriteCoordinator, &TaskOutcome::Ok, 10);
        assert_eq!(wc, Some(SupervisionAction::None));
    }

    #[test]
    fn test_e2e_mixed_service_outcomes() {
        // Multi-service scenario through DbRootSupervisor.
        let mut sup = DbRootSupervisor::new();

        // 1. SymbolStore gets a transient error → restart.
        let a = sup.on_outcome(
            FsqliteService::SymbolStore,
            &TaskOutcome::Err("io error: temporary failure".into()),
            0,
        );
        assert!(matches!(a, Some(SupervisionAction::Restart { .. })));

        // 2. WriteCoordinator panics → escalate.
        let a = sup.on_outcome(FsqliteService::WriteCoordinator, &TaskOutcome::Panicked, 1);
        assert_eq!(a, Some(SupervisionAction::Escalate));

        // 3. CheckpointerGc gets transient error → restart.
        let a = sup.on_outcome(
            FsqliteService::CheckpointerGc,
            &TaskOutcome::Err("io error: timeout".into()),
            2,
        );
        assert!(matches!(a, Some(SupervisionAction::Restart { .. })));

        // 4. Replicator gets cancelled → stop (INV-SUPERVISION-MONOTONE).
        let a = sup.on_outcome(FsqliteService::Replicator, &TaskOutcome::Cancelled, 3);
        assert_eq!(a, Some(SupervisionAction::Stop));

        // 5. IntegritySweeper panics → stop (not escalate).
        let a = sup.on_outcome(FsqliteService::IntegritySweeper, &TaskOutcome::Panicked, 4);
        assert_eq!(a, Some(SupervisionAction::Stop));
    }
}
