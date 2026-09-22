//! Opt-in, whole-transaction recovery for concurrent-writer conflicts.
//!
//! No runtime, worker thread, or writer lock is created here. Each attempt
//! uses a new engine transaction and must retire the previous attempt first.

use std::cell::Cell;
use std::fmt;
use std::future::{Future, poll_fn};
use std::ops::AsyncFnMut;
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use asupersync::channel::oneshot;
use asupersync::{Cx as NativeCx, types::Time};
use fsqlite_error::FrankenError;

use super::Transaction;
use crate::Connection;

/// Bounds for an explicitly replayable transaction body.
///
/// `timeout` is one cooperative runtime-time budget for admission, the body,
/// commit, cleanup retries, and backoff together (wall time in production,
/// virtual time in a lab runtime). It is checked between engine
/// calls; it does not preempt a running statement or an arbitrary user future.
/// Set the connection's `busy_timeout` appropriately for the application's
/// latency target. One rollback is attempted even after expiry, because
/// abandoning writes is not a successful timeout outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Includes the first attempt; must be nonzero.
    pub max_attempts: u32,
    pub timeout: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    /// Separate bound on rollback calls per failed attempt; must be nonzero.
    pub max_rollback_attempts: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            timeout: Duration::from_secs(30),
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(100),
            max_rollback_attempts: 8,
        }
    }
}

/// Why replay stopped. The original engine error is retained separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryStopReason {
    InvalidPolicy,
    AlreadyInTransaction,
    RuntimeUnavailable,
    NonTransient,
    AttemptsExhausted,
    DeadlineExceeded,
    Cancelled,
    RollbackFailed,
    /// The callback ended its transaction itself; its outcome is not replayable.
    TransactionEnded,
}

/// A failed retry operation, including its attempt census and cleanup outcome.
///
/// A rollback failure never starts another attempt. When `transaction_open`
/// is true, the abandoned owned transaction is marked for the engine's
/// mandatory deferred cleanup before subsequent SQL. Do not ignore that error
/// and assume the connection is idle. An already-active caller transaction is
/// refused without modifying it or installing a cleanup obligation.
#[derive(Debug)]
pub struct TransactionRetryError {
    pub reason: RetryStopReason,
    /// Number of BEGIN calls, including unsuccessful admission attempts.
    pub attempts: u32,
    pub elapsed: Duration,
    pub last_error: Option<Box<FrankenError>>,
    pub rollback_error: Option<Box<FrankenError>>,
    pub transaction_open: bool,
}

impl fmt::Display for TransactionRetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "transaction retry stopped ({:?}) after {} attempt(s), {:?}",
            self.reason, self.attempts, self.elapsed)?;
        if let Some(error) = &self.last_error {
            write!(f, ": {error}")?;
        }
        if let Some(error) = &self.rollback_error {
            write!(f, "; rollback failed: {error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for TransactionRetryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.last_error.as_deref()
            .or(self.rollback_error.as_deref())
            .map(|error| error as &(dyn std::error::Error + 'static))
    }
}

/// Explicit whole-transaction retry; ordinary `transaction()` stays single-shot.
///
/// The callback can run more than once. Keep external side effects outside it,
/// reread database state on every attempt, and do not use another alias of this
/// connection until the helper completes. The helper alone owns
/// BEGIN/COMMIT/ROLLBACK. All callback SQL entry points preflight the complete
/// batch and reject outer BEGIN, COMMIT/END and full ROLLBACK before effects;
/// nested savepoints and ROLLBACK TO remain supported. A transaction end
/// observed after bypassing the wrapper is terminal, never replay permission.
///
/// Dropping the future or unwinding the callback records the same mandatory
/// deferred rollback as dropping an ordinary [`Transaction`]. No synchronous
/// executor is constructed in Drop. A successful awaited COMMIT wins over a
/// deadline or cancellation that became observable while commit was settling.
///
/// This native API uses the caller's asupersync timer and entropy capabilities
/// for wake-driven waits and full jitter; it never sleeps an executor thread.
///
/// ```ignore
/// use fsqlite::compat::{RetryPolicy, TransactionRetryExt};
///
/// let value = conn.transaction_with_retry(RetryPolicy::default(), async |tx| {
///     tx.execute("INSERT INTO events(value) VALUES ('ready')").await?;
///     tx.last_insert_rowid()
/// }).await?;
/// ```
pub trait TransactionRetryExt {
    fn transaction_with_retry<T, F>(
        &self,
        policy: RetryPolicy,
        operation: F,
    ) -> impl Future<Output = Result<T, TransactionRetryError>>
    where
        F: for<'tx, 'conn> AsyncFnMut(&'tx Transaction<'conn>) -> Result<T, FrankenError>;
}

struct RetryRun<'a> {
    conn: &'a Connection,
    native: NativeCx,
    policy: RetryPolicy,
    started: Time,
    attempts: u32,
}

struct AttemptFailure {
    reason: Option<RetryStopReason>,
    error: Option<Box<FrankenError>>,
}

impl AttemptFailure {
    fn database(error: FrankenError) -> Self {
        let reason = matches!(error, FrankenError::Interrupt).then_some(RetryStopReason::Cancelled);
        Self { reason, error: Some(Box::new(error)) }
    }

    fn stopped(reason: RetryStopReason) -> Self {
        Self { reason: Some(reason), error: None }
    }
}

impl RetryRun<'_> {
    fn elapsed(&self) -> Duration {
        Duration::from_nanos(self.native.now().as_nanos().saturating_sub(self.started.as_nanos()))
    }

    fn stop_reason(&self) -> Option<RetryStopReason> {
        if self.conn.root_cx().checkpoint().is_err() || self.native.checkpoint().is_err() {
            Some(RetryStopReason::Cancelled)
        } else if self.elapsed() >= self.policy.timeout {
            Some(RetryStopReason::DeadlineExceeded)
        } else {
            None
        }
    }

    fn failure(
        &self,
        reason: RetryStopReason,
        last_error: Option<Box<FrankenError>>,
        rollback_error: Option<Box<FrankenError>>,
    ) -> TransactionRetryError {
        TransactionRetryError {
            reason,
            attempts: self.attempts,
            elapsed: self.elapsed(),
            last_error,
            rollback_error,
            transaction_open: self.conn.in_transaction(),
        }
    }

    async fn wait(&self, retry: u32) -> Result<(), RetryStopReason> {
        if let Some(reason) = self.stop_reason() {
            return Err(reason);
        }
        let remaining = self.policy.timeout.saturating_sub(self.elapsed());
        let cap = backoff_cap(self.policy, retry).min(remaining);
        let delay = full_jitter(cap, self.native.random_u64());
        tracing::debug!(target: "fsqlite::compat", event = "transaction_retry_wait",
            attempts = self.attempts, retry, ?delay);

        // Even a zero jitter draw yields once, so a losing transaction cannot
        // monopolize a single-thread executor and starve the winning writer.
        let mut yielded = false;
        poll_fn(|cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }).await;
        if let Some(reason) = self.stop_reason() {
            return Err(reason);
        }
        let delay = delay.min(self.policy.timeout.saturating_sub(self.elapsed()));
        let mut sleep = pin!(asupersync::time::sleep(self.native.now(), delay));
        // asupersync's `Cx` has no awaitable cancellation future. A oneshot
        // receiver whose sender is never used resolves exactly when the native
        // context is cancelled (`RecvError::Cancelled`); the sender is kept
        // alive until the race is over so it cannot resolve with `Closed`.
        let (native_cancel_tx, mut native_cancel_rx) = oneshot::channel::<()>();
        let mut native_cancel = pin!(native_cancel_rx.recv(&self.native));
        let local_cx = self.conn.root_cx();
        let mut local_cancel = pin!(local_cx.wait_for_local_cancel_request());
        let waited = poll_fn(|cx| {
            if native_cancel.as_mut().poll(cx).is_ready()
                || local_cancel.as_mut().poll(cx).is_ready()
            {
                return Poll::Ready(Err(RetryStopReason::Cancelled));
            }
            sleep.as_mut().poll(cx).map(|()| Ok(()))
        }).await;
        drop(native_cancel_tx);
        waited?;
        self.stop_reason().map_or(Ok(()), Err)
    }

    async fn attempt<T, F>(
        &self,
        tx: &mut Transaction<'_>,
        operation: &mut F,
    ) -> Result<T, AttemptFailure>
    where
        F: for<'tx, 'conn> AsyncFnMut(&'tx Transaction<'conn>) -> Result<T, FrankenError>,
    {
        self.conn.begin_transaction().await.map_err(AttemptFailure::database)?;
        if let Some(reason) = self.stop_reason() {
            return Err(AttemptFailure::stopped(reason));
        }
        let result = operation(tx).await;
        // A typed transient engine abort can retire its own scope. That is
        // not the same as a callback executing COMMIT then returning Busy.
        let retryable_abort = tx.retryable_abort.get()
            && result.as_ref().is_err_and(FrankenError::is_transient);
        if (tx.finalized.get() || !self.conn.in_transaction()) && !retryable_abort {
            return Err(AttemptFailure {
                reason: Some(RetryStopReason::TransactionEnded),
                error: result.err().map(Box::new),
            });
        }
        let value = result.map_err(AttemptFailure::database)?;
        if let Some(reason) = self.stop_reason() {
            return Err(AttemptFailure::stopped(reason));
        }
        tx.commit().await.map_err(AttemptFailure::database)?;
        // Do not check the deadline or cancellation after acknowledged commit.
        Ok(value)
    }

    async fn rollback(&self, tx: &mut Transaction<'_>) -> Result<(), Option<Box<FrankenError>>> {
        if !self.conn.in_transaction() {
            // An engine-initiated abort is already an authoritative idle state.
            tx.finalized.set(true);
            return Ok(());
        }
        if tx.finalized.get() {
            // The callback ended its scope and opened something else. Never
            // accidentally roll back that replacement transaction.
            return Err(None);
        }
        confirm_rollback(
            self.policy.max_rollback_attempts,
            async || tx.rollback().await,
            || self.conn.in_transaction(),
            async |retry| self.wait(retry).await,
        ).await
    }
}

// Keep the rollback proof shared with fault-injection tests. The production
// adapter above still uses the actual engine finalizer and transaction state;
// no error is converted into success merely because a retry budget expired.
async fn confirm_rollback<R, S, W>(
    max_attempts: u32,
    mut rollback: R,
    is_active: S,
    mut wait: W,
) -> Result<(), Option<Box<FrankenError>>>
where
    R: AsyncFnMut() -> Result<(), FrankenError>,
    S: Fn() -> bool,
    W: AsyncFnMut(u32) -> Result<(), RetryStopReason>,
{
    for attempt in 1..=max_attempts {
        let result = rollback().await;
        if result.is_ok() && !is_active() {
            return Ok(());
        }
        let error = result.err().map(Box::new);
        if !is_active() {
            // A failing finalizer is not a rollback receipt. Fail closed.
            return Err(error);
        }
        let transient = error.as_deref().is_some_and(FrankenError::is_transient);
        if !transient || attempt == max_attempts || wait(attempt - 1).await.is_err() {
            return Err(error);
        }
    }
    // Public policy validation forbids zero; keep the proof helper fail-closed.
    Err(None)
}

impl TransactionRetryExt for Connection {
    async fn transaction_with_retry<T, F>(
        &self,
        policy: RetryPolicy,
        mut operation: F,
    ) -> Result<T, TransactionRetryError>
    where
        F: for<'tx, 'conn> AsyncFnMut(&'tx Transaction<'conn>) -> Result<T, FrankenError>,
    {
        let started = Instant::now();
        let rejected = |reason| TransactionRetryError {
            reason, attempts: 0, elapsed: started.elapsed(), last_error: None,
            rollback_error: None, transaction_open: self.in_transaction(),
        };
        if policy.max_attempts == 0 || policy.max_rollback_attempts == 0
            || policy.initial_backoff > policy.max_backoff
        {
            return Err(rejected(RetryStopReason::InvalidPolicy));
        }
        if self.in_transaction() {
            return Err(rejected(RetryStopReason::AlreadyInTransaction));
        }
        let native = NativeCx::current()
            .ok_or_else(|| rejected(RetryStopReason::RuntimeUnavailable))?;
        let capabilities = native.capabilities();
        if !capabilities.time || !capabilities.entropy || self.root_cx().mask_depth() != 0 {
            return Err(rejected(RetryStopReason::RuntimeUnavailable));
        }
        let started = native.now();
        let mut run = RetryRun { conn: self, native, policy, started, attempts: 0 };
        let mut last_error = None;
        loop {
            if let Some(reason) = run.stop_reason() {
                return Err(run.failure(reason, last_error, None));
            }
            run.attempts += 1;
            // Arm the cancellation guard BEFORE polling BEGIN. An abandoned
            // admission future must not bypass Transaction::drop cleanup.
            let mut tx = Transaction {
                conn: self,
                finalized: Cell::new(false),
                allow_sql_transaction_control: false,
                retryable_abort: Cell::new(false),
            };
            let failure = match run.attempt(&mut tx, &mut operation).await {
                Ok(value) => return Ok(value),
                Err(failure) => failure,
            };
            if let Err(cleanup) = run.rollback(&mut tx).await {
                return Err(run.failure(RetryStopReason::RollbackFailed, failure.error, cleanup));
            }
            drop(tx);
            last_error = failure.error;
            if let Some(reason) = failure.reason {
                return Err(run.failure(reason, last_error, None));
            }
            if !last_error.as_deref().is_some_and(FrankenError::is_transient) {
                return Err(run.failure(RetryStopReason::NonTransient, last_error, None));
            }
            if run.attempts == policy.max_attempts {
                return Err(run.failure(RetryStopReason::AttemptsExhausted, last_error, None));
            }
            if let Err(reason) = run.wait(run.attempts - 1).await {
                return Err(run.failure(reason, last_error, None));
            }
        }
    }
}

fn backoff_cap(policy: RetryPolicy, retry: u32) -> Duration {
    let factor = 1_u128.checked_shl(retry).unwrap_or(u128::MAX);
    let nanos = policy.initial_backoff.as_nanos().saturating_mul(factor)
        .min(policy.max_backoff.as_nanos());
    // Bounded by a Duration, so both components fit their destination types.
    Duration::new((nanos / 1_000_000_000) as u64, (nanos % 1_000_000_000) as u32)
}

fn full_jitter(cap: Duration, entropy: u64) -> Duration {
    let nanos = u64::try_from(cap.as_nanos()).unwrap_or(u64::MAX);
    let draw = (u128::from(entropy) * (u128::from(nanos) + 1)) >> 64;
    Duration::from_nanos(u64::try_from(draw).unwrap_or(nanos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_and_exponential_backoff_are_bounded_at_numeric_extremes() {
        let policy = RetryPolicy::default();
        assert_eq!(backoff_cap(policy, 0), policy.initial_backoff);
        assert_eq!(backoff_cap(policy, 1), policy.initial_backoff * 2);
        assert_eq!(backoff_cap(policy, u32::MAX), policy.max_backoff);
        let tiny = RetryPolicy { initial_backoff: Duration::from_nanos(1),
            max_backoff: Duration::from_secs(60), ..policy };
        assert_eq!(backoff_cap(tiny, 32), Duration::from_nanos(1_u64 << 32));
        assert_eq!(backoff_cap(tiny, 100), tiny.max_backoff);
        let zero = RetryPolicy { initial_backoff: Duration::ZERO, ..policy };
        assert_eq!(backoff_cap(zero, u32::MAX), Duration::ZERO);
        for cap in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
            for entropy in [0, 1, u64::MAX / 2, u64::MAX] {
                assert!(full_jitter(cap, entropy) <= cap);
            }
            assert_eq!(full_jitter(cap, 0), Duration::ZERO);
        }
    }

    #[test]
    fn retry_rolls_back_partial_writes_and_returns_only_committed_value() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(value INTEGER)").await.unwrap();
            let mut attempts = 0;
            let value = conn.transaction_with_retry(RetryPolicy::default(), async |tx| {
                attempts += 1;
                tx.execute("INSERT INTO t VALUES (42)").await?;
                if attempts == 1 {
                    return Err(FrankenError::Busy);
                }
                Ok(attempts)
            }).await.unwrap();
            assert_eq!(value, 2);
            assert!(!conn.in_transaction());
            assert_eq!(conn.query("SELECT * FROM t").await.unwrap().len(), 1);
        });
    }

    #[test]
    fn exhausted_retries_retain_the_error_and_leave_no_writes() {
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            conn.execute("CREATE TABLE t(value INTEGER)").await.unwrap();
            let policy = RetryPolicy { max_attempts: 2, ..RetryPolicy::default() };
            let error = conn.transaction_with_retry(policy, async |tx| {
                tx.execute("INSERT INTO t VALUES (1)").await?;
                Err::<(), _>(FrankenError::Busy)
            }).await.unwrap_err();
            assert_eq!(error.reason, RetryStopReason::AttemptsExhausted);
            assert_eq!(error.attempts, 2);
            assert!(matches!(error.last_error.as_deref(), Some(FrankenError::Busy)));
            assert!(!error.transaction_open);
            assert!(error.rollback_error.is_none());
            assert!(conn.query("SELECT * FROM t").await.unwrap().is_empty());
        });
    }

    #[test]
    fn busy_rollback_must_be_acknowledged_before_replay_is_allowed() {
        asupersync::test_utils::run_test(|| async {
            let calls = Cell::new(0);
            let active = Cell::new(true);
            let waits = Cell::new(0);
            let outcome = confirm_rollback(3, async || {
                calls.set(calls.get() + 1);
                if calls.get() < 3 {
                    Err(FrankenError::Busy)
                } else {
                    active.set(false);
                    Ok(())
                }
            }, || active.get(), async |_| {
                waits.set(waits.get() + 1);
                Ok(())
            }).await;
            assert!(outcome.is_ok());
            assert_eq!(calls.get(), 3);
            assert_eq!(waits.get(), 2);
            assert!(!active.get());
        });
    }

    #[test]
    fn rollback_exhaustion_and_deadline_preserve_the_cleanup_failure() {
        asupersync::test_utils::run_test(|| async {
            for deadline in [false, true] {
                let calls = Cell::new(0);
                let failure = confirm_rollback(3, async || {
                    calls.set(calls.get() + 1);
                    Err(FrankenError::Busy)
                }, || true, async |_| {
                    if deadline { Err(RetryStopReason::DeadlineExceeded) } else { Ok(()) }
                }).await.unwrap_err();
                assert!(matches!(failure.as_deref(), Some(FrankenError::Busy)));
                assert_eq!(calls.get(), if deadline { 1 } else { 3 });
            }
        });
    }

    #[test]
    fn rollback_requires_both_success_and_an_idle_transaction() {
        asupersync::test_utils::run_test(|| async {
            let unretired = confirm_rollback(2, async || Ok(()), || true,
                async |_| panic!("a success without retirement must not retry")).await;
            assert!(matches!(unretired, Err(None)));
            let unacknowledged = confirm_rollback(2, async || Err(FrankenError::Busy),
                || false, async |_| panic!("an unacknowledged outcome must not retry")).await;
            assert!(matches!(unacknowledged, Err(Some(error)) if matches!(*error, FrankenError::Busy)));
        });
    }

    #[test]
    fn nontransient_rollback_failure_never_waits_or_retries() {
        asupersync::test_utils::run_test(|| async {
            let calls = Cell::new(0);
            let failure = confirm_rollback(8, async || {
                calls.set(calls.get() + 1);
                Err(FrankenError::DatabaseCorrupt { detail: "injected rollback failure".into() })
            }, || true, async |_| panic!("corruption must not be retried")).await;
            assert!(failure.is_err());
            assert_eq!(calls.get(), 1);
        });
    }

    #[test]
    fn an_engine_abort_receipt_is_distinct_from_successful_sql_finalization() {
        asupersync::test_utils::run_test(|| async {
            use crate::compat::TransactionExt;

            let conn = Connection::open(":memory:").await.unwrap();
            let tx = conn.transaction().await.unwrap();
            // Inject the two parts of an engine abort receipt: the engine
            // retired its scope and the wrapper observes a typed conflict.
            conn.rollback_transaction().await.unwrap();
            let result = tx.observe_transaction_state(Err::<(), _>(FrankenError::Busy));
            assert!(result.is_err());
            assert!(tx.finalized.get());
            assert!(tx.retryable_abort.get());
            drop(tx);

            let tx = conn.transaction().await.unwrap();
            tx.execute("COMMIT").await.unwrap();
            assert!(tx.finalized.get());
            assert!(!tx.retryable_abort.get());
            assert!(tx.execute("SELECT 1").await.is_err());
            assert!(!tx.retryable_abort.get());
        });
    }

}
