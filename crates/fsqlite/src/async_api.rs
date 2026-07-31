//! Async-native wrapper around [`Connection`] for use with asupersync's `Cx` capability context.
//!
//! Because [`Connection`] is `!Send` (it uses `Rc<RefCell<..>>` internally), this module
//! provides an [`AsyncConnection`] that runs a dedicated worker thread owning the
//! `Connection`. All SQL operations are dispatched to the worker via a command channel
//! and results are returned through response channels.
//!
//! Every async method accepts a `&Cx` and validates the cancellation checkpoint,
//! current runtime, and a usable native cancellation context before dispatching.
//! Only explicit async close needs a blocking-pool capability, because ordinary
//! command admission and responses are native async channel operations. If
//! preflight fails, the operation returns without touching the worker.
//!
//! Async entry points reject a caller FrankenSQLite [`Cx`] with an active
//! cancellation mask when the call starts. That final start-time check is the
//! mask linearization point: a mask acquired later through another `Cx` alias
//! does not retroactively invalidate an admitted call or defer its cancellation.
//! Masks protect bounded in-place cleanup/commit sections; carrying one into a
//! new worker-mailbox operation would conflict with the task-affine native
//! runtime context used to wake channel reservations.
//! Asupersync's native context owns an independent mask; its public API does
//! not currently expose that depth, so native masking retains its ordinary
//! deferred-cancellation semantics.
//!
//! Once a command has been admitted, the current worker protocol runs it to
//! completion even if the caller later abandons the response. Mid-flight
//! cancellation requires a separate worker-side cancellation protocol.
//!
//! # Feature gate
//!
//! This module is only available when the `async-api` feature is enabled on `fsqlite`.
//!
//! # Example
//!
//! ```ignore
//! use fsqlite::{AsyncConnection, SqliteValue};
//! use fsqlite_types::cx::Cx;
//! use std::sync::Arc;
//!
//! async fn example(cx: &Cx) -> Result<(), Arc<fsqlite::FrankenError>> {
//!     let mut conn = AsyncConnection::open(cx, ":memory:").await?;
//!     conn.execute(cx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").await?;
//!     conn.execute_with_params(
//!         cx,
//!         "INSERT INTO t VALUES (?1, ?2)",
//!         &[SqliteValue::Integer(1), SqliteValue::Text("hello".into())],
//!     ).await?;
//!     let rows = conn.query(cx, "SELECT * FROM t").await?;
//!     assert_eq!(rows.len(), 1);
//!     conn.close(cx).await?;
//!     Ok(())
//! }
//! ```

use crate::{Connection, ConnectionEnv, FrankenError, Row, SqliteValue};
use asupersync::channel::{mpsc as async_mpsc, oneshot};
use asupersync::cx::{Cx as NativeCx, cap as native_cap};
use asupersync::runtime::Runtime;
use asupersync::runtime::blocking_pool::{BlockingPoolHandle, BlockingTaskHandle};
use fsqlite_types::cx::Cx;
use futures_lite::future;
use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
#[cfg(test)]
use std::time::Duration;

// ---------------------------------------------------------------------------
// Command protocol between async methods and the worker thread
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum Responder<T> {
    Sync(mpsc::SyncSender<Result<T, FrankenError>>),
    Async(oneshot::Sender<Result<T, FrankenError>>),
}

impl<T> Responder<T> {
    fn respond(self, result: Result<T, FrankenError>) {
        match self {
            Self::Sync(tx) => {
                let _ = tx.send(result);
            }
            Self::Async(tx) => {
                // `send_blocking` is an immediate, context-free publication;
                // it does not park this dedicated engine thread.
                let _ = tx.send_blocking(result);
            }
        }
    }
}

fn sync_response_channel<T>() -> (Responder<T>, mpsc::Receiver<Result<T, FrankenError>>) {
    let (tx, rx) = mpsc::sync_channel(1);
    (Responder::Sync(tx), rx)
}

fn async_response_channel<T>() -> (Responder<T>, oneshot::Receiver<Result<T, FrankenError>>) {
    let (tx, rx) = oneshot::channel();
    (Responder::Async(tx), rx)
}

const COMMAND_MAILBOX_CAPACITY: usize = 32;
// Raw engine futures are deeply composed enough to overflow both Rust's
// default spawned-thread stack and an 8 MiB test-thread stack under the
// fs-ledger schema-migration workload. Each connection owns exactly one engine
// worker, so reserving a larger stack here is bounded per connection and keeps
// that implementation detail off both synchronous and asynchronous callers.
const WORKER_STACK_BYTES: usize = 16 * 1024 * 1024;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerPhase {
    Idle,
    InTransaction,
    Closing,
    Terminal,
}

struct WorkerState {
    phase: AtomicU8,
    #[cfg(test)]
    cleanup_calls: AtomicUsize,
    #[cfg(test)]
    panic_on_cleanup: AtomicBool,
    #[cfg(test)]
    hold_before_open_response: AtomicBool,
    #[cfg(test)]
    open_response_waiting: AtomicBool,
    #[cfg(test)]
    open_response_committed: AtomicBool,
    #[cfg(test)]
    unobserved_errors: Mutex<Vec<String>>,
    #[cfg(test)]
    forced_open_error: Mutex<Option<FrankenError>>,
}

impl WorkerState {
    fn new() -> Self {
        Self {
            phase: AtomicU8::new(WorkerPhase::Idle as u8),
            #[cfg(test)]
            cleanup_calls: AtomicUsize::new(0),
            #[cfg(test)]
            panic_on_cleanup: AtomicBool::new(false),
            #[cfg(test)]
            hold_before_open_response: AtomicBool::new(false),
            #[cfg(test)]
            open_response_waiting: AtomicBool::new(false),
            #[cfg(test)]
            open_response_committed: AtomicBool::new(false),
            #[cfg(test)]
            unobserved_errors: Mutex::new(Vec::new()),
            #[cfg(test)]
            forced_open_error: Mutex::new(None),
        }
    }

    fn publish_connection_state(&self, conn: &Connection) {
        let phase = if conn.in_transaction() {
            WorkerPhase::InTransaction
        } else {
            WorkerPhase::Idle
        };
        self.phase.store(phase as u8, Ordering::Release);
    }

    fn publish_phase(&self, phase: WorkerPhase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    fn in_transaction(&self) -> bool {
        self.phase.load(Ordering::Acquire) == WorkerPhase::InTransaction as u8
    }

    #[cfg(test)]
    fn pause_before_open_response(&self) {
        self.open_response_waiting.store(true, Ordering::Release);
        while self.hold_before_open_response.load(Ordering::Acquire) {
            thread::yield_now();
        }
    }

    #[cfg(test)]
    fn take_forced_open_error(&self) -> Option<FrankenError> {
        self.forced_open_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    #[cfg(test)]
    fn phase(&self) -> WorkerPhase {
        match self.phase.load(Ordering::Acquire) {
            value if value == WorkerPhase::InTransaction as u8 => WorkerPhase::InTransaction,
            value if value == WorkerPhase::Closing as u8 => WorkerPhase::Closing,
            value if value == WorkerPhase::Terminal as u8 => WorkerPhase::Terminal,
            _ => WorkerPhase::Idle,
        }
    }
}

fn report_unobserved_worker_error(state: &WorkerState, message: &str) {
    let phase = state.phase.load(Ordering::Acquire);
    let _ = catch_unwind(AssertUnwindSafe(|| {
        #[cfg(test)]
        {
            let mut errors = state
                .unobserved_errors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            errors.push(message.to_owned());
        }
        tracing::error!(
            target: "fsqlite.async_worker",
            phase,
            error = message,
            "unobserved async worker terminal error"
        );
    }));
}

struct OpenErrorDiagnostic {
    message: Arc<str>,
    acknowledged: AtomicBool,
    state: Arc<WorkerState>,
}

impl OpenErrorDiagnostic {
    fn new(error: &FrankenError, state: Arc<WorkerState>) -> Self {
        Self {
            message: Arc::from(error.to_string()),
            acknowledged: AtomicBool::new(false),
            state,
        }
    }

    fn acknowledge(&self) {
        self.acknowledged.store(true, Ordering::Release);
    }

    fn is_acknowledged(&self) -> bool {
        self.acknowledged.load(Ordering::Acquire)
    }
}

impl Drop for OpenErrorDiagnostic {
    fn drop(&mut self) {
        if !self.acknowledged.load(Ordering::Acquire) {
            report_unobserved_worker_error(&self.state, &self.message);
        }
    }
}

enum OpenHandshake {
    Opened,
    Failed {
        error: FrankenError,
        diagnostic: Arc<OpenErrorDiagnostic>,
    },
}

/// A command sent from an async method to the worker thread.
#[derive(Debug)]
enum Command {
    Prepare {
        sql: String,
        tx: Responder<()>,
    },
    Query {
        sql: String,
        tx: Responder<Vec<Row>>,
    },
    QueryWithParams {
        sql: String,
        params: Vec<SqliteValue>,
        tx: Responder<Vec<Row>>,
    },
    QueryWithParamsStream {
        sql: String,
        params: Vec<SqliteValue>,
        tx: mpsc::SyncSender<Result<Option<Row>, FrankenError>>,
    },
    QueryRow {
        sql: String,
        tx: Responder<Row>,
    },
    QueryRowWithParams {
        sql: String,
        params: Vec<SqliteValue>,
        tx: Responder<Row>,
    },
    Execute {
        sql: String,
        tx: Responder<usize>,
    },
    ExecuteWithParams {
        sql: String,
        params: Vec<SqliteValue>,
        tx: Responder<usize>,
    },
    ExecuteManyWithParamsInTransaction {
        sql: String,
        parameter_sets: Vec<Vec<SqliteValue>>,
        tx: Responder<usize>,
    },
    ExecuteBatch {
        sql: String,
        tx: Responder<()>,
    },
    BeginTransaction {
        tx: Responder<()>,
    },
    CommitTransaction {
        tx: Responder<()>,
    },
    RollbackTransaction {
        tx: Responder<()>,
    },
    LastInsertRowid {
        tx: Responder<i64>,
    },
    Close,
    Shutdown,
    #[cfg(test)]
    BlockForTest {
        entered_tx: mpsc::SyncSender<()>,
        release_rx: mpsc::Receiver<()>,
    },
    #[cfg(test)]
    PanicForTest,
}

#[derive(Default)]
struct CommandMailboxSignal {
    generation: Mutex<usize>,
    capacity_available: Condvar,
    sync_waiters: AtomicUsize,
    #[cfg(test)]
    async_reservers: AtomicUsize,
    #[cfg(test)]
    hold_after_async_reservation: AtomicBool,
    #[cfg(test)]
    async_permits: AtomicUsize,
    #[cfg(test)]
    sync_retry_attempts: AtomicUsize,
    #[cfg(test)]
    blocking_receives: AtomicUsize,
    #[cfg(test)]
    async_publications: AtomicUsize,
    #[cfg(test)]
    hold_before_sync_park: AtomicBool,
    #[cfg(test)]
    sync_park_predicates: AtomicUsize,
    #[cfg(test)]
    notification_observed_gate_contention: AtomicUsize,
    #[cfg(test)]
    panic_on_receiver_drop: AtomicBool,
}

struct SyncWaiterGuard<'a>(&'a AtomicUsize);

impl<'a> SyncWaiterGuard<'a> {
    fn new(waiters: &'a AtomicUsize) -> Self {
        waiters.fetch_add(1, Ordering::AcqRel);
        Self(waiters)
    }
}

impl Drop for SyncWaiterGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct CapacityChangeGuard<'a> {
    signal: &'a CommandMailboxSignal,
    armed: bool,
}

impl<'a> CapacityChangeGuard<'a> {
    fn new(signal: &'a CommandMailboxSignal) -> Self {
        Self {
            signal,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CapacityChangeGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.signal.notify_capacity_change();
        }
    }
}

struct TerminalNotificationGuard<'a>(&'a CommandMailboxSignal);

impl Drop for TerminalNotificationGuard<'_> {
    fn drop(&mut self) {
        self.0.notify_terminal();
    }
}

impl CommandMailboxSignal {
    fn lock_generation(&self) -> MutexGuard<'_, usize> {
        self.generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn notify_capacity_change(&self) {
        // Every dequeue pays this one waiter-count load. When no synchronous
        // sender is parked, the mutex and Condvar path is skipped.
        if self.sync_waiters.load(Ordering::Acquire) == 0 {
            return;
        }
        #[cfg(test)]
        if self.hold_before_sync_park.load(Ordering::Acquire) {
            match self.generation.try_lock() {
                Ok(generation) => drop(generation),
                Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                    drop(poisoned.into_inner());
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    self.notification_observed_gate_contention
                        .fetch_add(1, Ordering::AcqRel);
                }
            }
        }
        let mut generation = self.lock_generation();
        *generation = generation.wrapping_add(1);
        self.capacity_available.notify_one();
        drop(generation);
    }

    fn notify_terminal(&self) {
        if self.sync_waiters.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut generation = self.lock_generation();
        *generation = generation.wrapping_add(1);
        self.capacity_available.notify_all();
        drop(generation);
    }

    fn wait_for_change<'a>(
        &'a self,
        generation: MutexGuard<'a, usize>,
        observed: usize,
    ) -> MutexGuard<'a, usize> {
        let result = self
            .capacity_available
            .wait_while(generation, |generation| {
                let should_wait = *generation == observed;
                #[cfg(test)]
                if should_wait && self.hold_before_sync_park.load(Ordering::Acquire) {
                    self.sync_park_predicates.fetch_add(1, Ordering::AcqRel);
                    while self.hold_before_sync_park.load(Ordering::Acquire) {
                        thread::yield_now();
                    }
                }
                should_wait
            });

        match result {
            Ok(generation) => generation,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[cfg(test)]
    fn current_generation(&self) -> usize {
        *self.lock_generation()
    }

    fn record_async_publication(&self) {
        #[cfg(test)]
        self.async_publications.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    async fn pause_after_async_reservation(&self) {
        if !self.hold_after_async_reservation.load(Ordering::Acquire) {
            return;
        }
        let _permit = SyncWaiterGuard::new(&self.async_permits);
        while self.hold_after_async_reservation.load(Ordering::Acquire) {
            future::yield_now().await;
        }
    }
}

#[derive(Clone)]
struct CommandSender {
    inner: async_mpsc::Sender<Command>,
    signal: Arc<CommandMailboxSignal>,
}

impl CommandSender {
    fn send(&self, command: Command) -> Result<(), async_mpsc::SendError<Command>> {
        let mut command = match self.inner.try_send(command) {
            Ok(()) => return Ok(()),
            Err(async_mpsc::SendError::Full(command)) => command,
            Err(error) => return Err(error),
        };

        // `Sender::reserve` currently requires a full-capability native Cx, so
        // synchronous callers cannot drive it with a detached no-capability
        // context. Keep those callers in the same bounded mailbox with a
        // blocking try-send adapter. The condition generation and notifier
        // share the Condvar mutex, closing both dequeue-before-predicate and
        // predicate-before-park races. Every production reservation/permit
        // owner signals after releasing capacity.
        //
        // Published messages retain channel order, and queued async reservers
        // retain their FIFO. Saturated synchronous callers are not registered
        // in that reservation queue, so no cross-mode admission FIFO or
        // starvation guarantee is made.
        let _waiter = SyncWaiterGuard::new(&self.signal.sync_waiters);
        let mut generation = self.signal.lock_generation();
        loop {
            // Keep the predicate mutex across the retry. A notifier that
            // releases capacity must acquire this same mutex before advancing
            // the generation, so it cannot notify between the final predicate
            // check and Condvar registration.
            let observed = *generation;
            #[cfg(test)]
            self.signal
                .sync_retry_attempts
                .fetch_add(1, Ordering::AcqRel);
            match self.inner.try_send(command) {
                Ok(()) => return Ok(()),
                Err(async_mpsc::SendError::Full(returned)) => {
                    command = returned;
                    generation = self.signal.wait_for_change(generation, observed);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn send_async(
        &self,
        preflight: &AsyncCallPreflight,
        command: Command,
    ) -> Result<(), FrankenError> {
        // One-lock uncontended path. `try_send` itself refuses to overtake a
        // queued async reserver, so `Full` is the only case that needs the
        // two-phase slow path below.
        preflight.check_cancellation()?;
        let command = match self.inner.try_send(command) {
            Ok(()) => {
                self.signal.record_async_publication();
                return Ok(());
            }
            Err(async_mpsc::SendError::Full(command)) => command,
            Err(error) => {
                // Cancellation wins over simultaneous mailbox termination.
                preflight.check_cancellation()?;
                return Err(async_admission_err(error));
            }
        };

        #[cfg(test)]
        let _reserver = SyncWaiterGuard::new(&self.signal.async_reservers);

        // Declaration order is deliberate: if this future is dropped while
        // reserve is pending, the later-declared reservation is dropped (and
        // removes its async waiter) before the guard wakes a saturated sync
        // sender.
        let mut capacity_change = CapacityChangeGuard::new(&self.signal);
        // Asupersync's MPSC reservation registers only with the capacity
        // queue. A detached attached NativeCx can therefore become cancelled
        // without waking a full-mailbox reserve. A never-sent 0.3.10 oneshot
        // receiver supplies the exact native-cancellation waker for this slow
        // path; its sender remains live until the race has completed.
        let (native_cancel_tx, mut native_cancel_rx) = oneshot::channel::<()>();
        // The outer `or` polls reservation first. If capacity and either
        // cancellation source become ready together, the permit still faces
        // the final cancellation recheck below before publication. Every
        // losing future unregisters synchronously on Drop.
        let reservation = {
            let reserve = async { Some(self.inner.reserve(&preflight.native_cx).await) };
            let native_cancelled = async {
                let result = native_cancel_rx.recv(&preflight.native_cx).await;
                debug_assert!(matches!(result, Err(oneshot::RecvError::Cancelled)));
                None
            };
            let locally_cancelled = async {
                preflight.control_cx.wait_for_local_cancel_request().await;
                None
            };
            future::or(reserve, future::or(native_cancelled, locally_cancelled)).await
        };
        drop(native_cancel_tx);
        let Some(reservation) = reservation else {
            preflight.check_cancellation()?;
            return Err(FrankenError::Interrupt);
        };
        let permit = match reservation {
            Ok(permit) => permit,
            Err(error) => {
                // Cancellation wins over simultaneous mailbox termination.
                preflight.check_cancellation()?;
                return Err(async_admission_err(error));
            }
        };

        #[cfg(test)]
        self.signal.pause_after_async_reservation().await;

        // Cancellation observed by this final recheck wins. The subsequent
        // `try_send` is the publication linearization point: cancellation
        // racing after the recheck may lose, after which the admitted command
        // runs to completion under the current worker protocol.
        preflight.check_cancellation()?;
        match permit.try_send(command) {
            Ok(()) => {
                capacity_change.disarm();
                self.signal.record_async_publication();
                Ok(())
            }
            Err(error) => Err(async_admission_err(error)),
        }
    }

    fn try_send(&self, command: Command) -> Result<(), async_mpsc::SendError<Command>> {
        self.inner.try_send(command)
    }
}

struct CommandReceiver {
    inner: async_mpsc::Receiver<Command>,
    signal: Arc<CommandMailboxSignal>,
}

impl CommandReceiver {
    fn recv(&mut self, cx: &NativeCx<native_cap::None>) -> Result<Command, async_mpsc::RecvError> {
        // Hot command streams usually already have another item queued after
        // the engine finishes the previous command. Avoid entering a second
        // executor in that case; only an actually empty mailbox parks in
        // `block_on`.
        let result = match self.inner.try_recv() {
            Err(async_mpsc::RecvError::Empty) => {
                #[cfg(test)]
                self.signal.blocking_receives.fetch_add(1, Ordering::AcqRel);
                future::block_on(self.inner.recv(cx))
            }
            ready => ready,
        };
        if result.is_ok() {
            self.signal.notify_capacity_change();
        }
        result
    }

    #[cfg(test)]
    fn try_recv(&mut self) -> Result<Command, async_mpsc::RecvError> {
        let result = self.inner.try_recv();
        if result.is_ok() {
            self.signal.notify_capacity_change();
        }
        result
    }
}

impl Drop for CommandReceiver {
    fn drop(&mut self) {
        let _terminal_notification = TerminalNotificationGuard(&self.signal);
        // Close before notifying synchronous adapters. A sender woken by the
        // companion signal must observe terminal state immediately and must
        // never publish into a receiver that is about to be destroyed.
        self.inner.close();
        #[cfg(test)]
        if self
            .signal
            .panic_on_receiver_drop
            .swap(false, Ordering::AcqRel)
        {
            panic!("async command receiver drop panic sentinel");
        }
    }
}

fn command_channel(capacity: usize) -> (CommandSender, CommandReceiver) {
    let (tx, rx) = async_mpsc::channel(capacity);
    let signal = Arc::new(CommandMailboxSignal::default());
    (
        CommandSender {
            inner: tx,
            signal: Arc::clone(&signal),
        },
        CommandReceiver { inner: rx, signal },
    )
}

fn worker_open_err() -> FrankenError {
    FrankenError::Internal("async worker thread terminated during open".to_owned())
}

fn worker_dead_err() -> FrankenError {
    FrankenError::Internal("async worker thread terminated unexpectedly".to_owned())
}

fn worker_join_admission_err() -> FrankenError {
    FrankenError::Internal(
        "caller runtime rejected the async worker join; close may be retried or completed with close_sync"
            .to_owned(),
    )
}

fn worker_join_task_err() -> FrankenError {
    FrankenError::Internal(
        "async worker join task ended after claiming the worker but before publishing its result"
            .to_owned(),
    )
}

fn stream_consumer_dead_err() -> FrankenError {
    FrankenError::Internal("synchronous query consumer stopped receiving rows".to_owned())
}

fn requires_runtime_err() -> FrankenError {
    FrankenError::Internal(
        "AsyncConnection async methods require an active asupersync runtime".to_owned(),
    )
}

fn requires_join_pool_err() -> FrankenError {
    FrankenError::Internal(
        "AsyncConnection close requires a configured asupersync blocking pool for worker join"
            .to_owned(),
    )
}

fn masked_context_err() -> FrankenError {
    FrankenError::Internal(
        "AsyncConnection async methods cannot start while the caller FrankenSQLite Cx is masked"
            .to_owned(),
    )
}

fn require_unmasked<Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>>(
    cx: &Cx<Caps>,
) -> Result<(), FrankenError> {
    if cx.mask_depth() == 0 {
        Ok(())
    } else {
        Err(masked_context_err())
    }
}

fn worker_thread_spawn_err(error: std::io::Error) -> FrankenError {
    FrankenError::Internal(format!("failed to spawn async-api worker thread: {error}"))
}

fn native_cx_for_local<Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>>(
    cx: &Cx<Caps>,
) -> Result<NativeCx, FrankenError> {
    cx.attached_native_cx()
        .or_else(NativeCx::current)
        .ok_or_else(requires_runtime_err)
}

struct AsyncCallPreflight {
    control_cx: Cx<fsqlite_types::cx::cap::None>,
    native_cx: NativeCx,
}

impl AsyncCallPreflight {
    fn validate_start(&self) -> Result<(), FrankenError> {
        require_unmasked(&self.control_cx)?;
        self.check_cancellation()?;
        // This is the mask linearization point. A mask acquired through a
        // shared alias after this check cannot retroactively invalidate the
        // already-started operation.
        require_unmasked(&self.control_cx)
    }

    fn check_cancellation(&self) -> Result<(), FrankenError> {
        if self.control_cx.is_cancel_requested() {
            return Err(FrankenError::Interrupt);
        }
        checkpoint_or_interrupt(&self.control_cx)?;
        // `checkpoint()` honors a mask that another alias may have acquired
        // after `validate_start`. Raw cancellation must still terminate this
        // already-started operation under the start-time snapshot contract.
        if self.control_cx.is_cancel_requested() {
            return Err(FrankenError::Interrupt);
        }
        native_checkpoint_or_interrupt(&self.native_cx)
    }
}

fn preflight_async_call<Caps>(cx: &Cx<Caps>) -> Result<AsyncCallPreflight, FrankenError>
where
    Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
{
    require_unmasked(cx)?;
    if cx.is_cancel_requested() {
        return Err(FrankenError::Interrupt);
    }
    Runtime::current_handle().ok_or_else(requires_runtime_err)?;
    let native_cx = native_cx_for_local(cx)?;
    // Restriction shares the caller's cancellation node without widening
    // capabilities, attaching a task-affine native context, or accumulating a
    // child registration per SQL call. Native cancellation is observed
    // independently by `native_cx`.
    let control_cx = cx.restrict::<fsqlite_types::cx::cap::None>();
    let preflight = AsyncCallPreflight {
        control_cx,
        native_cx,
    };
    preflight.validate_start()?;
    Ok(preflight)
}

fn current_join_pool(preflight: &AsyncCallPreflight) -> Result<BlockingPoolHandle, FrankenError> {
    preflight.check_cancellation()?;
    let runtime = Runtime::current_handle().ok_or_else(requires_runtime_err)?;
    let pool = runtime
        .blocking_handle()
        .ok_or_else(requires_join_pool_err)?;
    preflight.check_cancellation()?;
    Ok(pool)
}

enum AsyncReceive<T> {
    Completed(T),
    Cancelled,
    Closed,
}

async fn wait_for_async_value<T>(
    preflight: &AsyncCallPreflight,
    rx: &mut oneshot::Receiver<T>,
) -> AsyncReceive<T> {
    let response = async {
        match rx.recv(&preflight.native_cx).await {
            Ok(result) => AsyncReceive::Completed(result),
            Err(oneshot::RecvError::Cancelled) => AsyncReceive::Cancelled,
            Err(oneshot::RecvError::Closed | oneshot::RecvError::PolledAfterCompletion) => {
                AsyncReceive::Closed
            }
        }
    };
    let locally_cancelled = async {
        preflight.control_cx.wait_for_local_cancel_request().await;
        AsyncReceive::Cancelled
    };

    // A committed response wins when both branches become ready in the same
    // poll. Bare response-channel closure is not a committed result: match the
    // admission contract by letting simultaneous cancellation win that tie.
    // Once admission has published a command, cancellation abandons only this
    // response wait; the worker still owns and completes the admitted effect.
    let outcome = future::or(response, locally_cancelled).await;
    if matches!(&outcome, AsyncReceive::Closed) && preflight.check_cancellation().is_err() {
        AsyncReceive::Cancelled
    } else {
        outcome
    }
}

async fn recv_async_response<T>(
    preflight: &AsyncCallPreflight,
    rx: &mut oneshot::Receiver<Result<T, FrankenError>>,
) -> Result<Result<T, FrankenError>, FrankenError> {
    match wait_for_async_value(preflight, rx).await {
        AsyncReceive::Completed(result) => Ok(result),
        AsyncReceive::Cancelled => Err(FrankenError::Interrupt),
        AsyncReceive::Closed => Err(worker_dead_err()),
    }
}

fn recv_worker_response<T>(rx: mpsc::Receiver<Result<T, FrankenError>>) -> Result<T, FrankenError> {
    rx.recv().map_err(|_| worker_dead_err())?
}

// ---------------------------------------------------------------------------
// Worker task
// ---------------------------------------------------------------------------

enum WorkerStop {
    ExplicitClose,
    Shutdown,
    CommandChannelDisconnected,
}

fn publish_and_respond<T>(
    conn: &Connection,
    state: &WorkerState,
    tx: Responder<T>,
    result: Result<T, FrankenError>,
) {
    // The worker publishes the engine's actual state before making the command
    // result visible. This also covers transaction control expressed as SQL
    // text and rollback-on-error paths, not just the convenience methods.
    state.publish_connection_state(conn);
    tx.respond(result);
}

fn worker_loop(conn: &Connection, rx: &mut CommandReceiver, state: &WorkerState) -> WorkerStop {
    let worker_cx = NativeCx::<native_cap::None>::detached_cancel_context();
    loop {
        let cmd = match rx.recv(&worker_cx) {
            Ok(cmd) => cmd,
            Err(
                async_mpsc::RecvError::Disconnected
                | async_mpsc::RecvError::Cancelled
                | async_mpsc::RecvError::Empty,
            ) => {
                return WorkerStop::CommandChannelDisconnected;
            }
        };

        match cmd {
            Command::Prepare { sql, tx } => {
                let result = future::block_on(conn.prepare(&sql)).map(drop);
                publish_and_respond(conn, state, tx, result);
            }
            Command::Query { sql, tx } => {
                let result = future::block_on(conn.query(&sql));
                publish_and_respond(conn, state, tx, result);
            }
            Command::QueryWithParams { sql, params, tx } => {
                let result = future::block_on(conn.query_with_params(&sql, &params));
                publish_and_respond(conn, state, tx, result);
            }
            Command::QueryWithParamsStream { sql, params, tx } => {
                let mut published_before_first_row = false;
                let result =
                    future::block_on(conn.query_with_params_for_each(&sql, &params, |row| {
                        // The core executes the complete batch before visiting the
                        // last statement's rows. A synchronous callback can inspect
                        // `in_transaction()` before this command's terminal response,
                        // so publish that completed-batch state before exposing row 1.
                        if !published_before_first_row {
                            state.publish_connection_state(conn);
                            published_before_first_row = true;
                        }
                        tx.send(Ok(Some(row.clone())))
                            .map_err(|_| stream_consumer_dead_err())
                    }));
                state.publish_connection_state(conn);
                match result {
                    Ok(()) => {
                        let _ = tx.send(Ok(None));
                    }
                    Err(error) => {
                        let _ = tx.send(Err(error));
                    }
                }
            }
            Command::QueryRow { sql, tx } => {
                let result = future::block_on(conn.query_row(&sql));
                publish_and_respond(conn, state, tx, result);
            }
            Command::QueryRowWithParams { sql, params, tx } => {
                let result = future::block_on(conn.query_row_with_params(&sql, &params));
                publish_and_respond(conn, state, tx, result);
            }
            Command::Execute { sql, tx } => {
                let result = future::block_on(conn.execute(&sql));
                publish_and_respond(conn, state, tx, result);
            }
            Command::ExecuteWithParams { sql, params, tx } => {
                let result = future::block_on(conn.execute_with_params(&sql, &params));
                publish_and_respond(conn, state, tx, result);
            }
            Command::ExecuteManyWithParamsInTransaction {
                sql,
                parameter_sets,
                tx,
            } => {
                let result = future::block_on(
                    conn.execute_many_with_params_skip_statement_savepoint_in_explicit_txn(
                        &sql,
                        &parameter_sets,
                    ),
                );
                publish_and_respond(conn, state, tx, result);
            }
            Command::ExecuteBatch { sql, tx } => {
                let result = future::block_on(conn.execute_batch(&sql));
                publish_and_respond(conn, state, tx, result);
            }
            Command::BeginTransaction { tx } => {
                let result = future::block_on(conn.begin_transaction());
                publish_and_respond(conn, state, tx, result);
            }
            Command::CommitTransaction { tx } => {
                let result = future::block_on(conn.commit_transaction());
                publish_and_respond(conn, state, tx, result);
            }
            Command::RollbackTransaction { tx } => {
                let result = future::block_on(conn.rollback_transaction());
                publish_and_respond(conn, state, tx, result);
            }
            Command::LastInsertRowid { tx } => {
                let result = Ok(conn.last_insert_rowid());
                publish_and_respond(conn, state, tx, result);
            }
            Command::Close => {
                return WorkerStop::ExplicitClose;
            }
            Command::Shutdown => {
                return WorkerStop::Shutdown;
            }
            #[cfg(test)]
            Command::BlockForTest {
                entered_tx,
                release_rx,
            } => {
                let _ = entered_tx.send(());
                let _ = release_rx.recv();
            }
            #[cfg(test)]
            Command::PanicForTest => {
                panic!("async worker command panic sentinel");
            }
        }
    }
}

fn panic_payload_text(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else {
        "non-string panic payload".to_owned()
    }
}

fn run_worker_to_terminal(
    mut conn: Connection,
    mut rx: CommandReceiver,
    state: &WorkerState,
) -> Result<(), FrankenError> {
    let loop_result = catch_unwind(AssertUnwindSafe(|| worker_loop(&conn, &mut rx, state)));
    // Receiver close wakes blocked senders. A hostile or buggy waker must not
    // be able to skip the worker's sole connection-cleanup path.
    let receiver_drop_result = catch_unwind(AssertUnwindSafe(|| drop(rx)));

    state.publish_phase(WorkerPhase::Closing);
    let cleanup_result = catch_unwind(AssertUnwindSafe(|| {
        #[cfg(test)]
        {
            state.cleanup_calls.fetch_add(1, Ordering::AcqRel);
            if state.panic_on_cleanup.swap(false, Ordering::AcqRel) {
                panic!("async worker cleanup panic sentinel");
            }
        }
        future::block_on(conn.close_in_place())
    }));
    state.publish_phase(WorkerPhase::Terminal);

    let mut failures = Vec::with_capacity(3);
    if let Err(worker_panic) = loop_result {
        failures.push(format!(
            "async worker command loop panicked: {}",
            panic_payload_text(worker_panic.as_ref())
        ));
    }
    if let Err(receiver_panic) = receiver_drop_result {
        failures.push(format!(
            "async worker command receiver close panicked: {}",
            panic_payload_text(receiver_panic.as_ref())
        ));
    }
    match cleanup_result {
        Ok(Ok(())) => {}
        Ok(Err(cleanup_error)) if failures.is_empty() => return Err(cleanup_error),
        Ok(Err(cleanup_error)) => {
            failures.push(format!("close cleanup failed: {cleanup_error}"));
        }
        Err(cleanup_panic) => {
            failures.push(format!(
                "close cleanup panicked: {}",
                panic_payload_text(cleanup_panic.as_ref())
            ));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(FrankenError::Internal(failures.join("; ")))
    }
}

struct WorkerTerminalOutcome {
    result: Option<Result<(), FrankenError>>,
    open_error: Option<Arc<OpenErrorDiagnostic>>,
    state: Arc<WorkerState>,
}

impl WorkerTerminalOutcome {
    fn new(
        result: Result<(), FrankenError>,
        open_error: Option<Arc<OpenErrorDiagnostic>>,
        state: Arc<WorkerState>,
    ) -> Self {
        Self {
            result: Some(result),
            open_error,
            state,
        }
    }

    fn into_result(mut self) -> Result<(), FrankenError> {
        let result = self.result.take().unwrap_or_else(|| {
            Err(FrankenError::Internal(
                "async worker terminal outcome was consumed twice".to_owned(),
            ))
        });
        let open_error = self.open_error.take();
        match result {
            Err(error) => {
                if let Some(diagnostic) = open_error {
                    diagnostic.acknowledge();
                }
                Err(error)
            }
            Ok(()) => {
                if let Some(diagnostic) = open_error {
                    if diagnostic.is_acknowledged() {
                        Ok(())
                    } else {
                        let message = Arc::clone(&diagnostic.message);
                        diagnostic.acknowledge();
                        Err(FrankenError::Internal(format!(
                            "async worker open failed before its response was observed: {message}"
                        )))
                    }
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl Drop for WorkerTerminalOutcome {
    fn drop(&mut self) {
        let Some(result) = self.result.take() else {
            return;
        };
        if let Err(error) = result {
            if let Some(diagnostic) = self.open_error.take() {
                diagnostic.acknowledge();
            }
            report_unobserved_worker_error(&self.state, &error.to_string());
        }
    }
}

struct WorkerHandle {
    thread: JoinHandle<WorkerTerminalOutcome>,
    state: Arc<WorkerState>,
}

impl WorkerHandle {
    fn join(self) -> WorkerTerminalOutcome {
        match self.thread.join() {
            Ok(outcome) => outcome,
            Err(panic) => WorkerTerminalOutcome::new(
                Err(FrankenError::Internal(format!(
                    "async worker thread panicked outside its terminal guard: {}",
                    panic_payload_text(panic.as_ref())
                ))),
                None,
                self.state,
            ),
        }
    }

    fn wait(self) -> Result<(), FrankenError> {
        self.join().into_result()
    }
}

#[derive(Clone)]
enum CloseMemo {
    Success,
    Failure(Arc<FrankenError>),
}

impl CloseMemo {
    fn replay(&self) -> Result<(), Arc<FrankenError>> {
        match self {
            Self::Success => Ok(()),
            Self::Failure(error) => Err(Arc::clone(error)),
        }
    }
}

struct JoinFlight {
    worker_slot: Arc<Mutex<Option<WorkerHandle>>>,
    _task: BlockingTaskHandle,
    result_rx: oneshot::Receiver<WorkerTerminalOutcome>,
}

impl JoinFlight {
    fn start(pool: &BlockingPoolHandle, worker: WorkerHandle) -> Result<Self, WorkerHandle> {
        let state = Arc::clone(&worker.state);
        let worker_slot = Arc::new(Mutex::new(Some(worker)));
        let worker_slot_for_task = Arc::clone(&worker_slot);
        let (result_tx, result_rx) = oneshot::channel();
        let task = pool.spawn(move || {
            let worker = match worker_slot_for_task.lock() {
                Ok(mut slot) => slot.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };
            let result = worker.map_or_else(
                || {
                    WorkerTerminalOutcome::new(
                        Err(FrankenError::Internal(
                            "async worker join task started without worker ownership".to_owned(),
                        )),
                        None,
                        state,
                    )
                },
                WorkerHandle::join,
            );
            let _ = result_tx.send_blocking(result);
        });

        // A shutting-down pool rejects by dropping the closure and returning
        // an already-cancelled, already-complete handle. Recover the exact
        // worker from the shared owner slot instead of silently detaching it.
        if task.is_cancelled() && task.is_done() {
            let recovered = match worker_slot.lock() {
                Ok(mut slot) => slot.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };
            if let Some(worker) = recovered {
                return Err(worker);
            }
        }

        Ok(Self {
            worker_slot,
            _task: task,
            result_rx,
        })
    }

    fn recover_unclaimed_worker(&self) -> Option<WorkerHandle> {
        match self.worker_slot.lock() {
            Ok(mut slot) => slot.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        }
    }
}

enum JoinOwnership {
    Unscheduled(WorkerHandle),
    InFlight(JoinFlight),
}

enum WorkerLifecycle {
    Running {
        tx: CommandSender,
        worker: WorkerHandle,
    },
    Closing {
        join: JoinOwnership,
    },
    Terminal(CloseMemo),
}

struct PendingOpen {
    tx: Option<CommandSender>,
    worker: Option<WorkerHandle>,
}

impl PendingOpen {
    fn new(tx: CommandSender, worker: WorkerHandle) -> Self {
        Self {
            tx: Some(tx),
            worker: Some(worker),
        }
    }

    fn into_running(mut self) -> Result<(CommandSender, WorkerHandle), FrankenError> {
        let tx = self.tx.take().ok_or_else(|| {
            FrankenError::Internal("pending async open lost its command sender".to_owned())
        })?;
        let worker = self.worker.take().ok_or_else(|| {
            FrankenError::Internal("pending async open lost its worker handle".to_owned())
        })?;
        Ok((tx, worker))
    }

    fn into_worker_for_join(mut self) -> Result<WorkerHandle, FrankenError> {
        // Disconnect first. If the connection opened after the caller stopped
        // observing it, the worker must drain directly into its sole cleanup
        // path rather than wait for a command that can never arrive.
        drop(self.tx.take());
        self.worker.take().ok_or_else(|| {
            FrankenError::Internal("pending async open lost its worker handle".to_owned())
        })
    }
}

impl Drop for PendingOpen {
    fn drop(&mut self) {
        // Dropping a JoinHandle detaches without stopping the worker. Sender
        // disconnection is the shutdown fence; the worker still owns and
        // closes any successfully opened Connection exactly once.
        drop(self.tx.take());
        drop(self.worker.take());
    }
}

fn spawn_worker_thread(
    path: String,
    env: ConnectionEnv,
    cmd_rx: CommandReceiver,
    open_tx: Responder<OpenHandshake>,
    state: Arc<WorkerState>,
) -> Result<WorkerHandle, FrankenError> {
    let handle_state = Arc::clone(&state);
    thread::Builder::new()
        .name("fsqlite-worker".to_owned())
        .stack_size(WORKER_STACK_BYTES)
        .spawn(move || {
            let outcome_state = Arc::clone(&state);
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                #[cfg(test)]
                let open_result = match state.take_forced_open_error() {
                    Some(error) => Err(error),
                    None => future::block_on(Connection::open_with_env(path, env)),
                };
                #[cfg(not(test))]
                let open_result = future::block_on(Connection::open_with_env(path, env));

                match open_result {
                    Ok(conn) => {
                        // bd-bjm5d: this thread is a dedicated engine OS thread —
                        // it runs a bare single-future executor
                        // (futures_lite::block_on) that owns exactly one
                        // Connection and serializes its command stream, so a
                        // bounded inline pread/pwrite can stall nothing but this
                        // connection's own next command. This is the ONLY
                        // permitted set site (enforced by
                        // blocking_io_inline_marker_has_exactly_one_set_site).
                        conn.root_cx().mark_blocking_io_inline_safe();
                        #[cfg(test)]
                        state.pause_before_open_response();
                        open_tx.respond(Ok(OpenHandshake::Opened));
                        #[cfg(test)]
                        state.open_response_committed.store(true, Ordering::Release);
                        WorkerTerminalOutcome::new(
                            run_worker_to_terminal(conn, cmd_rx, &state),
                            None,
                            Arc::clone(&state),
                        )
                    }
                    Err(error) => {
                        state.publish_phase(WorkerPhase::Terminal);
                        let diagnostic =
                            Arc::new(OpenErrorDiagnostic::new(&error, Arc::clone(&state)));
                        #[cfg(test)]
                        state.pause_before_open_response();
                        open_tx.respond(Ok(OpenHandshake::Failed {
                            error,
                            diagnostic: Arc::clone(&diagnostic),
                        }));
                        #[cfg(test)]
                        state.open_response_committed.store(true, Ordering::Release);
                        WorkerTerminalOutcome::new(Ok(()), Some(diagnostic), Arc::clone(&state))
                    }
                }
            }));
            state.publish_phase(WorkerPhase::Terminal);
            match outcome {
                Ok(outcome) => outcome,
                Err(panic) => WorkerTerminalOutcome::new(
                    Err(FrankenError::Internal(format!(
                        "async worker thread panicked outside its terminal guard: {}",
                        panic_payload_text(panic.as_ref())
                    ))),
                    None,
                    outcome_state,
                ),
            }
        })
        .map(|thread| WorkerHandle {
            thread,
            state: handle_state,
        })
        .map_err(worker_thread_spawn_err)
}

fn wait_for_worker_open(
    open_rx: mpsc::Receiver<Result<OpenHandshake, FrankenError>>,
) -> Result<OpenHandshake, FrankenError> {
    open_rx.recv().map_err(|_| worker_open_err())?
}

// ---------------------------------------------------------------------------
// Cx → FrankenError bridge
// ---------------------------------------------------------------------------

/// Map a `Cx::checkpoint()` cancellation error to a `FrankenError::Interrupt`.
fn checkpoint_or_interrupt<Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>>(
    cx: &Cx<Caps>,
) -> Result<(), FrankenError> {
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)
}

fn native_checkpoint_or_interrupt(cx: &NativeCx) -> Result<(), FrankenError> {
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)
}

/// Map a synchronous command-send error to a worker-lifecycle error.
fn send_err<T>(_: async_mpsc::SendError<T>) -> FrankenError {
    FrankenError::Internal("async worker thread is no longer running".to_owned())
}

fn async_admission_err<T>(error: async_mpsc::SendError<T>) -> FrankenError {
    match error {
        async_mpsc::SendError::Cancelled(_) => FrankenError::Interrupt,
        async_mpsc::SendError::Disconnected(_) => worker_dead_err(),
        async_mpsc::SendError::Full(_) => FrankenError::Internal(
            "async command mailbox lost a previously reserved slot".to_owned(),
        ),
    }
}

// ---------------------------------------------------------------------------
// AsyncConnection
// ---------------------------------------------------------------------------

/// Async-native wrapper around [`Connection`] for use with asupersync's `Cx`
/// capability context.
///
/// All async methods validate `cx.checkpoint()`, the current runtime, and a
/// usable native cancellation context before dispatching. Only async close
/// needs the runtime's blocking pool in order to observe the OS-thread join.
/// A failed preflight returns without touching the underlying connection.
/// Commands that pass preflight currently run to completion once admitted to
/// the worker.
///
/// A caller context with a nonzero cancellation-mask depth is rejected when
/// the call starts. A mask acquired through another alias after that start-time
/// check does not retroactively invalidate the operation or defer its
/// cancellation. Do not enter a new async worker operation while holding a
/// [`fsqlite_types::cx::MaskGuard`].
///
/// The connection itself lives on a dedicated large-stack worker thread (because
/// [`Connection`] is `!Send`). Commands are dispatched via an internal channel
/// and results flow back through response waiters owned by the caller runtime.
///
/// # Shutdown
///
/// When `AsyncConnection` is dropped, the worker thread is signalled to shut
/// down. The underlying [`Connection`] is closed on the worker thread as part
/// of its normal drop sequence.
///
/// For explicit, error-checked shutdown use [`close`](Self::close) on the
/// async path or [`close_sync`](Self::close_sync) on the synchronous path.
pub struct AsyncConnection {
    lifecycle: WorkerLifecycle,
    /// Worker-published transaction and terminal phase. The dedicated worker is
    /// the only writer, so cancellation cannot leave caller-maintained state
    /// behind the engine's actual transaction state.
    state: Arc<WorkerState>,
    /// A synchronous row callback executes while the worker is blocked on its
    /// one-row response channel. Dispatching another command through this same
    /// connection in that interval would wait on the blocked worker forever.
    sync_stream_active: AtomicBool,
}

struct SyncStreamGuard<'a> {
    active: &'a AtomicBool,
}

impl<'a> SyncStreamGuard<'a> {
    fn enter(active: &'a AtomicBool) -> Result<Self, FrankenError> {
        active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self { active })
            .map_err(|_| FrankenError::SynchronousStreamReentrancy)
    }
}

impl Drop for SyncStreamGuard<'_> {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

impl AsyncConnection {
    /// Open a database connection asynchronously with `Cx` integration.
    ///
    /// The `Cx` is checkpointed before the blocking open. On success, a
    /// dedicated large-stack worker thread is spawned to own the `Connection`.
    pub async fn open<Caps>(cx: &Cx<Caps>, path: impl Into<String>) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        Self::open_with_env(cx, path, ConnectionEnv::default()).await
    }

    /// Open a database connection without a capability context (convenience).
    ///
    /// The raw connection is born on a dedicated large-stack worker thread and
    /// remains there for its lifetime. No cancellation check is performed;
    /// synchronous consumers should use the `*_sync` methods and
    /// [`close_sync`](Self::close_sync).
    pub fn open_sync(path: impl Into<String>) -> Result<Self, FrankenError> {
        Self::open_sync_with_env(path, ConnectionEnv::default())
    }

    /// Open a database connection without a capability context, with a custom
    /// [`ConnectionEnv`].
    pub fn open_sync_with_env(
        path: impl Into<String>,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError> {
        let path = path.into();
        let (open_tx, open_rx) = sync_response_channel();
        let (cmd_tx, cmd_rx) = command_channel(COMMAND_MAILBOX_CAPACITY);
        let state = Arc::new(WorkerState::new());
        let worker = spawn_worker_thread(path, env, cmd_rx, open_tx, Arc::clone(&state))?;

        match wait_for_worker_open(open_rx) {
            Ok(OpenHandshake::Opened) => Ok(Self {
                lifecycle: WorkerLifecycle::Running { tx: cmd_tx, worker },
                state,
                sync_stream_active: AtomicBool::new(false),
            }),
            Ok(OpenHandshake::Failed { error, diagnostic }) => {
                diagnostic.acknowledge();
                match worker.wait() {
                    Ok(()) => Err(error),
                    Err(worker_error) => Err(worker_error),
                }
            }
            Err(error) => match worker.wait() {
                Ok(()) => Err(error),
                Err(worker_error) => Err(worker_error),
            },
        }
    }

    async fn finish_pending_open(
        preflight: AsyncCallPreflight,
        pending: PendingOpen,
        mut open_rx: oneshot::Receiver<Result<OpenHandshake, FrankenError>>,
        state: Arc<WorkerState>,
    ) -> Result<Self, FrankenError> {
        match recv_async_response(&preflight, &mut open_rx).await {
            Ok(Ok(OpenHandshake::Opened)) => {
                let (tx, worker) = pending.into_running()?;
                Ok(Self {
                    lifecycle: WorkerLifecycle::Running { tx, worker },
                    state,
                    sync_stream_active: AtomicBool::new(false),
                })
            }
            Ok(Ok(OpenHandshake::Failed { error, diagnostic })) => {
                // No suspension is permitted after acknowledgement. The
                // already-committed public error becomes Ready in this same
                // poll, while sender disconnection leaves the detached worker
                // responsible for its one terminal cleanup path.
                let worker = pending.into_worker_for_join()?;
                diagnostic.acknowledge();
                drop(worker);
                Err(error)
            }
            Ok(Err(error)) => {
                let worker = pending.into_worker_for_join()?;
                drop(worker);
                Err(error)
            }
            Err(FrankenError::Interrupt) => {
                drop(pending);
                Err(FrankenError::Interrupt)
            }
            Err(error) => {
                let worker = pending.into_worker_for_join()?;
                drop(worker);
                Err(error)
            }
        }
    }

    /// Open a database connection with an explicit [`ConnectionEnv`].
    pub async fn open_with_env<Caps>(
        cx: &Cx<Caps>,
        path: impl Into<String>,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;

        let path = path.into();

        // The raw engine lives on a dedicated large-stack thread. The
        // connection is !Send, so it must be born on and stay on that thread.
        // Open admission and its response use native async channels and do not
        // require a blocking pool.
        let (open_tx, open_rx) = async_response_channel();
        let (cmd_tx, cmd_rx) = command_channel(COMMAND_MAILBOX_CAPACITY);
        let state = Arc::new(WorkerState::new());
        preflight.check_cancellation()?;
        let worker = spawn_worker_thread(path, env, cmd_rx, open_tx, Arc::clone(&state))?;
        let pending = PendingOpen::new(cmd_tx, worker);

        // PendingOpen is the cancellation and arbitrary-Future-drop fence.
        // It disconnects the command channel before detaching the join handle,
        // leaving the worker itself responsible for exactly-once Connection
        // cleanup without delaying cancellation on an OS-thread join.
        Self::finish_pending_open(preflight, pending, open_rx, state).await
    }

    /// Return a reference to the command sender, or an error if the worker is gone.
    fn sender(&self) -> Result<&CommandSender, FrankenError> {
        if self.sync_stream_active.load(Ordering::Acquire) {
            return Err(FrankenError::SynchronousStreamReentrancy);
        }
        self.running_sender()
    }

    fn running_sender(&self) -> Result<&CommandSender, FrankenError> {
        match &self.lifecycle {
            WorkerLifecycle::Running { tx, .. } => Ok(tx),
            WorkerLifecycle::Closing { .. } | WorkerLifecycle::Terminal(_) => Err(
                FrankenError::Internal("AsyncConnection has been closed".to_owned()),
            ),
        }
    }

    /// Validate and prepare one SQL statement on the dedicated worker.
    ///
    /// This is the synchronous-consumer counterpart to the async methods
    /// below. It intentionally performs no cancellation check and blocks the
    /// caller until the worker responds.
    pub fn prepare_sync(&self, sql: &str) -> Result<(), FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::Prepare {
                sql: sql.to_owned(),
                tx,
            })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Execute a query through the dedicated worker and block for all rows.
    pub fn query_sync(&self, sql: &str) -> Result<Vec<Row>, FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::Query {
                sql: sql.to_owned(),
                tx,
            })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Execute a parameterized query through the dedicated worker.
    pub fn query_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::QueryWithParams {
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Stream a parameterized query through a one-row bounded worker channel.
    ///
    /// The callback runs on the caller thread. Returning an error stops the
    /// stream, releases the worker, and returns that callback error. Dispatching
    /// another operation through this same connection from the callback is
    /// rejected with [`FrankenError::SynchronousStreamReentrancy`]; use another
    /// connection or defer it until this method returns.
    pub fn query_with_params_for_each_sync<F>(
        &self,
        sql: &str,
        params: &[SqliteValue],
        mut f: F,
    ) -> Result<(), FrankenError>
    where
        F: FnMut(&Row) -> Result<(), FrankenError>,
    {
        let _stream_guard = SyncStreamGuard::enter(&self.sync_stream_active)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.running_sender()?
            .send(Command::QueryWithParamsStream {
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
            .map_err(send_err)?;

        loop {
            match rx.recv().map_err(|_| worker_dead_err())?? {
                Some(row) => f(&row)?,
                None => return Ok(()),
            }
        }
    }

    /// Execute a query through the dedicated worker and return exactly one row.
    pub fn query_row_sync(&self, sql: &str) -> Result<Row, FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::QueryRow {
                sql: sql.to_owned(),
                tx,
            })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Execute a parameterized query and return exactly one row.
    pub fn query_row_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Row, FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::QueryRowWithParams {
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Execute SQL through the dedicated worker.
    pub fn execute_sync(&self, sql: &str) -> Result<usize, FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::Execute {
                sql: sql.to_owned(),
                tx,
            })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Execute parameterized SQL through the dedicated worker.
    pub fn execute_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::ExecuteWithParams {
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Execute parameter sets as one worker command inside an explicit transaction.
    ///
    /// Inputs must be fully validated before this call. The worker deliberately
    /// skips per-statement savepoints; if one execution fails, earlier effects
    /// remain pending and the caller must roll back the enclosing transaction.
    pub fn execute_many_with_params_in_transaction_sync(
        &self,
        sql: &str,
        parameter_sets: &[Vec<SqliteValue>],
    ) -> Result<usize, FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::ExecuteManyWithParamsInTransaction {
                sql: sql.to_owned(),
                parameter_sets: parameter_sets.to_vec(),
                tx,
            })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Execute zero or more SQL statements through the dedicated worker.
    pub fn execute_batch_sync(&self, sql: &str) -> Result<(), FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::ExecuteBatch {
                sql: sql.to_owned(),
                tx,
            })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Begin a transaction through the dedicated worker.
    pub fn begin_transaction_sync(&self) -> Result<(), FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::BeginTransaction { tx })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Commit the active transaction through the dedicated worker.
    pub fn commit_transaction_sync(&self) -> Result<(), FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::CommitTransaction { tx })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Roll back the active transaction through the dedicated worker.
    pub fn rollback_transaction_sync(&self) -> Result<(), FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::RollbackTransaction { tx })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Return the worker-owned connection's last inserted row identifier.
    pub fn last_insert_rowid_sync(&self) -> Result<i64, FrankenError> {
        let (tx, rx) = sync_response_channel();
        self.sender()?
            .send(Command::LastInsertRowid { tx })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Validate and prepare one SQL statement on the dedicated worker.
    pub async fn prepare<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;
        let (tx, mut rx) = async_response_channel();
        self.sender()?
            .send_async(
                &preflight,
                Command::Prepare {
                    sql: sql.to_owned(),
                    tx,
                },
            )
            .await?;
        recv_async_response(&preflight, &mut rx).await?
    }

    /// Execute a SQL query and return all result rows.
    pub async fn query<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<Vec<Row>, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;
        let (tx, mut rx) = async_response_channel();
        self.sender()?
            .send_async(
                &preflight,
                Command::Query {
                    sql: sql.to_owned(),
                    tx,
                },
            )
            .await?;
        recv_async_response(&preflight, &mut rx).await?
    }

    /// Execute a query with bound parameters and return all result rows.
    pub async fn query_with_params<Caps>(
        &self,
        cx: &Cx<Caps>,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;
        let (tx, mut rx) = async_response_channel();
        self.sender()?
            .send_async(
                &preflight,
                Command::QueryWithParams {
                    sql: sql.to_owned(),
                    params: params.to_vec(),
                    tx,
                },
            )
            .await?;
        recv_async_response(&preflight, &mut rx).await?
    }

    /// Execute a query and return exactly one row.
    pub async fn query_row<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<Row, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;
        let (tx, mut rx) = async_response_channel();
        self.sender()?
            .send_async(
                &preflight,
                Command::QueryRow {
                    sql: sql.to_owned(),
                    tx,
                },
            )
            .await?;
        recv_async_response(&preflight, &mut rx).await?
    }

    /// Execute a query with parameters and return exactly one row.
    pub async fn query_row_with_params<Caps>(
        &self,
        cx: &Cx<Caps>,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Row, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;
        let (tx, mut rx) = async_response_channel();
        self.sender()?
            .send_async(
                &preflight,
                Command::QueryRowWithParams {
                    sql: sql.to_owned(),
                    params: params.to_vec(),
                    tx,
                },
            )
            .await?;
        recv_async_response(&preflight, &mut rx).await?
    }

    /// Execute SQL and return the number of affected/output rows.
    pub async fn execute<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<usize, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;
        let (tx, mut rx) = async_response_channel();
        self.sender()?
            .send_async(
                &preflight,
                Command::Execute {
                    sql: sql.to_owned(),
                    tx,
                },
            )
            .await?;
        recv_async_response(&preflight, &mut rx).await?
    }

    /// Execute SQL with bound parameters and return the number of affected/output rows.
    pub async fn execute_with_params<Caps>(
        &self,
        cx: &Cx<Caps>,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;
        let (tx, mut rx) = async_response_channel();
        self.sender()?
            .send_async(
                &preflight,
                Command::ExecuteWithParams {
                    sql: sql.to_owned(),
                    params: params.to_vec(),
                    tx,
                },
            )
            .await?;
        recv_async_response(&preflight, &mut rx).await?
    }

    /// Execute zero or more SQL statements separated by semicolons.
    pub async fn execute_batch<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;
        let (tx, mut rx) = async_response_channel();
        self.sender()?
            .send_async(
                &preflight,
                Command::ExecuteBatch {
                    sql: sql.to_owned(),
                    tx,
                },
            )
            .await?;
        recv_async_response(&preflight, &mut rx).await?
    }

    /// Begin a transaction.
    pub async fn begin_transaction<Caps>(&self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;
        let (tx, mut rx) = async_response_channel();
        self.sender()?
            .send_async(&preflight, Command::BeginTransaction { tx })
            .await?;
        recv_async_response(&preflight, &mut rx).await?
    }

    /// Commit the active transaction.
    pub async fn commit_transaction<Caps>(&self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;
        let (tx, mut rx) = async_response_channel();
        self.sender()?
            .send_async(&preflight, Command::CommitTransaction { tx })
            .await?;
        recv_async_response(&preflight, &mut rx).await?
    }

    /// Roll back the active transaction.
    pub async fn rollback_transaction<Caps>(&self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        let preflight = preflight_async_call(cx)?;
        let (tx, mut rx) = async_response_channel();
        self.sender()?
            .send_async(&preflight, Command::RollbackTransaction { tx })
            .await?;
        recv_async_response(&preflight, &mut rx).await?
    }

    /// Returns `true` if an explicit transaction is currently active.
    ///
    /// This is a cheap local read — no round-trip to the worker thread.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.state.in_transaction()
    }

    fn begin_close(&mut self, command: Command) {
        let lifecycle = std::mem::replace(
            &mut self.lifecycle,
            WorkerLifecycle::Terminal(CloseMemo::Success),
        );
        self.lifecycle = match lifecycle {
            WorkerLifecycle::Running { tx, worker } => {
                // A full mailbox may reject the marker. Dropping the last
                // sender remains the authoritative FIFO shutdown signal: all
                // already-published commands drain before disconnection.
                let _ = tx.try_send(command);
                drop(tx);
                WorkerLifecycle::Closing {
                    join: JoinOwnership::Unscheduled(worker),
                }
            }
            lifecycle @ (WorkerLifecycle::Closing { .. } | WorkerLifecycle::Terminal(_)) => {
                lifecycle
            }
        };
    }

    fn ensure_join_scheduled(&mut self, pool: &BlockingPoolHandle) -> Result<(), FrankenError> {
        let lifecycle = std::mem::replace(
            &mut self.lifecycle,
            WorkerLifecycle::Terminal(CloseMemo::Success),
        );
        self.lifecycle = match lifecycle {
            WorkerLifecycle::Closing {
                join: JoinOwnership::Unscheduled(worker),
            } => match JoinFlight::start(pool, worker) {
                Ok(flight) => WorkerLifecycle::Closing {
                    join: JoinOwnership::InFlight(flight),
                },
                Err(worker) => {
                    self.lifecycle = WorkerLifecycle::Closing {
                        join: JoinOwnership::Unscheduled(worker),
                    };
                    return Err(worker_join_admission_err());
                }
            },
            lifecycle => lifecycle,
        };
        Ok(())
    }

    fn finish_close(&mut self, result: Result<(), FrankenError>) -> Result<(), Arc<FrankenError>> {
        match result {
            Ok(()) => {
                self.lifecycle = WorkerLifecycle::Terminal(CloseMemo::Success);
                Ok(())
            }
            Err(error) => {
                let error = Arc::new(error);
                self.lifecycle = WorkerLifecycle::Terminal(CloseMemo::Failure(Arc::clone(&error)));
                Err(error)
            }
        }
    }

    /// Explicitly close the connection, returning any error from the close operation.
    ///
    /// After this call, all subsequent operations will return an error. On
    /// uninterrupted completion, the worker thread has been joined before
    /// returning. If the wait is cancelled, this connection retains both join
    /// ownership and the terminal result so a later `close`/`close_sync` can
    /// resume the same obligation. Terminal failures are returned in an
    /// [`Arc`] so every replay preserves the exact error variant, payload,
    /// source chain, and allocation identity.
    pub async fn close<Caps>(&mut self, cx: &Cx<Caps>) -> Result<(), Arc<FrankenError>>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        fsqlite_types::cx::cap::None: fsqlite_types::cx::cap::SubsetOf<Caps>,
    {
        if let WorkerLifecycle::Terminal(memo) = &self.lifecycle {
            return memo.replay();
        }
        let preflight = preflight_async_call(cx).map_err(Arc::new)?;
        let join_pool = match &self.lifecycle {
            WorkerLifecycle::Running { .. }
            | WorkerLifecycle::Closing {
                join: JoinOwnership::Unscheduled(_),
            } => Some(current_join_pool(&preflight).map_err(Arc::new)?),
            WorkerLifecycle::Closing {
                join: JoinOwnership::InFlight(_),
            } => None,
            WorkerLifecycle::Terminal(memo) => return memo.replay(),
        };
        // Acquire the lifecycle-specific join capability before changing a
        // running connection into Closing. A zero-blocking runtime therefore
        // leaves the connection fully usable.
        self.begin_close(Command::Close);
        if let Some(pool) = &join_pool {
            self.ensure_join_scheduled(pool).map_err(Arc::new)?;
        }

        let outcome = match &mut self.lifecycle {
            WorkerLifecycle::Closing {
                join: JoinOwnership::InFlight(flight),
            } => wait_for_async_value(&preflight, &mut flight.result_rx).await,
            WorkerLifecycle::Terminal(memo) => return memo.replay(),
            WorkerLifecycle::Running { .. }
            | WorkerLifecycle::Closing {
                join: JoinOwnership::Unscheduled(_),
            } => {
                return Err(Arc::new(FrankenError::Internal(
                    "async worker close reached an invalid join state".to_owned(),
                )));
            }
        };

        match outcome {
            AsyncReceive::Completed(outcome) => self.finish_close(outcome.into_result()),
            AsyncReceive::Cancelled => Err(Arc::new(FrankenError::Interrupt)),
            AsyncReceive::Closed => {
                let recovered = match &self.lifecycle {
                    WorkerLifecycle::Closing {
                        join: JoinOwnership::InFlight(flight),
                    } => flight.recover_unclaimed_worker(),
                    _ => None,
                };
                if let Some(worker) = recovered {
                    self.lifecycle = WorkerLifecycle::Closing {
                        join: JoinOwnership::Unscheduled(worker),
                    };
                    Err(Arc::new(worker_join_admission_err()))
                } else {
                    self.finish_close(Err(worker_join_task_err()))
                }
            }
        }
    }

    /// Explicitly close a synchronously used connection and join its worker.
    ///
    /// A terminal failure is memoized and replayed as the same
    /// `Arc<FrankenError>` on every later close attempt.
    pub fn close_sync(&mut self) -> Result<(), Arc<FrankenError>> {
        if let WorkerLifecycle::Terminal(memo) = &self.lifecycle {
            return memo.replay();
        }
        self.begin_close(Command::Close);
        let lifecycle = std::mem::replace(
            &mut self.lifecycle,
            WorkerLifecycle::Terminal(CloseMemo::Success),
        );
        let result = match lifecycle {
            WorkerLifecycle::Closing {
                join: JoinOwnership::Unscheduled(worker),
            } => worker.wait(),
            WorkerLifecycle::Closing {
                join: JoinOwnership::InFlight(mut flight),
            } => {
                let cleanup_cx = NativeCx::<native_cap::None>::detached_cancel_context();
                match future::block_on(flight.result_rx.recv(&cleanup_cx)) {
                    Ok(outcome) => outcome.into_result(),
                    Err(oneshot::RecvError::Cancelled) => Err(FrankenError::Internal(
                        "detached synchronous close context was unexpectedly cancelled".to_owned(),
                    )),
                    Err(oneshot::RecvError::Closed | oneshot::RecvError::PolledAfterCompletion) => {
                        flight
                            .recover_unclaimed_worker()
                            .map_or_else(|| Err(worker_join_task_err()), WorkerHandle::wait)
                    }
                }
            }
            WorkerLifecycle::Terminal(memo) => {
                self.lifecycle = WorkerLifecycle::Terminal(memo.clone());
                return memo.replay();
            }
            WorkerLifecycle::Running { .. } => Err(FrankenError::Internal(
                "synchronous close failed to enter the closing state".to_owned(),
            )),
        };
        self.finish_close(result)
    }
}

impl Drop for AsyncConnection {
    fn drop(&mut self) {
        let lifecycle = std::mem::replace(
            &mut self.lifecycle,
            WorkerLifecycle::Terminal(CloseMemo::Success),
        );
        match lifecycle {
            WorkerLifecycle::Running { tx, worker } => {
                let _ = tx.try_send(Command::Shutdown);
                drop(tx);
                // Dropping a JoinHandle detaches. The worker still owns the
                // Connection and runs its one terminal cleanup path; Drop must
                // never park an arbitrary caller or runtime thread.
                drop(worker);
            }
            WorkerLifecycle::Closing {
                join: JoinOwnership::Unscheduled(worker),
            } => drop(worker),
            WorkerLifecycle::Closing {
                join: JoinOwnership::InFlight(flight),
            } => drop(flight),
            WorkerLifecycle::Terminal(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::cx::Cx;

    fn test_runtime() -> Runtime {
        RuntimeBuilder::current_thread()
            .blocking_threads(2, 2)
            .build()
            .expect("test runtime should build")
    }

    fn zero_blocking_runtime() -> Runtime {
        RuntimeBuilder::current_thread()
            .blocking_threads(0, 0)
            .build()
            .expect("zero-blocking-thread test runtime should build")
    }

    fn terminal_test_connection() -> AsyncConnection {
        AsyncConnection {
            lifecycle: WorkerLifecycle::Terminal(CloseMemo::Success),
            state: Arc::new(WorkerState::new()),
            sync_stream_active: AtomicBool::new(false),
        }
    }

    fn stall_worker(conn: &AsyncConnection) -> mpsc::SyncSender<()> {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        conn.sender()
            .expect("worker sender")
            .send(Command::BlockForTest {
                entered_tx,
                release_rx,
            })
            .expect("blocking test command should be admitted");
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker should enter deterministic gate");
        release_tx
    }

    fn fill_worker_mailbox(conn: &AsyncConnection) {
        for _ in 0..COMMAND_MAILBOX_CAPACITY {
            let (response_tx, response_rx) = mpsc::sync_channel(1);
            drop(response_rx);
            conn.sender()
                .expect("worker sender")
                .try_send(Command::LastInsertRowid {
                    tx: Responder::Sync(response_tx),
                })
                .expect("mailbox fill command should fit");
        }
    }

    struct OpenResponseGate {
        state: Arc<WorkerState>,
    }

    impl OpenResponseGate {
        fn release(&self) {
            self.state
                .hold_before_open_response
                .store(false, Ordering::Release);
        }
    }

    impl Drop for OpenResponseGate {
        fn drop(&mut self) {
            self.release();
        }
    }

    fn spawn_stalled_pending_open() -> (
        Arc<WorkerState>,
        OpenResponseGate,
        PendingOpen,
        oneshot::Receiver<Result<OpenHandshake, FrankenError>>,
    ) {
        let state = Arc::new(WorkerState::new());
        state
            .hold_before_open_response
            .store(true, Ordering::Release);
        let gate = OpenResponseGate {
            state: Arc::clone(&state),
        };
        let (open_tx, open_rx) = async_response_channel();
        let (cmd_tx, cmd_rx) = command_channel(COMMAND_MAILBOX_CAPACITY);
        let worker = spawn_worker_thread(
            ":memory:".to_owned(),
            ConnectionEnv::default(),
            cmd_rx,
            open_tx,
            Arc::clone(&state),
        )
        .expect("test worker should spawn");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !state.open_response_waiting.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < deadline,
                "worker did not reach the open-response publication gate"
            );
            thread::yield_now();
        }
        (state, gate, PendingOpen::new(cmd_tx, worker), open_rx)
    }

    fn wait_for_worker_terminal(state: &WorkerState) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while state.phase() != WorkerPhase::Terminal {
            assert!(
                std::time::Instant::now() < deadline,
                "worker did not reach its terminal phase"
            );
            thread::yield_now();
        }
    }

    fn unobserved_worker_errors(state: &WorkerState) -> Vec<String> {
        state
            .unobserved_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn wait_for_unobserved_worker_errors(state: &WorkerState, count: usize) -> Vec<String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let errors = unobserved_worker_errors(state);
            if errors.len() >= count {
                return errors;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker terminal error was not reported"
            );
            thread::yield_now();
        }
    }

    fn assert_stream_reentrancy<T>(result: Result<T, FrankenError>) {
        let Err(error) = result else {
            panic!("same-connection stream reentrancy unexpectedly succeeded");
        };
        assert!(
            matches!(&error, FrankenError::SynchronousStreamReentrancy),
            "same-connection stream reentrancy must fail with SQLITE_MISUSE"
        );
        assert_eq!(error.error_code(), fsqlite_error::ErrorCode::Misuse);
    }

    #[test]
    fn cancelled_open_returns_before_worker_release_and_cleans_once() {
        let runtime = test_runtime();
        let cx = Cx::new();
        let (state, gate, pending, open_rx) = spawn_stalled_pending_open();
        let preflight = runtime
            .block_on(async { preflight_async_call(&cx).expect("open preflight should succeed") });
        cx.cancel_with_reason(fsqlite_types::cx::CancelReason::UserInterrupt);

        let result = runtime.block_on(AsyncConnection::finish_pending_open(
            preflight,
            pending,
            open_rx,
            Arc::clone(&state),
        ));
        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "cancelled open should return Interrupt promptly"
        );
        assert!(
            state.hold_before_open_response.load(Ordering::Acquire),
            "open cancellation waited for the worker publication gate"
        );
        assert_eq!(
            state.cleanup_calls.load(Ordering::Acquire),
            0,
            "cleanup cannot run while the worker remains gated"
        );

        gate.release();
        wait_for_worker_terminal(&state);
        assert_eq!(
            state.cleanup_calls.load(Ordering::Acquire),
            1,
            "detached worker must clean its Connection exactly once"
        );
        assert!(
            unobserved_worker_errors(&state).is_empty(),
            "a clean cancelled open must not report a terminal error"
        );
    }

    #[test]
    fn dropped_open_future_is_nonblocking_and_cleans_once() {
        let runtime = test_runtime();
        let cx = Cx::new();
        let (state, gate, pending, open_rx) = spawn_stalled_pending_open();
        let preflight = runtime
            .block_on(async { preflight_async_call(&cx).expect("open preflight should succeed") });

        runtime.block_on(async {
            let mut open = Box::pin(AsyncConnection::finish_pending_open(
                preflight,
                pending,
                open_rx,
                Arc::clone(&state),
            ));
            assert!(
                future::poll_once(&mut open).await.is_none(),
                "gated worker must keep the open future pending"
            );
            drop(open);
        });
        assert!(
            state.hold_before_open_response.load(Ordering::Acquire),
            "dropping an open future waited for the worker publication gate"
        );
        assert_eq!(
            state.cleanup_calls.load(Ordering::Acquire),
            0,
            "cleanup cannot run while the worker remains gated"
        );

        gate.release();
        wait_for_worker_terminal(&state);
        assert_eq!(
            state.cleanup_calls.load(Ordering::Acquire),
            1,
            "dropped open future must leave exactly one worker cleanup"
        );
        assert!(
            unobserved_worker_errors(&state).is_empty(),
            "clean dropped open must not report a terminal error"
        );
    }

    #[test]
    fn cancelled_open_cleanup_error_is_reported_exactly_once() {
        let runtime = test_runtime();
        let cx = Cx::new();
        let (state, gate, pending, open_rx) = spawn_stalled_pending_open();
        state.panic_on_cleanup.store(true, Ordering::Release);
        let preflight = runtime
            .block_on(async { preflight_async_call(&cx).expect("open preflight should succeed") });
        cx.cancel_with_reason(fsqlite_types::cx::CancelReason::UserInterrupt);

        let result = runtime.block_on(AsyncConnection::finish_pending_open(
            preflight,
            pending,
            open_rx,
            Arc::clone(&state),
        ));
        assert!(matches!(result, Err(FrankenError::Interrupt)));
        gate.release();
        wait_for_worker_terminal(&state);
        let errors = wait_for_unobserved_worker_errors(&state, 1);
        assert_eq!(errors.len(), 1, "terminal failure must be reported once");
        assert!(
            errors[0].contains("async worker cleanup panic sentinel"),
            "unexpected terminal diagnostic: {}",
            errors[0]
        );
        assert_eq!(
            state.cleanup_calls.load(Ordering::Acquire),
            1,
            "failing cleanup still runs exactly once"
        );
    }

    #[test]
    fn committed_but_unconsumed_open_error_is_reported_exactly_once() {
        let state = Arc::new(WorkerState::new());
        state.publish_phase(WorkerPhase::Terminal);
        let error = FrankenError::Internal("committed open error sentinel".to_owned());
        let diagnostic = Arc::new(OpenErrorDiagnostic::new(&error, Arc::clone(&state)));
        let outcome =
            WorkerTerminalOutcome::new(Ok(()), Some(Arc::clone(&diagnostic)), Arc::clone(&state));
        let (tx, rx) = oneshot::channel();
        assert!(
            tx.send_blocking(Ok::<OpenHandshake, FrankenError>(OpenHandshake::Failed {
                error,
                diagnostic
            }))
            .is_ok(),
            "open error should commit before the receiver is abandoned"
        );

        drop(rx);
        drop(outcome);
        let errors = wait_for_unobserved_worker_errors(&state, 1);
        assert_eq!(errors.len(), 1, "abandoned open error must report once");
        assert!(
            errors[0].contains("committed open error sentinel"),
            "unexpected open diagnostic: {}",
            errors[0]
        );
    }

    #[test]
    fn dropped_join_flight_reports_terminal_error_exactly_once() {
        let runtime = test_runtime();
        let pool = runtime.block_on(async {
            Runtime::current_handle()
                .expect("test runtime handle")
                .blocking_handle()
                .expect("test blocking pool")
        });
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        let state = Arc::clone(&conn.state);
        state.panic_on_cleanup.store(true, Ordering::Release);
        conn.begin_close(Command::Close);
        conn.ensure_join_scheduled(&pool)
            .expect("join observation should be admitted");
        drop(conn);

        let _ = release_tx.send(());
        wait_for_worker_terminal(&state);
        let errors = wait_for_unobserved_worker_errors(&state, 1);
        assert_eq!(errors.len(), 1, "abandoned join failure must report once");
        assert!(
            errors[0].contains("async worker cleanup panic sentinel"),
            "unexpected join diagnostic: {}",
            errors[0]
        );
        assert_eq!(
            state.cleanup_calls.load(Ordering::Acquire),
            1,
            "abandoned join still owns exactly one cleanup"
        );
    }

    #[test]
    fn synchronous_stream_reentrancy_fails_fast_and_other_connection_remains_usable() {
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let test_thread = thread::spawn(move || {
            let runtime = test_runtime();
            let mut primary =
                AsyncConnection::open_sync(":memory:").expect("primary worker should open");
            primary
                .execute_batch_sync(
                    "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1), (2);",
                )
                .expect("primary fixture should initialize");
            let mut other =
                AsyncConnection::open_sync(":memory:").expect("second worker should open");
            let mut callbacks = 0usize;

            primary
                .query_with_params_for_each_sync("SELECT id FROM t ORDER BY id", &[], |row| {
                    callbacks += 1;
                    assert!(row.get(0).is_some());
                    if callbacks != 1 {
                        return Ok(());
                    }

                    assert_stream_reentrancy(primary.prepare_sync("SELECT 1"));
                    assert_stream_reentrancy(primary.query_sync("SELECT 1"));
                    assert_stream_reentrancy(
                        primary.query_with_params_sync("SELECT ?1", &[SqliteValue::Integer(1)]),
                    );
                    assert_stream_reentrancy(primary.query_with_params_for_each_sync(
                        "SELECT 1",
                        &[],
                        |_| Ok(()),
                    ));
                    assert_stream_reentrancy(primary.query_row_sync("SELECT 1"));
                    assert_stream_reentrancy(
                        primary.query_row_with_params_sync("SELECT ?1", &[SqliteValue::Integer(1)]),
                    );
                    assert_stream_reentrancy(primary.execute_sync("INSERT INTO t VALUES (99)"));
                    assert_stream_reentrancy(primary.execute_with_params_sync(
                        "INSERT INTO t VALUES (?1)",
                        &[SqliteValue::Integer(99)],
                    ));
                    assert_stream_reentrancy(primary.execute_many_with_params_in_transaction_sync(
                        "INSERT INTO t VALUES (?1)",
                        &[vec![SqliteValue::Integer(99)]],
                    ));
                    assert_stream_reentrancy(primary.execute_batch_sync("SELECT 1;"));
                    assert_stream_reentrancy(primary.begin_transaction_sync());
                    assert_stream_reentrancy(primary.commit_transaction_sync());
                    assert_stream_reentrancy(primary.rollback_transaction_sync());
                    assert_stream_reentrancy(primary.last_insert_rowid_sync());

                    let cx = Cx::new();
                    assert_stream_reentrancy(runtime.block_on(primary.query(&cx, "SELECT 1")));
                    assert_stream_reentrancy(
                        runtime.block_on(primary.execute(&cx, "INSERT INTO t VALUES (99)")),
                    );
                    assert!(
                        !primary.in_transaction(),
                        "local state reads remain safe during a callback"
                    );
                    assert_eq!(
                        other
                            .query_sync("SELECT 1")
                            .expect("a different connection must remain usable")
                            .len(),
                        1
                    );
                    Ok(())
                })
                .expect("primary stream should finish");

            assert_eq!(callbacks, 2, "both primary rows must be delivered");
            let rows = primary
                .query_sync("SELECT id FROM t ORDER BY id")
                .expect("primary connection must be reusable after streaming");
            assert_eq!(rows.len(), 2, "rejected writes must have no effect");
            primary.close_sync().expect("primary close should succeed");
            other.close_sync().expect("second close should succeed");
            let _ = done_tx.send(());
        });

        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("same-connection callback reentrancy deadlocked");
        test_thread
            .join()
            .expect("reentrancy test thread should not panic");
    }

    #[test]
    fn synchronous_stream_callback_observes_current_worker_transaction_state() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        assert!(!conn.in_transaction());

        let mut begin_rows = 0usize;
        conn.query_with_params_for_each_sync("BEGIN; SELECT 1", &[], |_| {
            begin_rows += 1;
            assert!(
                conn.in_transaction(),
                "the BEGIN state must publish before its following row"
            );
            Ok(())
        })
        .expect("BEGIN followed by a row should stream successfully");
        assert_eq!(begin_rows, 1);
        assert!(conn.in_transaction());

        let mut commit_rows = 0usize;
        conn.query_with_params_for_each_sync("COMMIT; SELECT 1", &[], |_| {
            commit_rows += 1;
            assert!(
                !conn.in_transaction(),
                "the COMMIT state must publish before its following row"
            );
            Ok(())
        })
        .expect("COMMIT followed by a row should stream successfully");
        assert_eq!(commit_rows, 1);
        assert!(!conn.in_transaction());

        conn.close_sync().expect("worker should close");
    }

    #[test]
    fn synchronous_stream_guard_clears_after_callback_error_and_panic() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        conn.execute_batch_sync(
            "CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1), (2);",
        )
        .expect("fixture should initialize");

        let error = conn
            .query_with_params_for_each_sync("SELECT id FROM t ORDER BY id", &[], |_| {
                Err(FrankenError::Abort)
            })
            .expect_err("callback error should stop streaming");
        assert!(matches!(error, FrankenError::Abort));
        assert_eq!(
            conn.query_sync("SELECT 1")
                .expect("guard must clear after callback error")
                .len(),
            1
        );

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = conn.query_with_params_for_each_sync(
                "SELECT id FROM t ORDER BY id",
                &[],
                |_| -> Result<(), FrankenError> {
                    panic!("synchronous stream callback panic sentinel");
                },
            );
        }));
        assert!(
            panic.is_err(),
            "callback panic should propagate to its caller"
        );
        assert_eq!(
            conn.query_sync("SELECT 1")
                .expect("guard must clear while unwinding a callback panic")
                .len(),
            1
        );
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn receiver_drop_panic_cannot_skip_connection_cleanup() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let state = Arc::clone(&conn.state);
        conn.running_sender()
            .expect("worker sender")
            .signal
            .panic_on_receiver_drop
            .store(true, Ordering::Release);

        let error = conn
            .close_sync()
            .expect_err("receiver-drop panic should surface through close");
        assert!(
            error
                .to_string()
                .contains("async command receiver drop panic sentinel"),
            "unexpected receiver-drop diagnostic: {error}"
        );
        assert_eq!(
            state.cleanup_calls.load(Ordering::Acquire),
            1,
            "receiver-drop panic must not skip connection cleanup"
        );
        assert_eq!(state.phase(), WorkerPhase::Terminal);
    }

    #[test]
    fn test_async_connection_basic() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");

            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
                .await
                .expect("create table should succeed");

            conn.execute_with_params(
                &cx,
                "INSERT INTO t VALUES (?1, ?2)",
                &[SqliteValue::Integer(1), SqliteValue::Text("hello".into())],
            )
            .await
            .expect("insert should succeed");

            let rows = conn
                .query(&cx, "SELECT * FROM t")
                .await
                .expect("query should succeed");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
            assert_eq!(rows[0].get(1), Some(&SqliteValue::Text("hello".into())));

            let row = conn
                .query_row(&cx, "SELECT name FROM t WHERE id = 1")
                .await
                .expect("query_row should succeed");
            assert_eq!(row.get(0), Some(&SqliteValue::Text("hello".into())));

            let count = conn
                .execute(&cx, "DELETE FROM t")
                .await
                .expect("delete should succeed");
            assert_eq!(count, 1);
        });
    }

    #[test]
    fn test_async_connection_transaction() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");

            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("create should succeed");

            // Begin, insert, rollback — row should not persist.
            conn.begin_transaction(&cx).await.expect("begin");
            conn.execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert");
            conn.rollback_transaction(&cx).await.expect("rollback");

            let rows = conn.query(&cx, "SELECT * FROM t").await.expect("query");
            assert!(rows.is_empty(), "rollback should have removed the row");

            // Begin, insert, commit — row should persist.
            conn.begin_transaction(&cx).await.expect("begin");
            conn.execute(&cx, "INSERT INTO t VALUES (2)")
                .await
                .expect("insert");
            conn.commit_transaction(&cx).await.expect("commit");

            let rows = conn.query(&cx, "SELECT * FROM t").await.expect("query");
            assert_eq!(rows.len(), 1);
        });
    }

    #[test]
    fn ordinary_async_response_does_not_use_the_blocking_pool() {
        let runtime = RuntimeBuilder::current_thread()
            .blocking_threads(1, 1)
            .build()
            .expect("single-blocking-thread runtime should build");
        let pool = runtime.block_on(async {
            Runtime::current_handle()
                .expect("test runtime handle")
                .blocking_handle()
                .expect("test blocking pool")
        });
        let (blocker_entered_tx, blocker_entered_rx) = mpsc::sync_channel(1);
        let (blocker_release_tx, blocker_release_rx) = mpsc::sync_channel(1);
        let blocker = pool.spawn(move || {
            let _ = blocker_entered_tx.send(());
            let _ = blocker_release_rx.recv();
        });
        blocker_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("blocking-pool sentinel should start");

        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let blocker_release_from_watchdog = blocker_release_tx.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                let _ = blocker_release_from_watchdog.send(());
            }
        });

        let rows = runtime
            .block_on(conn.query(&Cx::new(), "SELECT 1"))
            .expect("direct worker oneshot response should not need the blocking pool");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            pool.pending_count(),
            0,
            "ordinary async response must not enqueue a blocking bridge job"
        );
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "query waited for the occupied blocking pool"
        );
        let _ = watchdog_cancel_tx.send(());
        let _ = blocker_release_tx.send(());
        blocker.wait();
        watchdog.join().expect("watchdog should not panic");
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn async_open_handshake_does_not_use_the_blocking_pool() {
        let runtime = RuntimeBuilder::current_thread()
            .blocking_threads(1, 1)
            .build()
            .expect("single-blocking-thread runtime should build");
        let pool = runtime.block_on(async {
            Runtime::current_handle()
                .expect("test runtime handle")
                .blocking_handle()
                .expect("test blocking pool")
        });
        let (blocker_entered_tx, blocker_entered_rx) = mpsc::sync_channel(1);
        let (blocker_release_tx, blocker_release_rx) = mpsc::sync_channel(1);
        let blocker = pool.spawn(move || {
            let _ = blocker_entered_tx.send(());
            let _ = blocker_release_rx.recv();
        });
        blocker_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("blocking-pool sentinel should start");

        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let blocker_release_from_watchdog = blocker_release_tx.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                let _ = blocker_release_from_watchdog.send(());
            }
        });

        let mut conn = runtime
            .block_on(AsyncConnection::open(&Cx::new(), ":memory:"))
            .expect("open handshake should not need the blocking pool");
        assert_eq!(
            pool.pending_count(),
            0,
            "successful async open must not enqueue a blocking response bridge"
        );
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "async open waited for the occupied blocking pool"
        );
        let _ = watchdog_cancel_tx.send(());
        let _ = blocker_release_tx.send(());
        blocker.wait();
        watchdog.join().expect("watchdog should not panic");
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn ordinary_async_calls_work_without_a_blocking_pool() {
        let runtime = zero_blocking_runtime();
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");

        runtime.block_on(async {
            let cx = Cx::new();
            conn.prepare(&cx, "SELECT 1")
                .await
                .expect("prepare should use only native async transport");
            conn.execute(&cx, "CREATE TABLE t(id INTEGER PRIMARY KEY, value INTEGER)")
                .await
                .expect("DDL should use only native async transport");
            conn.execute_with_params(
                &cx,
                "INSERT INTO t VALUES (?1, ?2)",
                &[SqliteValue::Integer(1), SqliteValue::Integer(7)],
            )
            .await
            .expect("parameterized DML should use only native async transport");
            let rows = conn
                .query(&cx, "SELECT value FROM t WHERE id = 1")
                .await
                .expect("query should use only native async transport");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(7)));
        });

        conn.close_sync()
            .expect("sync close should join the worker");
    }

    #[test]
    fn async_open_succeeds_without_a_blocking_pool() {
        let runtime = zero_blocking_runtime();
        let mut conn = runtime.block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open handshake should not require blocking threads");
            let rows = conn
                .query(&cx, "SELECT 1")
                .await
                .expect("opened connection should be usable");
            assert_eq!(rows.len(), 1);
            conn
        });

        conn.close_sync()
            .expect("sync close should join the worker");
    }

    #[test]
    fn failed_open_without_a_pool_returns_the_primary_error() {
        let state = Arc::new(WorkerState::new());
        *state
            .forced_open_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(FrankenError::BusySnapshot {
                conflicting_pages: "7, 11".to_owned(),
            });
        let (open_tx, open_rx) = async_response_channel();
        let (cmd_tx, cmd_rx) = command_channel(COMMAND_MAILBOX_CAPACITY);
        let worker = spawn_worker_thread(
            ":memory:".to_owned(),
            ConnectionEnv::default(),
            cmd_rx,
            open_tx,
            Arc::clone(&state),
        )
        .expect("worker should spawn");
        let pending = PendingOpen::new(cmd_tx, worker);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !state.open_response_committed.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < deadline,
                "forced worker did not commit its open failure"
            );
            thread::yield_now();
        }

        let runtime = zero_blocking_runtime();
        let result = runtime.block_on(async {
            let preflight =
                preflight_async_call(&Cx::new()).expect("zero-pool preflight should succeed");
            let mut finish = Box::pin(AsyncConnection::finish_pending_open(
                preflight,
                pending,
                open_rx,
                Arc::clone(&state),
            ));
            future::poll_once(&mut finish)
                .await
                .expect("a committed open error must become terminal in one poll")
        });
        let error = match result {
            Ok(_) => panic!("the forced primary error must be returned"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            FrankenError::BusySnapshot { conflicting_pages }
                if conflicting_pages == "7, 11"
        ));

        wait_for_worker_terminal(&state);
        assert_eq!(
            state.cleanup_calls.load(Ordering::Acquire),
            0,
            "a failed open has no Connection instance to clean"
        );
        assert!(
            unobserved_worker_errors(&state).is_empty(),
            "the observed engine open error must not be reported again"
        );
    }

    #[test]
    fn async_close_without_a_pool_leaves_a_running_connection_usable() {
        let runtime = zero_blocking_runtime();
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        conn.execute_sync("CREATE TABLE t(id INTEGER PRIMARY KEY)")
            .expect("fixture should initialize");

        let error = runtime
            .block_on(conn.close(&Cx::new()))
            .expect_err("async close needs a join-capable blocking pool");
        assert!(
            error.to_string().contains("blocking pool for worker join"),
            "unexpected zero-pool close diagnostic: {error}"
        );
        assert_eq!(
            conn.query_sync("SELECT 1")
                .expect("failed preflight must leave the connection usable")
                .len(),
            1
        );
        conn.close_sync()
            .expect("synchronous close should still own and join the worker");
    }

    #[test]
    fn inflight_async_close_can_resume_without_a_blocking_pool() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        let join_runtime = test_runtime();
        let first_cx = Cx::new();
        let first_result = join_runtime.block_on(async {
            let mut close = Box::pin(conn.close(&first_cx));
            assert!(
                future::poll_once(&mut close).await.is_none(),
                "stalled close should retain its in-flight join"
            );
            first_cx.cancel();
            close.await
        });
        assert!(
            matches!(
                &first_result,
                Err(error) if matches!(error.as_ref(), FrankenError::Interrupt)
            ),
            "first close wait should be cancelled"
        );
        assert!(matches!(
            &conn.lifecycle,
            WorkerLifecycle::Closing {
                join: JoinOwnership::InFlight(_)
            }
        ));

        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let release_from_watchdog = release_tx.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                let _ = release_from_watchdog.send(());
            }
        });

        zero_blocking_runtime().block_on(async {
            let cx = Cx::new();
            let close = conn.close(&cx);
            let release = async move {
                let _ = release_tx.send(());
                let _ = watchdog_cancel_tx.send(());
            };
            let (result, ()) = future::zip(close, release).await;
            result.expect("an existing join flight must not reacquire a blocking pool");
        });
        watchdog.join().expect("watchdog should not panic");
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "resumed close blocked the zero-pool current-thread runtime"
        );
    }

    #[test]
    fn async_response_preserves_engine_interrupt_inside_transport_envelope() {
        let runtime = test_runtime();
        runtime.block_on(async {
            let cx = Cx::new();
            let preflight = preflight_async_call(&cx).expect("preflight should succeed");
            let (tx, mut rx) = async_response_channel::<()>();
            tx.respond(Err(FrankenError::Interrupt));
            let result = recv_async_response(&preflight, &mut rx).await;
            assert!(
                matches!(result, Ok(Err(FrankenError::Interrupt))),
                "engine Interrupt must remain inside the response envelope: {result:?}"
            );
        });
    }

    #[test]
    fn committed_async_response_wins_late_masked_cancellation_tie() {
        let runtime = test_runtime();
        runtime.block_on(async {
            let cx = Cx::new();
            let native = NativeCx::for_testing();
            cx.set_native_cx(native.clone());
            let preflight = preflight_async_call(&cx).expect("preflight should succeed");
            let (tx, mut rx) = async_response_channel::<usize>();
            tx.respond(Ok(41));
            let _late_mask = cx.masked();
            cx.cancel();
            native.set_cancel_reason(asupersync::types::CancelReason::user(
                "response completion tie",
            ));

            let result = recv_async_response(&preflight, &mut rx).await;
            assert!(
                matches!(result, Ok(Ok(41))),
                "committed response must win the cancellation tie: {result:?}"
            );
        });
    }

    #[test]
    fn cancellation_wins_a_response_channel_closure_tie() {
        let runtime = test_runtime();
        runtime.block_on(async {
            let cx = Cx::new();
            let preflight = preflight_async_call(&cx).expect("preflight should succeed");
            let (tx, mut rx) = async_response_channel::<usize>();
            drop(tx);
            cx.cancel();

            assert!(matches!(
                wait_for_async_value(&preflight, &mut rx).await,
                AsyncReceive::Cancelled
            ));
        });
    }

    #[test]
    fn cancelled_admitted_call_does_not_pin_blocking_pool_thread() {
        let runtime = RuntimeBuilder::current_thread()
            .blocking_threads(1, 1)
            .build()
            .expect("single-blocking-thread runtime should build");
        let pool = runtime.block_on(async {
            Runtime::current_handle()
                .expect("test runtime handle")
                .blocking_handle()
                .expect("test blocking pool")
        });
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        let signal = Arc::clone(&conn.sender().expect("worker sender").signal);
        let cx = Cx::new();
        let cx_for_cancel = cx.clone();

        let result = runtime.block_on(async {
            let execute = conn.execute(
                &cx,
                "CREATE TABLE response_wait_cancelled (id INTEGER PRIMARY KEY)",
            );
            let cancel = async {
                while signal.async_publications.load(Ordering::Acquire) == 0 {
                    future::yield_now().await;
                }
                cx_for_cancel.cancel();
            };
            let (result, ()) = future::zip(execute, cancel).await;
            result
        });
        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "response cancellation should abandon the wait: {result:?}"
        );

        let (sentinel_tx, sentinel_rx) = mpsc::sync_channel(1);
        let sentinel = pool.spawn(move || {
            let _ = sentinel_tx.send(());
        });
        if sentinel_rx.recv_timeout(Duration::from_secs(2)).is_err() {
            let _ = release_tx.send(());
            sentinel.wait();
            panic!("cancelled admitted call pinned the only blocking-pool thread");
        }
        sentinel.wait();
        assert_eq!(
            pool.pending_count(),
            0,
            "direct response cancellation must leave no bridge task queued"
        );

        let _ = release_tx.send(());
        conn.last_insert_rowid_sync()
            .expect("FIFO barrier should observe the admitted effect");
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn transaction_state_is_worker_published_when_response_is_abandoned() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");

        let (begin_tx, begin_rx) = mpsc::sync_channel(1);
        drop(begin_rx);
        conn.sender()
            .expect("worker sender")
            .send(Command::BeginTransaction {
                tx: Responder::Sync(begin_tx),
            })
            .expect("begin command should be admitted");
        conn.last_insert_rowid_sync()
            .expect("FIFO barrier after abandoned begin response");
        assert!(
            conn.in_transaction(),
            "worker state must publish before the abandoned response"
        );

        let (rollback_tx, rollback_rx) = mpsc::sync_channel(1);
        drop(rollback_rx);
        conn.sender()
            .expect("worker sender")
            .send(Command::RollbackTransaction {
                tx: Responder::Sync(rollback_tx),
            })
            .expect("rollback command should be admitted");
        conn.last_insert_rowid_sync()
            .expect("FIFO barrier after abandoned rollback response");
        assert!(
            !conn.in_transaction(),
            "worker state must remain correct when rollback response is abandoned"
        );

        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn textual_transaction_control_updates_worker_published_state() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");

        conn.execute_sync("BEGIN")
            .expect("textual BEGIN should succeed");
        assert!(conn.in_transaction());

        conn.execute_batch_sync("ROLLBACK; BEGIN; COMMIT;")
            .expect("textual transaction batch should succeed");
        assert!(
            !conn.in_transaction(),
            "final engine state after a transaction batch must be published"
        );

        conn.close_sync().expect("close should succeed");
    }

    fn assert_terminal_cleanup_once(state: &WorkerState) {
        assert_eq!(state.phase(), WorkerPhase::Terminal);
        assert_eq!(
            state.cleanup_calls.load(Ordering::Acquire),
            1,
            "each successfully opened worker must attempt connection cleanup exactly once"
        );
    }

    fn take_running_worker(conn: &mut AsyncConnection) -> (CommandSender, WorkerHandle) {
        match std::mem::replace(
            &mut conn.lifecycle,
            WorkerLifecycle::Terminal(CloseMemo::Success),
        ) {
            WorkerLifecycle::Running { tx, worker } => (tx, worker),
            _ => panic!("test expected a running AsyncConnection"),
        }
    }

    #[test]
    fn cleanup_runs_once_for_every_worker_exit() {
        {
            let mut conn =
                AsyncConnection::open_sync(":memory:").expect("explicit-close worker should open");
            let state = Arc::clone(&conn.state);
            conn.close_sync().expect("explicit close should succeed");
            assert_terminal_cleanup_once(&state);
        }

        {
            let mut conn =
                AsyncConnection::open_sync(":memory:").expect("shutdown worker should open");
            let state = Arc::clone(&conn.state);
            let (sender, worker) = take_running_worker(&mut conn);
            sender
                .try_send(Command::Shutdown)
                .expect("shutdown command should fit");
            drop(sender);
            worker.wait().expect("shutdown cleanup should succeed");
            assert_terminal_cleanup_once(&state);
        }

        {
            let mut conn =
                AsyncConnection::open_sync(":memory:").expect("disconnect worker should open");
            let state = Arc::clone(&conn.state);
            let (sender, worker) = take_running_worker(&mut conn);
            drop(sender);
            worker.wait().expect("disconnect cleanup should succeed");
            assert_terminal_cleanup_once(&state);
        }

        {
            let mut conn =
                AsyncConnection::open_sync(":memory:").expect("panic worker should open");
            let state = Arc::clone(&conn.state);
            conn.sender()
                .expect("panic sender")
                .send(Command::PanicForTest)
                .expect("panic command should be admitted");
            let error = conn
                .close_sync()
                .expect_err("worker panic must be reported by close");
            assert!(
                error
                    .to_string()
                    .contains("async worker command panic sentinel"),
                "unexpected worker panic diagnostic: {error}"
            );
            assert_terminal_cleanup_once(&state);
        }
    }

    #[test]
    fn cleanup_panic_is_reported_and_still_publishes_terminal_state() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let state = Arc::clone(&conn.state);
        state.panic_on_cleanup.store(true, Ordering::Release);

        let error = conn
            .close_sync()
            .expect_err("cleanup panic must be reported");
        assert!(
            error
                .to_string()
                .contains("async worker cleanup panic sentinel"),
            "unexpected cleanup panic diagnostic: {error}"
        );
        assert_terminal_cleanup_once(&state);
    }

    #[test]
    fn failed_close_replays_the_exact_shared_busy_snapshot_error() {
        let mut conn = terminal_test_connection();
        let first = conn
            .finish_close(Err(FrankenError::BusySnapshot {
                conflicting_pages: "7, 11".to_owned(),
            }))
            .expect_err("injected terminal close should fail");
        let second = conn
            .close_sync()
            .expect_err("terminal close should replay its failure");

        assert!(
            Arc::ptr_eq(&first, &second),
            "terminal replay must return the same error allocation"
        );
        assert!(matches!(
            first.as_ref(),
            FrankenError::BusySnapshot { conflicting_pages }
                if conflicting_pages == "7, 11"
        ));
        assert_eq!(first.error_code(), fsqlite_error::ErrorCode::Busy);
        assert_eq!(first.extended_error_code(), 517);
        assert!(first.is_transient());
        assert_eq!(first.to_string(), second.to_string());
    }

    #[test]
    fn failed_close_preserves_io_error_identity_and_source() {
        let mut conn = terminal_test_connection();
        let first = conn
            .finish_close(Err(FrankenError::Io(std::io::Error::from_raw_os_error(28))))
            .expect_err("injected I/O close should fail");
        let second = conn
            .close_sync()
            .expect_err("terminal I/O close should replay its failure");

        assert!(Arc::ptr_eq(&first, &second));
        match (first.as_ref(), second.as_ref()) {
            (FrankenError::Io(left), FrankenError::Io(right)) => {
                assert!(
                    std::ptr::eq(left, right),
                    "replay must preserve the exact inner std::io::Error"
                );
                assert_eq!(left.kind(), right.kind());
                assert_eq!(left.raw_os_error(), Some(28));
            }
            other => panic!("expected two exact I/O variants, got {other:?}"),
        }
        assert!(
            std::error::Error::source(first.as_ref()).is_some(),
            "the original I/O source chain must remain available"
        );
    }

    #[test]
    fn terminal_close_error_identity_survives_sync_async_replay_order() {
        let mut async_replay = terminal_test_connection();
        let first = async_replay
            .finish_close(Err(FrankenError::BusyRecovery))
            .expect_err("injected terminal close should fail");
        let cancelled = Cx::new();
        cancelled.cancel();
        let second = future::block_on(async_replay.close(&cancelled))
            .expect_err("terminal result must win over later caller cancellation");
        assert!(Arc::ptr_eq(&first, &second));

        let mut sync_replay = terminal_test_connection();
        let first = sync_replay
            .finish_close(Err(FrankenError::Busy))
            .expect_err("injected terminal close should fail");
        let second = sync_replay
            .close_sync()
            .expect_err("synchronous replay should preserve identity");
        let third = future::block_on(sync_replay.close(&Cx::new()))
            .expect_err("async replay should preserve sync-published identity");
        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(&second, &third));
    }

    #[test]
    fn async_close_does_not_join_on_the_runtime_thread() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        conn.sender()
            .expect("worker sender")
            .send(Command::BlockForTest {
                entered_tx,
                release_rx,
            })
            .expect("blocking test command should be admitted");
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker should enter deterministic gate");

        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let release_from_watchdog = release_tx.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                let _ = release_from_watchdog.send(());
            }
        });

        RuntimeBuilder::current_thread()
            .blocking_threads(1, 1)
            .build()
            .expect("single-blocking-thread test runtime should build")
            .block_on(async {
                let cx = Cx::new();
                let close = conn.close(&cx);
                let release = async move {
                    let _ = release_tx.send(());
                    let _ = watchdog_cancel_tx.send(());
                };
                let (close_result, ()) = future::zip(close, release).await;
                close_result.expect("async close should succeed");
            });

        watchdog.join().expect("watchdog should not panic");
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "async close blocked the current-thread runtime before its sibling could run"
        );
    }

    #[test]
    fn drop_is_nonblocking_while_worker_is_stalled_and_cleanup_eventually_runs() {
        let conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        let state = Arc::clone(&conn.state);
        let (dropped_tx, dropped_rx) = mpsc::sync_channel(1);
        let dropper = thread::spawn(move || {
            drop(conn);
            let _ = dropped_tx.send(());
        });

        if dropped_rx.recv_timeout(Duration::from_secs(2)).is_err() {
            let _ = release_tx.send(());
            dropper.join().expect("dropper should not panic");
            panic!("AsyncConnection::drop blocked on the stalled worker");
        }
        let _ = release_tx.send(());
        dropper.join().expect("dropper should not panic");

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while state.phase() != WorkerPhase::Terminal {
            assert!(
                std::time::Instant::now() < deadline,
                "detached worker did not reach terminal cleanup after release"
            );
            thread::yield_now();
        }
        assert_terminal_cleanup_once(&state);
    }

    #[test]
    fn cancelled_close_retains_join_and_second_close_observes_cleanup_failure() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        let state = Arc::clone(&conn.state);
        state.panic_on_cleanup.store(true, Ordering::Release);
        let runtime = test_runtime();
        let first_cx = Cx::new();

        let first_result = runtime.block_on(async {
            let mut close = Box::pin(conn.close(&first_cx));
            assert!(
                future::poll_once(&mut close).await.is_none(),
                "stalled worker close should remain pending"
            );
            first_cx.cancel();
            close.await
        });
        assert!(
            matches!(
                &first_result,
                Err(error) if matches!(error.as_ref(), FrankenError::Interrupt)
            ),
            "cancelled close should return Interrupt: {first_result:?}"
        );
        assert!(
            matches!(
                &conn.lifecycle,
                WorkerLifecycle::Closing {
                    join: JoinOwnership::InFlight(_)
                }
            ),
            "cancelled close must retain the in-flight join obligation"
        );

        let _ = release_tx.send(());
        let second_cx = Cx::new();
        let error = runtime
            .block_on(conn.close(&second_cx))
            .expect_err("retry must observe cleanup failure");
        assert!(
            error
                .to_string()
                .contains("async worker cleanup panic sentinel"),
            "retry lost the worker's exact cleanup failure: {error}"
        );
        let replay = runtime
            .block_on(conn.close(&Cx::new()))
            .expect_err("third close should replay the terminal cleanup failure");
        assert!(
            Arc::ptr_eq(&error, &replay),
            "retry and terminal replay must share the exact failure"
        );
        assert_terminal_cleanup_once(&state);
    }

    #[test]
    fn worker_panic_result_survives_cancelled_close() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        let state = Arc::clone(&conn.state);
        conn.sender()
            .expect("worker sender")
            .send(Command::PanicForTest)
            .expect("panic sentinel should be admitted");
        let runtime = test_runtime();
        let first_cx = Cx::new();

        let first_result = runtime.block_on(async {
            let mut close = Box::pin(conn.close(&first_cx));
            assert!(
                future::poll_once(&mut close).await.is_none(),
                "blocked worker close should remain pending"
            );
            first_cx.cancel();
            close.await
        });
        assert!(
            matches!(
                &first_result,
                Err(error) if matches!(error.as_ref(), FrankenError::Interrupt)
            ),
            "first close wait should be cancelled: {first_result:?}"
        );

        let _ = release_tx.send(());
        let second_cx = Cx::new();
        let error = runtime
            .block_on(conn.close(&second_cx))
            .expect_err("retry must observe the worker panic");
        assert!(
            error
                .to_string()
                .contains("async worker command panic sentinel"),
            "retry lost the worker's command-loop panic: {error}"
        );
        assert_terminal_cleanup_once(&state);
    }

    #[test]
    fn dropping_close_future_retains_join_obligation() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        let state = Arc::clone(&conn.state);
        let runtime = test_runtime();

        runtime.block_on(async {
            let cx = Cx::new();
            let mut close = Box::pin(conn.close(&cx));
            assert!(
                future::poll_once(&mut close).await.is_none(),
                "stalled worker close should remain pending"
            );
            drop(close);
        });
        assert!(
            matches!(
                &conn.lifecycle,
                WorkerLifecycle::Closing {
                    join: JoinOwnership::InFlight(_)
                }
            ),
            "dropping the close future must not drop join ownership"
        );

        let _ = release_tx.send(());
        conn.close_sync()
            .expect("synchronous retry should finish the retained join");
        assert_terminal_cleanup_once(&state);
    }

    #[test]
    fn rejected_join_admission_recovers_worker_handle() {
        let runtime = test_runtime();
        let rejected_pool = runtime.block_on(async {
            Runtime::current_handle()
                .expect("test runtime handle")
                .blocking_handle()
                .expect("test blocking pool")
        });
        drop(runtime);
        assert!(
            rejected_pool.is_shutdown(),
            "dropping the runtime should close its blocking pool"
        );

        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let state = Arc::clone(&conn.state);
        conn.begin_close(Command::Close);
        let error = conn
            .ensure_join_scheduled(&rejected_pool)
            .expect_err("shut-down pool must reject the join");
        assert!(
            error.to_string().contains("rejected the async worker join"),
            "unexpected join-admission error: {error}"
        );
        assert!(
            matches!(
                &conn.lifecycle,
                WorkerLifecycle::Closing {
                    join: JoinOwnership::Unscheduled(_)
                }
            ),
            "rejected admission must restore the exact worker handle"
        );
        conn.close_sync()
            .expect("recovered worker handle should remain synchronously joinable");
        assert_terminal_cleanup_once(&state);
    }

    #[test]
    fn full_mailbox_close_drains_every_published_command_before_cleanup() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        let state = Arc::clone(&conn.state);
        let mut responses = Vec::with_capacity(COMMAND_MAILBOX_CAPACITY);
        for _ in 0..COMMAND_MAILBOX_CAPACITY {
            let (tx, rx) = sync_response_channel();
            conn.sender()
                .expect("worker sender")
                .try_send(Command::LastInsertRowid { tx })
                .expect("mailbox fill command should fit");
            responses.push(rx);
        }

        conn.begin_close(Command::Close);
        assert!(
            matches!(
                &conn.lifecycle,
                WorkerLifecycle::Closing {
                    join: JoinOwnership::Unscheduled(_)
                }
            ),
            "full-mailbox close must still retain join ownership"
        );
        let _ = release_tx.send(());
        conn.close_sync()
            .expect("sender disconnection should close after FIFO drain");

        for response in responses {
            response
                .recv_timeout(Duration::from_secs(5))
                .expect("every published command must receive a response")
                .expect("queued last_insert_rowid command should succeed");
        }
        assert_terminal_cleanup_once(&state);
    }

    #[test]
    fn async_admission_does_not_block_current_thread_runtime_when_mailbox_is_full() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        conn.sender()
            .expect("worker sender")
            .send(Command::BlockForTest {
                entered_tx,
                release_rx,
            })
            .expect("blocking test command should be admitted");
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker should enter deterministic gate");

        for _ in 0..COMMAND_MAILBOX_CAPACITY {
            let (response_tx, response_rx) = mpsc::sync_channel(1);
            drop(response_rx);
            conn.sender()
                .expect("worker sender")
                .try_send(Command::LastInsertRowid {
                    tx: Responder::Sync(response_tx),
                })
                .expect("mailbox fill command should fit");
        }

        let admission_signal = Arc::clone(&conn.sender().expect("worker sender").signal);
        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let watchdog_observer = Arc::clone(&watchdog_timed_out);
        let release_from_watchdog = release_tx.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                let _ = release_from_watchdog.send(());
            }
        });

        let admission_signal_for_release = Arc::clone(&admission_signal);
        test_runtime().block_on(async {
            let cx = Cx::new();
            let query = conn.query(&cx, "SELECT 1");
            let release = async move {
                while admission_signal_for_release
                    .async_reservers
                    .load(Ordering::Acquire)
                    == 0
                {
                    assert!(
                        !watchdog_observer.load(Ordering::Acquire),
                        "query failed before reaching async mailbox reservation"
                    );
                    future::yield_now().await;
                }
                let _ = release_tx.send(());
                let _ = watchdog_cancel_tx.send(());
            };
            let (result, ()) = future::zip(query, release).await;
            let rows = result.expect("query should run after mailbox capacity becomes available");
            assert_eq!(rows.len(), 1);
        });

        watchdog.join().expect("watchdog should not panic");
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "full-mailbox async admission blocked the current-thread runtime"
        );
        assert_eq!(
            admission_signal.async_publications.load(Ordering::Acquire),
            1,
            "capacity release must publish the waiting async command exactly once"
        );
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn late_alias_mask_does_not_invalidate_started_admission() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        fill_worker_mailbox(&conn);
        let signal = Arc::clone(&conn.sender().expect("worker sender").signal);
        let cx = Cx::new();
        let alias = cx.clone();

        let rows = test_runtime().block_on(async {
            let query = conn.query(&cx, "SELECT 1");
            let control = async {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while signal.async_reservers.load(Ordering::Acquire) == 0 {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "query did not reach the controlled reservation state"
                    );
                    future::yield_now().await;
                }
                let late_mask = alias.masked();
                let _ = release_tx.send(());
                while signal.async_publications.load(Ordering::Acquire) == 0 {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "started query did not publish while the late mask was held"
                    );
                    future::yield_now().await;
                }
                drop(late_mask);
            };
            let (result, ()) = future::zip(query, control).await;
            result.expect("a late alias mask must not invalidate a started call")
        });

        assert_eq!(rows.len(), 1);
        assert_eq!(
            signal.async_publications.load(Ordering::Acquire),
            1,
            "the started command must publish exactly once"
        );
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn late_alias_mask_does_not_defer_started_call_cancellation() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        fill_worker_mailbox(&conn);
        let signal = Arc::clone(&conn.sender().expect("worker sender").signal);
        let cx = Cx::new();
        let alias = cx.clone();

        let result = test_runtime().block_on(async {
            let execute = conn.execute(
                &cx,
                "CREATE TABLE late_mask_must_not_publish(id INTEGER PRIMARY KEY)",
            );
            let control = async {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while signal.async_reservers.load(Ordering::Acquire) == 0 {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "execute did not reach the controlled reservation state"
                    );
                    future::yield_now().await;
                }
                let late_mask = alias.masked();
                alias.cancel();
                while signal.async_reservers.load(Ordering::Acquire) != 0 {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "cancelled execute did not leave the reservation state"
                    );
                    future::yield_now().await;
                }
                drop(late_mask);
            };
            let (result, ()) = future::zip(execute, control).await;
            result
        });

        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "late masking must not defer cancellation of a started call: {result:?}"
        );
        assert_eq!(
            signal.async_publications.load(Ordering::Acquire),
            0,
            "a cancelled reservation must not publish its command"
        );
        let _ = release_tx.send(());
        let rows = conn
            .query_sync(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name = 'late_mask_must_not_publish'",
            )
            .expect("schema query should succeed");
        assert!(rows.is_empty());
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn late_alias_mask_does_not_defer_native_only_cancellation() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        fill_worker_mailbox(&conn);
        let signal = Arc::clone(&conn.sender().expect("worker sender").signal);
        let cx = Cx::new();
        let alias = cx.clone();
        let native = NativeCx::for_testing();
        cx.set_native_cx(native.clone());

        let result = test_runtime().block_on(async {
            let execute = conn.execute(
                &cx,
                "CREATE TABLE native_cancel_must_not_publish(id INTEGER PRIMARY KEY)",
            );
            let control = async {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while signal.async_reservers.load(Ordering::Acquire) == 0 {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "execute did not reach the controlled reservation state"
                    );
                    future::yield_now().await;
                }
                let late_mask = alias.masked();
                native.set_cancel_reason(asupersync::types::CancelReason::user(
                    "native-only late cancellation",
                ));
                while signal.async_reservers.load(Ordering::Acquire) != 0 {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "native-cancelled execute did not leave the reservation state"
                    );
                    future::yield_now().await;
                }
                drop(late_mask);
            };
            let (result, ()) = future::zip(execute, control).await;
            result
        });

        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "late masking must not defer native-only cancellation: {result:?}"
        );
        assert_eq!(
            signal.async_publications.load(Ordering::Acquire),
            0,
            "a native-cancelled reservation must not publish its command"
        );
        let _ = release_tx.send(());
        let rows = conn
            .query_sync(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name = 'native_cancel_must_not_publish'",
            )
            .expect("schema query should succeed");
        assert!(rows.is_empty());
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn ordinary_caller_cancellation_while_reserving_never_publishes_command() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        conn.sender()
            .expect("worker sender")
            .send(Command::BlockForTest {
                entered_tx,
                release_rx,
            })
            .expect("blocking test command should be admitted");
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker should enter deterministic gate");

        for _ in 0..COMMAND_MAILBOX_CAPACITY {
            let (response_tx, response_rx) = mpsc::sync_channel(1);
            drop(response_rx);
            conn.sender()
                .expect("worker sender")
                .try_send(Command::LastInsertRowid {
                    tx: Responder::Sync(response_tx),
                })
                .expect("mailbox fill command should fit");
        }

        let cx = Cx::new();
        let cx_for_cancel = cx.clone();
        let admission_signal = Arc::clone(&conn.sender().expect("worker sender").signal);

        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let watchdog_observer = Arc::clone(&watchdog_timed_out);
        let release_from_watchdog = release_tx.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                let _ = release_from_watchdog.send(());
            }
        });

        let runtime = test_runtime();
        let result = runtime.block_on(async {
            let execute = conn.execute(
                &cx,
                "CREATE TABLE cancelled_before_admission (id INTEGER PRIMARY KEY)",
            );
            let cancel = async move {
                while admission_signal.async_reservers.load(Ordering::Acquire) == 0 {
                    assert!(
                        !watchdog_observer.load(Ordering::Acquire),
                        "execute failed before reaching async mailbox reservation"
                    );
                    future::yield_now().await;
                }
                cx_for_cancel.cancel();
            };
            let (result, ()) = future::zip(execute, cancel).await;
            result
        });
        let _ = watchdog_cancel_tx.send(());
        let _ = release_tx.send(());
        watchdog.join().expect("watchdog should not panic");

        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "reservation cancellation should surface as Interrupt: {result:?}"
        );
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "caller Cx cancellation did not wake the full-mailbox reservation"
        );

        let rows = conn
            .query_sync(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name = 'cancelled_before_admission'",
            )
            .expect("schema query should succeed");
        assert!(
            rows.is_empty(),
            "a command cancelled while reserving capacity must never be published"
        );
        conn.close_sync().expect("close should succeed");
        drop(runtime);
    }

    #[test]
    fn local_relay_cancellation_while_reserving_never_publishes_command() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        conn.sender()
            .expect("worker sender")
            .send(Command::BlockForTest {
                entered_tx,
                release_rx,
            })
            .expect("blocking test command should be admitted");
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker should enter deterministic gate");

        for _ in 0..COMMAND_MAILBOX_CAPACITY {
            let (response_tx, response_rx) = mpsc::sync_channel(1);
            drop(response_rx);
            conn.sender()
                .expect("worker sender")
                .try_send(Command::LastInsertRowid {
                    tx: Responder::Sync(response_tx),
                })
                .expect("mailbox fill command should fit");
        }

        let root = Cx::new();
        let (operation, relay) = root.create_child_with_local_cancel_relay();
        let admission_signal = Arc::clone(&conn.sender().expect("worker sender").signal);
        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let watchdog_observer = Arc::clone(&watchdog_timed_out);
        let release_from_watchdog = release_tx.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                let _ = release_from_watchdog.send(());
            }
        });

        let runtime = test_runtime();
        let result = runtime.block_on(async {
            let execute = conn.execute(
                &operation,
                "CREATE TABLE local_cancelled_before_admission (id INTEGER PRIMARY KEY)",
            );
            let cancel = async move {
                while admission_signal.async_reservers.load(Ordering::Acquire) == 0 {
                    assert!(
                        !watchdog_observer.load(Ordering::Acquire),
                        "execute failed before reaching async mailbox reservation"
                    );
                    future::yield_now().await;
                }
                assert!(
                    relay.cancel_local(fsqlite_types::cx::CancelReason::UserInterrupt),
                    "live operation should accept relayed cancellation"
                );
            };
            let (result, ()) = future::zip(execute, cancel).await;
            result
        });
        let _ = watchdog_cancel_tx.send(());
        let _ = release_tx.send(());
        watchdog.join().expect("watchdog should not panic");

        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "local reservation cancellation should surface as Interrupt: {result:?}"
        );
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "local relay cancellation did not wake the full-mailbox reservation"
        );
        assert!(
            root.checkpoint().is_ok(),
            "operation-local relay must not cancel the parent context"
        );

        let rows = conn
            .query_sync(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name = 'local_cancelled_before_admission'",
            )
            .expect("schema query should succeed");
        assert!(
            rows.is_empty(),
            "a locally cancelled command must never be published"
        );
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn attached_detached_native_cancellation_while_reserving_never_publishes_command() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        fill_worker_mailbox(&conn);

        let operation = Cx::new();
        let native = NativeCx::for_testing();
        operation.set_native_cx(native.clone());
        let signal = Arc::clone(&conn.sender().expect("worker sender").signal);
        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let watchdog_observer = Arc::clone(&watchdog_timed_out);
        let release_from_watchdog = release_tx.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                let _ = release_from_watchdog.send(());
            }
        });

        let runtime = test_runtime();
        let result = runtime.block_on(async {
            let execute = conn.execute(
                &operation,
                "CREATE TABLE attached_native_cancelled_before_admission \
                 (id INTEGER PRIMARY KEY)",
            );
            let cancel = async {
                while signal.async_reservers.load(Ordering::Acquire) == 0 {
                    assert!(
                        !watchdog_observer.load(Ordering::Acquire),
                        "execute failed before reaching async mailbox reservation"
                    );
                    future::yield_now().await;
                }
                native.set_cancel_reason(asupersync::types::CancelReason::user(
                    "attached native admission cancellation test",
                ));
            };
            let (result, ()) = future::zip(execute, cancel).await;
            result
        });

        let _ = watchdog_cancel_tx.send(());
        let _ = release_tx.send(());
        watchdog.join().expect("watchdog should not panic");
        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "attached native cancellation should interrupt admission: {result:?}"
        );
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "attached native cancellation did not wake the full-mailbox reservation"
        );
        assert_eq!(
            signal.async_publications.load(Ordering::Acquire),
            0,
            "cancelled attached-native admission must not publish a command"
        );
        let rows = conn
            .query_sync(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name = \
                 'attached_native_cancelled_before_admission'",
            )
            .expect("FIFO schema query should succeed");
        assert!(rows.is_empty(), "cancelled command must have no SQL effect");
        conn.close_sync().expect("close should succeed");
        drop(runtime);
    }

    #[test]
    fn runtime_current_native_cancellation_while_reserving_never_publishes_command() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let release_tx = stall_worker(&conn);
        fill_worker_mailbox(&conn);

        let operation = Cx::new();
        let signal = Arc::clone(&conn.sender().expect("worker sender").signal);
        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let watchdog_observer = Arc::clone(&watchdog_timed_out);
        let release_from_watchdog = release_tx.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                let _ = release_from_watchdog.send(());
            }
        });

        let runtime = test_runtime();
        let result = runtime.block_on(async {
            let current_native =
                NativeCx::current().expect("test must run in an asupersync runtime");
            let execute = conn.execute(
                &operation,
                "CREATE TABLE runtime_native_cancelled_before_admission \
                 (id INTEGER PRIMARY KEY)",
            );
            let cancel = async {
                while signal.async_reservers.load(Ordering::Acquire) == 0 {
                    assert!(
                        !watchdog_observer.load(Ordering::Acquire),
                        "execute failed before reaching async mailbox reservation"
                    );
                    future::yield_now().await;
                }
                current_native.set_cancel_reason(asupersync::types::CancelReason::user(
                    "runtime-current admission cancellation test",
                ));
            };
            let (result, ()) = future::zip(execute, cancel).await;
            result
        });

        let _ = watchdog_cancel_tx.send(());
        let _ = release_tx.send(());
        watchdog.join().expect("watchdog should not panic");
        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "runtime-current native cancellation should interrupt admission: {result:?}"
        );
        assert!(
            !operation.is_cancel_requested(),
            "native-only cancellation must not mutate local FrankenSQLite cancellation state"
        );
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "runtime-current cancellation did not wake the full-mailbox reservation"
        );
        assert_eq!(
            signal.async_publications.load(Ordering::Acquire),
            0,
            "cancelled runtime-native admission must not publish a command"
        );
        let rows = conn
            .query_sync(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name = \
                 'runtime_native_cancelled_before_admission'",
            )
            .expect("FIFO schema query should succeed");
        assert!(rows.is_empty(), "cancelled command must have no SQL effect");
        conn.close_sync().expect("close should succeed");
        drop(runtime);
    }

    #[test]
    fn local_cancellation_after_publication_abandons_only_the_response_wait() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        conn.sender()
            .expect("worker sender")
            .send(Command::BlockForTest {
                entered_tx,
                release_rx,
            })
            .expect("blocking test command should be admitted");
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker should enter deterministic gate");

        let signal = Arc::clone(&conn.sender().expect("worker sender").signal);
        let root = Cx::new();
        let (operation, relay) = root.create_child_with_local_cancel_relay();
        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let release_from_watchdog = release_tx.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                let _ = release_from_watchdog.send(());
            }
        });

        let runtime = test_runtime();
        let result = runtime.block_on(async {
            let execute = conn.execute(
                &operation,
                "CREATE TABLE locally_cancelled_after_publication (id INTEGER PRIMARY KEY)",
            );
            let cancel = async {
                while signal.async_publications.load(Ordering::Acquire) == 0 {
                    future::yield_now().await;
                }
                assert!(
                    relay.cancel_local(fsqlite_types::cx::CancelReason::UserInterrupt),
                    "live operation should accept relayed cancellation"
                );
            };
            let (result, ()) = future::zip(execute, cancel).await;
            result
        });
        let _ = watchdog_cancel_tx.send(());
        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "published response wait should observe local cancellation: {result:?}"
        );
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "local cancellation did not release the response waiter promptly"
        );
        assert!(
            root.checkpoint().is_ok(),
            "operation-local cancellation must not affect its parent"
        );

        let _ = release_tx.send(());
        watchdog.join().expect("watchdog should not panic");
        let rows = conn
            .query_sync(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name = 'locally_cancelled_after_publication'",
            )
            .expect("schema query should succeed after releasing the worker");
        assert_eq!(
            rows.len(),
            1,
            "publication transfers effect ownership to the worker; response cancellation \
             must not silently retract an admitted command"
        );
        conn.close_sync().expect("close should succeed");
        drop(runtime);
    }

    #[test]
    fn native_cancellation_after_publication_abandons_only_the_response_wait() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("worker should open");
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        conn.sender()
            .expect("worker sender")
            .send(Command::BlockForTest {
                entered_tx,
                release_rx,
            })
            .expect("blocking test command should be admitted");
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker should enter deterministic gate");

        let signal = Arc::clone(&conn.sender().expect("worker sender").signal);
        let operation = Cx::new();
        let native = NativeCx::for_testing();
        operation.set_native_cx(native.clone());
        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let release_from_watchdog = release_tx.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                let _ = release_from_watchdog.send(());
            }
        });

        let runtime = test_runtime();
        let result = runtime.block_on(async {
            let execute = conn.execute(
                &operation,
                "CREATE TABLE native_cancelled_after_publication (id INTEGER PRIMARY KEY)",
            );
            let cancel = async {
                while signal.async_publications.load(Ordering::Acquire) == 0 {
                    future::yield_now().await;
                }
                native.set_cancel_reason(asupersync::types::CancelReason::user(
                    "native response cancellation test",
                ));
            };
            let (result, ()) = future::zip(execute, cancel).await;
            result
        });
        let _ = watchdog_cancel_tx.send(());
        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "published response wait should observe native cancellation: {result:?}"
        );
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "native cancellation did not release the response waiter promptly"
        );

        let _ = release_tx.send(());
        watchdog.join().expect("watchdog should not panic");
        let rows = conn
            .query_sync(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name = 'native_cancelled_after_publication'",
            )
            .expect("schema query should succeed after releasing the worker");
        assert_eq!(
            rows.len(),
            1,
            "native response cancellation must not silently retract an admitted command"
        );
        conn.close_sync().expect("close should succeed");
        drop(runtime);
    }

    #[test]
    fn cancellation_after_permit_before_publication_drops_command() {
        let (sender, mut receiver) = command_channel(1);
        let (fill_tx, fill_rx) = mpsc::sync_channel(1);
        drop(fill_rx);
        sender
            .try_send(Command::LastInsertRowid {
                tx: Responder::Sync(fill_tx),
            })
            .expect("initial command should fill the mailbox");
        let signal = Arc::clone(&sender.signal);
        signal
            .hold_after_async_reservation
            .store(true, Ordering::Release);

        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let watchdog_observer = Arc::clone(&watchdog_timed_out);
        let signal_for_watchdog = Arc::clone(&signal);
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                signal_for_watchdog
                    .hold_after_async_reservation
                    .store(false, Ordering::Release);
            }
        });

        let result = test_runtime().block_on(async {
            let cx = Cx::new();
            let cx_for_cancel = cx.clone();
            let preflight = preflight_async_call(&cx).expect("preflight should succeed");
            let (response_tx, response_rx) = mpsc::sync_channel(1);
            drop(response_rx);
            let admission = sender.send_async(
                &preflight,
                Command::Prepare {
                    sql: "must not publish".to_owned(),
                    tx: Responder::Sync(response_tx),
                },
            );
            let signal_for_cancel = Arc::clone(&signal);
            let cancel = async {
                while signal_for_cancel.async_reservers.load(Ordering::Acquire) == 0 {
                    assert!(
                        !watchdog_observer.load(Ordering::Acquire),
                        "send failed before reaching async mailbox reservation"
                    );
                    future::yield_now().await;
                }
                drop(
                    receiver
                        .try_recv()
                        .expect("driver should free one mailbox slot"),
                );
                while signal_for_cancel.async_permits.load(Ordering::Acquire) == 0 {
                    assert!(
                        !watchdog_observer.load(Ordering::Acquire),
                        "send failed before acquiring its mailbox permit"
                    );
                    future::yield_now().await;
                }
                cx_for_cancel.cancel();
                signal_for_cancel
                    .hold_after_async_reservation
                    .store(false, Ordering::Release);
            };
            let (result, ()) = future::zip(admission, cancel).await;
            result
        });
        let _ = watchdog_cancel_tx.send(());
        watchdog.join().expect("watchdog should not panic");

        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "pre-publication cancellation should surface as Interrupt: {result:?}"
        );
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "test reached its rescue path before permit cancellation completed"
        );
        assert_eq!(
            signal.async_permits.load(Ordering::Acquire),
            0,
            "test permit gate must be released"
        );
        assert!(
            matches!(receiver.try_recv(), Err(async_mpsc::RecvError::Empty)),
            "cancellation before publication must leave the mailbox empty"
        );
    }

    #[test]
    fn native_cancellation_after_permit_before_publication_drops_command() {
        let (sender, mut receiver) = command_channel(1);
        let (fill_tx, fill_rx) = mpsc::sync_channel(1);
        drop(fill_rx);
        sender
            .try_send(Command::LastInsertRowid {
                tx: Responder::Sync(fill_tx),
            })
            .expect("initial command should fill the mailbox");
        let signal = Arc::clone(&sender.signal);
        signal
            .hold_after_async_reservation
            .store(true, Ordering::Release);

        let watchdog_timed_out = Arc::new(AtomicBool::new(false));
        let watchdog_timed_out_in_thread = Arc::clone(&watchdog_timed_out);
        let watchdog_observer = Arc::clone(&watchdog_timed_out);
        let signal_for_watchdog = Arc::clone(&signal);
        let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::sync_channel(1);
        let watchdog = thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                watchdog_timed_out_in_thread.store(true, Ordering::Release);
                signal_for_watchdog
                    .hold_after_async_reservation
                    .store(false, Ordering::Release);
            }
        });

        let runtime = test_runtime();
        let cx = Cx::new();
        let native = NativeCx::for_testing();
        cx.set_native_cx(native.clone());
        let result = runtime.block_on(async {
            let preflight = preflight_async_call(&cx).expect("preflight should succeed");
            let (response_tx, response_rx) = mpsc::sync_channel(1);
            drop(response_rx);
            let admission = sender.send_async(
                &preflight,
                Command::Prepare {
                    sql: "native cancellation must not publish".to_owned(),
                    tx: Responder::Sync(response_tx),
                },
            );
            let signal_for_cancel = Arc::clone(&signal);
            let cancel = async {
                while signal_for_cancel.async_reservers.load(Ordering::Acquire) == 0 {
                    assert!(
                        !watchdog_observer.load(Ordering::Acquire),
                        "send failed before reaching async mailbox reservation"
                    );
                    future::yield_now().await;
                }
                drop(
                    receiver
                        .try_recv()
                        .expect("driver should free one mailbox slot"),
                );
                while signal_for_cancel.async_permits.load(Ordering::Acquire) == 0 {
                    assert!(
                        !watchdog_observer.load(Ordering::Acquire),
                        "send failed before acquiring its mailbox permit"
                    );
                    future::yield_now().await;
                }
                native.set_cancel_reason(asupersync::types::CancelReason::user(
                    "native permit cancellation test",
                ));
                signal_for_cancel
                    .hold_after_async_reservation
                    .store(false, Ordering::Release);
            };
            let (result, ()) = future::zip(admission, cancel).await;
            result
        });
        let _ = watchdog_cancel_tx.send(());
        watchdog.join().expect("watchdog should not panic");

        assert!(
            matches!(result, Err(FrankenError::Interrupt)),
            "native cancellation at the final checkpoint should interrupt admission: {result:?}"
        );
        assert!(
            cx.is_cancel_requested(),
            "an attached native cancellation must be mirrored into the local Cx family"
        );
        assert_eq!(
            cx.cancel_reason(),
            Some(fsqlite_types::cx::CancelReason::UserInterrupt),
            "the mirrored local reason must preserve native user cancellation semantics"
        );
        assert!(
            !watchdog_timed_out.load(Ordering::Acquire),
            "test reached its rescue path before native permit cancellation completed"
        );
        assert_eq!(
            signal.async_permits.load(Ordering::Acquire),
            0,
            "test permit gate must be released"
        );
        assert_eq!(
            signal.async_publications.load(Ordering::Acquire),
            0,
            "cancellation observed at the final checkpoint must prevent publication"
        );
        assert!(
            matches!(receiver.try_recv(), Err(async_mpsc::RecvError::Empty)),
            "native cancellation before publication must leave the mailbox empty"
        );
        drop(runtime);
    }

    #[test]
    fn dropping_production_async_admission_notifies_saturated_sync_sender() {
        let (sender, mut receiver) = command_channel(1);
        let (fill_tx, fill_rx) = mpsc::sync_channel(1);
        drop(fill_rx);
        sender
            .try_send(Command::LastInsertRowid {
                tx: Responder::Sync(fill_tx),
            })
            .expect("initial command should fill the mailbox");

        test_runtime().block_on(async {
            let cx = Cx::new();
            let preflight = preflight_async_call(&cx).expect("preflight should succeed");
            let (async_tx, async_rx) = mpsc::sync_channel(1);
            drop(async_rx);
            let mut admission = Box::pin(sender.send_async(
                &preflight,
                Command::Prepare {
                    sql: "async waiter".to_owned(),
                    tx: Responder::Sync(async_tx),
                },
            ));
            assert!(
                future::poll_once(&mut admission).await.is_none(),
                "production send_async should wait behind the full mailbox"
            );

            drop(
                receiver
                    .try_recv()
                    .expect("dequeue should free physical capacity"),
            );

            let sender_in_thread = sender.clone();
            let (sync_tx, sync_rx) = mpsc::sync_channel(1);
            drop(sync_rx);
            let (done_tx, done_rx) = mpsc::sync_channel(1);
            let sync_sender = thread::spawn(move || {
                let result = sender_in_thread.send(Command::Query {
                    sql: "sync waiter".to_owned(),
                    tx: Responder::Sync(sync_tx),
                });
                let _ = done_tx.send(result);
            });

            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while sender.signal.sync_waiters.load(Ordering::Acquire) == 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "sync sender did not park behind production async admission"
                );
                future::yield_now().await;
            }

            let generation_before_drop = sender.signal.current_generation();
            drop(admission);
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("production CapacityChangeGuard should wake sync sender")
                .expect("sync sender should claim capacity after async drop");
            sync_sender.join().expect("sync sender should not panic");
            assert_ne!(
                sender.signal.current_generation(),
                generation_before_drop,
                "dropping send_async must signal after unregistering its reservation"
            );
        });

        assert_eq!(
            sender.signal.async_reservers.load(Ordering::Acquire),
            0,
            "dropped admission must unregister before waking a synchronous sender"
        );
        assert!(
            matches!(
                receiver.try_recv(),
                Ok(Command::Query { ref sql, .. }) if sql == "sync waiter"
            ),
            "sync command should occupy the capacity released by async drop"
        );
    }

    #[test]
    fn guarded_async_reservation_drop_notifies_saturated_sync_sender() {
        let (sender, mut receiver) = command_channel(1);
        let (first_tx, first_rx) = mpsc::sync_channel(1);
        drop(first_rx);
        sender
            .try_send(Command::LastInsertRowid {
                tx: Responder::Sync(first_tx),
            })
            .expect("initial command should fill the mailbox");

        let native_cx = NativeCx::for_testing();
        let capacity_change = CapacityChangeGuard::new(&sender.signal);
        let mut reservation = sender.inner.reserve(&native_cx);
        assert!(
            future::block_on(future::poll_once(&mut reservation)).is_none(),
            "reservation should wait behind the full mailbox"
        );
        drop(
            receiver
                .try_recv()
                .expect("removing the queued command should expose one reserved slot"),
        );

        let sender_in_thread = sender.clone();
        let (second_tx, second_rx) = mpsc::sync_channel(1);
        drop(second_rx);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let sync_sender = thread::spawn(move || {
            let result = sender_in_thread.send(Command::LastInsertRowid {
                tx: Responder::Sync(second_tx),
            });
            let _ = done_tx.send(result);
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sender.signal.sync_waiters.load(Ordering::Acquire) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "synchronous sender did not reach the saturated wait state"
            );
            thread::yield_now();
        }

        // The reservation must unregister before its companion guard signals
        // the synchronous adapter.
        drop(reservation);
        drop(capacity_change);
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("guard notification should wake the synchronous sender")
            .expect("synchronous sender should claim the released slot");
        sync_sender.join().expect("sync sender should not panic");

        assert!(
            matches!(receiver.try_recv(), Ok(Command::LastInsertRowid { .. })),
            "the synchronous command must be published into the same mailbox"
        );
    }

    #[test]
    fn uncontended_sync_admission_and_dequeue_do_not_advance_signal_generation() {
        let (sender, mut receiver) = command_channel(2);
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        drop(response_rx);

        sender
            .send(Command::LastInsertRowid {
                tx: Responder::Sync(response_tx),
            })
            .expect("uncontended sync command should use the fast path");
        assert_eq!(
            sender.signal.current_generation(),
            0,
            "uncontended admission must not enter the signaling protocol"
        );
        assert_eq!(
            sender.signal.sync_retry_attempts.load(Ordering::Acquire),
            0,
            "uncontended admission must not enter the slow retry loop"
        );

        drop(
            receiver
                .try_recv()
                .expect("uncontended command should be queued"),
        );
        assert_eq!(
            sender.signal.current_generation(),
            0,
            "dequeue with no parked sync sender must not advance signal generation"
        );
    }

    #[test]
    fn queued_worker_commands_skip_per_command_executor_entry() {
        let (sender, mut receiver) = command_channel(2);
        for _ in 0..2 {
            let (response_tx, response_rx) = mpsc::sync_channel(1);
            drop(response_rx);
            sender
                .try_send(Command::LastInsertRowid {
                    tx: Responder::Sync(response_tx),
                })
                .expect("queued command should fit");
        }

        let worker_cx = NativeCx::<native_cap::None>::detached_cancel_context();
        drop(
            receiver
                .recv(&worker_cx)
                .expect("first queued command should be ready"),
        );
        drop(
            receiver
                .recv(&worker_cx)
                .expect("second queued command should be ready"),
        );
        assert_eq!(
            sender.signal.blocking_receives.load(Ordering::Acquire),
            0,
            "a hot queued dequeue must not enter a nested executor"
        );
    }

    #[test]
    fn saturated_sync_sender_does_not_poll_without_capacity_event() {
        let (sender, mut receiver) = command_channel(1);
        let (fill_tx, fill_rx) = mpsc::sync_channel(1);
        drop(fill_rx);
        sender
            .try_send(Command::LastInsertRowid {
                tx: Responder::Sync(fill_tx),
            })
            .expect("initial command should fill the mailbox");

        let sender_in_thread = sender.clone();
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        drop(response_rx);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let sync_sender = thread::spawn(move || {
            let result = sender_in_thread.send(Command::LastInsertRowid {
                tx: Responder::Sync(response_tx),
            });
            let _ = done_tx.send(result);
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sender.signal.sync_retry_attempts.load(Ordering::Acquire) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "synchronous sender did not enter event-driven wait"
            );
            thread::yield_now();
        }
        let attempts_after_park = sender.signal.sync_retry_attempts.load(Ordering::Acquire);
        thread::sleep(Duration::from_millis(75));
        assert_eq!(
            sender.signal.sync_retry_attempts.load(Ordering::Acquire),
            attempts_after_park,
            "a saturated sender must not retry without a capacity notification"
        );

        drop(
            receiver
                .try_recv()
                .expect("dequeue should notify the parked sender"),
        );
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("capacity event should wake the sender")
            .expect("sender should claim released capacity");
        sync_sender.join().expect("sync sender should not panic");
        assert!(
            matches!(receiver.try_recv(), Ok(Command::LastInsertRowid { .. })),
            "woken synchronous command should be published"
        );
    }

    #[test]
    fn capacity_notification_cannot_be_lost_between_predicate_and_park() {
        let (sender, mut receiver) = command_channel(1);
        let (fill_tx, fill_rx) = mpsc::sync_channel(1);
        drop(fill_rx);
        sender
            .try_send(Command::LastInsertRowid {
                tx: Responder::Sync(fill_tx),
            })
            .expect("initial command should fill the mailbox");
        sender
            .signal
            .hold_before_sync_park
            .store(true, Ordering::Release);

        let sender_in_thread = sender.clone();
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        drop(response_rx);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let sync_sender = thread::spawn(move || {
            let result = sender_in_thread.send(Command::LastInsertRowid {
                tx: Responder::Sync(response_tx),
            });
            let _ = done_tx.send(result);
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sender.signal.sync_park_predicates.load(Ordering::Acquire) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "synchronous sender did not reach the predicate-to-park boundary"
            );
            thread::yield_now();
        }

        let (receiver_tx, receiver_rx) = mpsc::sync_channel(1);
        let dequeue = thread::spawn(move || {
            drop(
                receiver
                    .try_recv()
                    .expect("dequeue should release physical capacity"),
            );
            let _ = receiver_tx.send(receiver);
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sender
            .signal
            .notification_observed_gate_contention
            .load(Ordering::Acquire)
            == 0
        {
            assert!(
                std::time::Instant::now() < deadline,
                "dequeue notifier did not observe the waiter holding the predicate mutex"
            );
            thread::yield_now();
        }
        assert!(
            matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "sender must remain at the controlled pre-park boundary"
        );
        assert!(
            matches!(receiver_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "notifier must wait for the predicate mutex before completing"
        );

        sender
            .signal
            .hold_before_sync_park
            .store(false, Ordering::Release);
        receiver = receiver_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("notifier should advance the generation after the waiter parks");
        dequeue.join().expect("dequeue thread should not panic");
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("coupled notification should wake the synchronous sender")
            .expect("synchronous sender should claim the released capacity");
        sync_sender.join().expect("sync sender should not panic");
        assert!(
            matches!(receiver.try_recv(), Ok(Command::LastInsertRowid { .. })),
            "woken synchronous command should be published"
        );
    }

    #[test]
    fn guarded_reserved_permit_drop_notifies_saturated_sync_sender() {
        let (sender, mut receiver) = command_channel(1);
        let native_cx = NativeCx::for_testing();
        let capacity_change = CapacityChangeGuard::new(&sender.signal);
        let permit = future::block_on(sender.inner.reserve(&native_cx))
            .expect("empty mailbox should yield a reserved permit");

        let sender_in_thread = sender.clone();
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        drop(response_rx);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let sync_sender = thread::spawn(move || {
            let result = sender_in_thread.send(Command::LastInsertRowid {
                tx: Responder::Sync(response_tx),
            });
            let _ = done_tx.send(result);
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sender.signal.sync_waiters.load(Ordering::Acquire) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "synchronous sender did not wait behind the reserved permit"
            );
            thread::yield_now();
        }

        drop(permit);
        drop(capacity_change);
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("guard notification should wake the synchronous sender")
            .expect("synchronous sender should claim released reserved capacity");
        sync_sender.join().expect("sync sender should not panic");
        assert!(
            matches!(receiver.try_recv(), Ok(Command::LastInsertRowid { .. })),
            "the synchronous command must occupy the released slot"
        );
    }

    #[test]
    fn earlier_async_reserver_precedes_later_sync_sender() {
        let (sender, mut receiver) = command_channel(1);
        let (fill_tx, fill_rx) = mpsc::sync_channel(1);
        drop(fill_rx);
        sender
            .try_send(Command::LastInsertRowid {
                tx: Responder::Sync(fill_tx),
            })
            .expect("initial command should fill the mailbox");

        let native_cx = NativeCx::for_testing();
        let mut reservation = sender.inner.reserve(&native_cx);
        assert!(
            future::block_on(future::poll_once(&mut reservation)).is_none(),
            "async reserver A should queue behind the full mailbox"
        );

        let sender_b = sender.clone();
        let (b_tx, b_rx) = mpsc::sync_channel(1);
        drop(b_rx);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let sync_sender = thread::spawn(move || {
            let result = sender_b.send(Command::Query {
                sql: "B".to_owned(),
                tx: Responder::Sync(b_tx),
            });
            let _ = done_tx.send(result);
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sender.signal.sync_waiters.load(Ordering::Acquire) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "sync sender B did not wait behind async reserver A"
            );
            thread::yield_now();
        }

        drop(
            receiver
                .try_recv()
                .expect("removing the fill command should wake reserver A"),
        );
        let permit_a = future::block_on(future::poll_once(&mut reservation))
            .expect("reserver A should become ready after dequeue")
            .expect("reserver A should claim capacity");
        let (a_tx, a_rx) = mpsc::sync_channel(1);
        drop(a_rx);
        permit_a
            .try_send(Command::Prepare {
                sql: "A".to_owned(),
                tx: Responder::Sync(a_tx),
            })
            .expect("reserver A should publish first");

        assert!(
            matches!(
                receiver.try_recv(),
                Ok(Command::Prepare { ref sql, .. }) if sql == "A"
            ),
            "the earlier async reserver must publish before sync sender B"
        );
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("sync sender B should finish after A is dequeued")
            .expect("sync sender B should publish second");
        sync_sender.join().expect("sync sender should not panic");
        assert!(
            matches!(
                receiver.try_recv(),
                Ok(Command::Query { ref sql, .. }) if sql == "B"
            ),
            "sync sender B must remain behind async reserver A"
        );
    }

    #[test]
    fn terminal_receiver_drop_wakes_blocked_async_and_sync_admissions() {
        let (sender, receiver) = command_channel(1);
        let (fill_tx, fill_rx) = mpsc::sync_channel(1);
        drop(fill_rx);
        sender
            .try_send(Command::LastInsertRowid {
                tx: Responder::Sync(fill_tx),
            })
            .expect("initial command should fill the mailbox");

        let native_cx = NativeCx::for_testing();
        let mut reservation = sender.inner.reserve(&native_cx);
        assert!(
            future::block_on(future::poll_once(&mut reservation)).is_none(),
            "async admission should be pending behind the full mailbox"
        );

        let sender_in_thread = sender.clone();
        let (sync_tx, sync_rx) = mpsc::sync_channel(1);
        drop(sync_rx);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let sync_sender = thread::spawn(move || {
            let result = sender_in_thread.send(Command::LastInsertRowid {
                tx: Responder::Sync(sync_tx),
            });
            let _ = done_tx.send(result);
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sender.signal.sync_waiters.load(Ordering::Acquire) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "sync admission did not reach its terminal-wakeup wait state"
            );
            thread::yield_now();
        }

        let generation_before_drop = sender.signal.current_generation();
        drop(receiver);
        assert_ne!(
            sender.signal.current_generation(),
            generation_before_drop,
            "receiver Drop must synchronously publish the terminal epoch"
        );
        let (late_tx, late_rx) = mpsc::sync_channel(1);
        drop(late_rx);
        assert!(
            matches!(
                sender.try_send(Command::LastInsertRowid {
                    tx: Responder::Sync(late_tx),
                }),
                Err(async_mpsc::SendError::Disconnected(_))
            ),
            "terminal notification must occur only after the receiver is closed"
        );

        assert!(
            matches!(
                future::block_on(future::poll_once(&mut reservation)),
                Some(Err(async_mpsc::SendError::Disconnected(())))
            ),
            "receiver termination must wake the async reservation as disconnected"
        );
        assert!(
            matches!(
                done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("terminal notification should wake sync admission"),
                Err(async_mpsc::SendError::Disconnected(_))
            ),
            "receiver termination must fail the sync admission as disconnected"
        );
        sync_sender.join().expect("sync sender should not panic");
    }

    #[test]
    fn receiver_drop_panic_still_wakes_a_saturated_sync_sender() {
        let (sender, receiver) = command_channel(1);
        let (fill_tx, fill_rx) = mpsc::sync_channel(1);
        drop(fill_rx);
        sender
            .try_send(Command::LastInsertRowid {
                tx: Responder::Sync(fill_tx),
            })
            .expect("initial command should fill the mailbox");

        let sender_in_thread = sender.clone();
        let (sync_tx, sync_rx) = mpsc::sync_channel(1);
        drop(sync_rx);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let sync_sender = thread::spawn(move || {
            let result = sender_in_thread.send(Command::LastInsertRowid {
                tx: Responder::Sync(sync_tx),
            });
            let _ = done_tx.send(result);
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sender.signal.sync_waiters.load(Ordering::Acquire) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "sync admission did not reach its terminal-wakeup wait state"
            );
            thread::yield_now();
        }
        sender
            .signal
            .panic_on_receiver_drop
            .store(true, Ordering::Release);
        let generation_before_drop = sender.signal.current_generation();
        let panic = catch_unwind(AssertUnwindSafe(|| drop(receiver)));
        assert!(panic.is_err(), "the receiver-drop sentinel must unwind");
        assert_ne!(
            sender.signal.current_generation(),
            generation_before_drop,
            "the unwind guard must publish the terminal epoch"
        );
        assert!(
            matches!(
                done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("terminal unwind must wake the sync admission"),
                Err(async_mpsc::SendError::Disconnected(_))
            ),
            "the woken sender must observe the already-closed receiver"
        );
        sync_sender.join().expect("sync sender should not panic");
    }

    #[test]
    fn test_async_connection_cancel() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");

            // Cancel the context — subsequent operations should fail.
            cx.cancel();

            let result = conn.execute(&cx, "SELECT 1").await;
            assert!(result.is_err(), "operation should fail after cancellation");
            match result.unwrap_err() {
                FrankenError::Interrupt => {}
                other => panic!("expected Interrupt, got: {other}"),
            }
        });
    }

    #[test]
    fn local_mask_is_rejected_with_attached_native_context_before_admission() {
        let (sender, mut receiver) = command_channel(1);
        let (fill_tx, fill_rx) = mpsc::sync_channel(1);
        drop(fill_rx);
        sender
            .try_send(Command::LastInsertRowid {
                tx: Responder::Sync(fill_tx),
            })
            .expect("initial command should fill the mailbox");

        test_runtime().block_on(async {
            let cx = Cx::new();
            let attached = NativeCx::for_testing();
            cx.set_native_cx(attached.clone());
            let mask = cx.masked();
            cx.cancel();
            assert!(
                attached.is_cancel_requested(),
                "ordinary cancellation should reach the attached native context"
            );

            let error = match preflight_async_call(&cx) {
                Ok(_) => panic!("masked async preflight must fail"),
                Err(error) => error,
            };
            match error {
                FrankenError::Internal(message) => assert!(
                    message.contains("cannot start while the caller FrankenSQLite Cx is masked"),
                    "unexpected masked-context diagnostic: {message}"
                ),
                other => panic!("expected masked-context error, got: {other}"),
            }
            assert_eq!(
                sender.signal.async_reservers.load(Ordering::Acquire),
                0,
                "masked preflight must not touch mailbox reservation state"
            );

            drop(mask);
            assert!(
                cx.checkpoint().is_err(),
                "cancellation becomes observable after the caller unmasks"
            );
        });

        assert!(
            matches!(receiver.try_recv(), Ok(Command::LastInsertRowid { .. })),
            "rejected preflight must leave the full mailbox untouched"
        );
    }

    #[test]
    fn async_runtime_preflight_rejects_before_dispatch() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        let cx = Cx::new();

        let error = future::block_on(
            conn.execute(&cx, "CREATE TABLE must_not_run (id INTEGER PRIMARY KEY)"),
        )
        .expect_err("an async call outside an asupersync runtime must fail preflight");
        match error {
            FrankenError::Internal(message) => assert!(
                message.contains("require an active asupersync runtime"),
                "unexpected preflight diagnostic: {message}"
            ),
            other => panic!("expected runtime preflight error, got: {other}"),
        }

        let rows = conn
            .query_sync(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name = 'must_not_run'",
            )
            .expect("schema query should succeed");
        assert!(
            rows.is_empty(),
            "a command rejected by async preflight must never reach the worker"
        );

        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn test_async_connection_execute_batch() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");

            conn.execute_batch(&cx, "CREATE TABLE a (x INTEGER); CREATE TABLE b (y TEXT);")
                .await
                .expect("batch should succeed");

            // Verify both tables exist.
            let _ = conn.query(&cx, "SELECT * FROM a").await.expect("table a");
            let _ = conn.query(&cx, "SELECT * FROM b").await.expect("table b");
        });
    }

    #[test]
    fn test_async_connection_close() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");

            conn.close(&cx).await.expect("close should succeed");

            // After close, operations should fail.
            let result = conn.query(&cx, "SELECT 1").await;
            assert!(result.is_err(), "query after close should fail");
        });
    }
}
