//! Async-native wrapper around [`Connection`] for use with asupersync's `Cx` capability context.
//!
//! Because [`Connection`] is `!Send` (it uses `Rc<RefCell<..>>` internally), this module
//! provides an [`AsyncConnection`] that runs a dedicated worker thread owning the
//! `Connection`. All SQL operations are dispatched to the worker via a command channel
//! and results are returned through response channels.
//!
//! Every async method accepts a `&Cx` and calls [`Cx::checkpoint()`] before dispatching,
//! ensuring cancel-correctness: if the context has been cancelled, the operation fails
//! fast without blocking on the worker.
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
//!
//! async fn example(cx: &Cx) -> Result<(), fsqlite::FrankenError> {
//!     let conn = AsyncConnection::open(cx, ":memory:").await?;
//!     conn.execute(cx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").await?;
//!     conn.execute_with_params(
//!         cx,
//!         "INSERT INTO t VALUES (?1, ?2)",
//!         &[SqliteValue::Integer(1), SqliteValue::Text("hello".into())],
//!     ).await?;
//!     let rows = conn.query(cx, "SELECT * FROM t").await?;
//!     assert_eq!(rows.len(), 1);
//!     Ok(())
//! }
//! ```

use crate::{Connection, ConnectionEnv, FrankenError, Row, SqliteValue};
#[cfg(test)]
use crate::{RuntimeConfig, RuntimeContext};
use asupersync::channel::mpsc as async_mpsc;
use asupersync::cx::Cx as NativeCx;
use asupersync::cx::cap as native_cap;
use asupersync::sync::OnceCell as NativeOnceCell;
use fsqlite_types::cx::{Cx, OperationCancellationSource, OperationCancellationToken};
use futures_lite::future;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as sync_mpsc;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle, Thread};

// ---------------------------------------------------------------------------
// Command protocol between async methods and the worker thread
// ---------------------------------------------------------------------------

const COMMAND_CAPACITY: usize = 32;
// Raw engine futures are deeply composed enough to overflow both Rust's
// default spawned-thread stack and an 8 MiB test-thread stack under the
// fs-ledger schema-migration workload. Each connection owns exactly one engine
// worker, so reserving a larger stack here is bounded per connection and keeps
// that implementation detail off both synchronous and asynchronous callers.
const WORKER_STACK_BYTES: usize = 16 * 1024 * 1024;

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A single-resolution response independent of a caller's cancellation
/// context. Before actor admission no sender exists in the mailbox. After
/// admission, the worker owns the sender and must either publish a result or
/// drop it, waking both blocking and async receivers.
enum ResponseStatus<T> {
    Pending(Option<Waker>),
    Ready(T),
    Disconnected,
}

struct ResponseState<T> {
    status: Mutex<ResponseStatus<T>>,
    ready: Condvar,
    receiver_alive: AtomicBool,
}

struct ResponseSender<T> {
    state: Option<Arc<ResponseState<T>>>,
}

struct ResponseReceiver<T> {
    state: Arc<ResponseState<T>>,
}

#[derive(Debug, Clone, Copy)]
struct ResponseDisconnected;

fn response_channel<T>() -> (ResponseSender<T>, ResponseReceiver<T>) {
    let state = Arc::new(ResponseState {
        status: Mutex::new(ResponseStatus::Pending(None)),
        ready: Condvar::new(),
        receiver_alive: AtomicBool::new(true),
    });
    (
        ResponseSender {
            state: Some(Arc::clone(&state)),
        },
        ResponseReceiver { state },
    )
}

impl<T> ResponseSender<T> {
    fn send(mut self, value: T) {
        let Some(state) = self.state.take() else {
            return;
        };
        if !state.receiver_alive.load(Ordering::Acquire) {
            return;
        }

        let mut value = Some(value);
        let (waker, accepted) = {
            let mut status = lock_unpoisoned(&state.status);
            if !state.receiver_alive.load(Ordering::Acquire) {
                (None, false)
            } else {
                match &mut *status {
                    ResponseStatus::Pending(waker) => {
                        let waker = waker.take();
                        *status = ResponseStatus::Ready(
                            value
                                .take()
                                .expect("response sender must retain its value until resolution"),
                        );
                        (waker, true)
                    }
                    ResponseStatus::Ready(_) | ResponseStatus::Disconnected => (None, false),
                }
            }
        };
        // A duplicate sender is a protocol violation, but do not destroy its
        // arbitrary response value while holding the response-state mutex.
        if !accepted {
            drop(value);
            return;
        }
        state.ready.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Drop for ResponseSender<T> {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        if !state.receiver_alive.load(Ordering::Acquire) {
            return;
        }

        let waker = {
            let mut status = lock_unpoisoned(&state.status);
            match &mut *status {
                ResponseStatus::Pending(waker) => {
                    let waker = waker.take();
                    *status = ResponseStatus::Disconnected;
                    waker
                }
                ResponseStatus::Ready(_) | ResponseStatus::Disconnected => None,
            }
        };
        state.ready.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> ResponseReceiver<T> {
    fn recv_blocking(self) -> Result<T, ResponseDisconnected> {
        let mut status = lock_unpoisoned(&self.state.status);
        loop {
            match std::mem::replace(&mut *status, ResponseStatus::Disconnected) {
                ResponseStatus::Ready(value) => return Ok(value),
                ResponseStatus::Disconnected => return Err(ResponseDisconnected),
                ResponseStatus::Pending(waker) => {
                    *status = ResponseStatus::Pending(waker);
                    status = self
                        .state
                        .ready
                        .wait(status)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            }
        }
    }
}

impl<T> Future for ResponseReceiver<T> {
    type Output = Result<T, ResponseDisconnected>;

    fn poll(self: Pin<&mut Self>, task_cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `RawWaker::clone` is user-defined and may be reentrant. Clone before
        // taking `status` so a custom waker cannot re-lock this mutex.
        let replacement_waker = task_cx.waker().clone();
        let (poll, displaced_waker, unused_replacement_waker) = {
            let mut status = lock_unpoisoned(&self.state.status);
            match std::mem::replace(&mut *status, ResponseStatus::Disconnected) {
                ResponseStatus::Ready(value) => {
                    (Poll::Ready(Ok(value)), None, Some(replacement_waker))
                }
                ResponseStatus::Disconnected => (
                    Poll::Ready(Err(ResponseDisconnected)),
                    None,
                    Some(replacement_waker),
                ),
                ResponseStatus::Pending(mut registered) => {
                    let (displaced_waker, unused_replacement_waker) = if registered
                        .as_ref()
                        .is_none_or(|existing| !existing.will_wake(&replacement_waker))
                    {
                        (
                            std::mem::replace(&mut registered, Some(replacement_waker)),
                            None,
                        )
                    } else {
                        (None, Some(replacement_waker))
                    };
                    *status = ResponseStatus::Pending(registered);
                    (Poll::Pending, displaced_waker, unused_replacement_waker)
                }
            }
        };
        // A Waker may execute reentrant code in Drop. In particular, never
        // destroy a replaced response waker while holding `status`.
        drop(displaced_waker);
        drop(unused_replacement_waker);
        poll
    }
}

impl<T> Drop for ResponseReceiver<T> {
    fn drop(&mut self) {
        self.state.receiver_alive.store(false, Ordering::Release);
        let previous = {
            let mut status = lock_unpoisoned(&self.state.status);
            std::mem::replace(&mut *status, ResponseStatus::Disconnected)
        };
        drop(previous);
    }
}

type Responder<T> = ResponseSender<Result<T, FrankenError>>;

struct CommandEnvelope {
    cancellation: Option<OperationCancellationToken>,
    command: Command,
}

/// A command sent from an async method to the worker thread.
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
        tx: sync_mpsc::SyncSender<Result<Option<Row>, FrankenError>>,
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
    Close {
        tx: Responder<()>,
    },
    #[cfg(test)]
    TestBlockActor {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
        tx: Responder<()>,
    },
    #[cfg(test)]
    TestActorContext {
        tx: Responder<ActorContextSnapshot>,
    },
    #[cfg(test)]
    TestFailClose {
        tx: Responder<()>,
    },
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActorContextSnapshot {
    task_native_cx_present: bool,
    root_native_cx_present: bool,
}

fn worker_open_err() -> FrankenError {
    FrankenError::Internal("async worker thread terminated during open".to_owned())
}

fn worker_dead_err() -> FrankenError {
    FrankenError::Internal("async worker thread terminated unexpectedly".to_owned())
}

fn stream_consumer_dead_err() -> FrankenError {
    FrankenError::Internal("synchronous query consumer stopped receiving rows".to_owned())
}

fn requires_runtime_err() -> FrankenError {
    FrankenError::Internal(
        "AsyncConnection async methods require the ambient asupersync task context".to_owned(),
    )
}

fn sync_on_runtime_err(operation: &str) -> FrankenError {
    FrankenError::Internal(format!(
        "{operation} cannot block an asupersync runtime task; use the async API"
    ))
}

fn worker_thread_spawn_err(error: std::io::Error) -> FrankenError {
    FrankenError::Internal(format!("failed to spawn async-api worker thread: {error}"))
}

fn actor_incompatible_env_err() -> FrankenError {
    FrankenError::Internal(
        "explicit ConnectionEnv is incompatible with AsyncConnection's dedicated actor because its runtime is not fully detached; both a task-affine native Cx on the runtime root and a captured native runtime handle must be absent"
            .to_owned(),
    )
}

/// Return the native context of the task currently polling the public async
/// operation. An attached project `Cx` may be shared by distinct tasks, so it
/// must never be used as this operation's cancellation-waker slot.
fn native_cx_for_polling_task() -> Result<NativeCx, FrankenError> {
    NativeCx::current().ok_or_else(requires_runtime_err)
}

/// Build a capability-empty native cancellation context for one pending
/// mailbox admission. Its cancellation state is independent from ambient
/// sibling operations, but the public project-Cx child below propagates the
/// caller's cancellation into it.
fn detached_native_cancel_cx() -> NativeCx<native_cap::None> {
    NativeCx::<native_cap::None>::detached_cancel_context()
}

async fn recv_authoritative_worker_response<T>(
    rx: ResponseReceiver<Result<T, FrankenError>>,
    mut cancellation_source: OperationCancellationSource,
    polling_native_cx: NativeCx,
) -> Result<T, FrankenError> {
    let cancellation_sentinel = NativeOnceCell::<()>::new();
    let mut cancellation = std::pin::pin!(cancellation_sentinel.wait(&polling_native_cx));
    let mut response = std::pin::pin!(rx);
    let mut cancellation_forwarded = false;

    std::future::poll_fn(|task_cx| {
        // A terminal worker result is authoritative. Poll it before the
        // caller-cancellation lane so a completed publication is never
        // overwritten by a cancellation observed in the same scheduler turn.
        match response.as_mut().poll(task_cx) {
            Poll::Ready(Ok(result)) => {
                cancellation_source.disarm();
                return Poll::Ready(result);
            }
            Poll::Ready(Err(_)) => {
                cancellation_source.disarm();
                return Poll::Ready(Err(worker_dead_err()));
            }
            Poll::Pending => {}
        }

        if !cancellation_forwarded {
            let native_cancelled = polling_native_cx.checkpoint().is_err()
                || matches!(cancellation.as_mut().poll(task_cx), Poll::Ready(Err(_)));
            if native_cancelled {
                cancellation_source.cancel_from_native_cx(&polling_native_cx);
                cancellation_forwarded = true;
            }
        }

        // After admission, cancellation signals engine work but never creates
        // a second public terminal outcome. Continue draining the one response
        // owned by the actor.
        Poll::Pending
    })
    .await
}

fn recv_worker_response<T>(
    rx: ResponseReceiver<Result<T, FrankenError>>,
) -> Result<T, FrankenError> {
    rx.recv_blocking().map_err(|_| worker_dead_err())?
}

// ---------------------------------------------------------------------------
// Worker task
// ---------------------------------------------------------------------------

struct WorkerLifecycle {
    finished: AtomicBool,
    waiter: Mutex<Option<Waker>>,
    changed: Condvar,
    command_capacity: Arc<CommandCapacitySignal>,
    #[cfg(test)]
    close_connection_calls: AtomicUsize,
}

impl WorkerLifecycle {
    fn new(command_capacity: Arc<CommandCapacitySignal>) -> Self {
        Self {
            finished: AtomicBool::new(false),
            waiter: Mutex::new(None),
            changed: Condvar::new(),
            command_capacity,
            #[cfg(test)]
            close_connection_calls: AtomicUsize::new(0),
        }
    }

    fn finish(&self) {
        self.finished.store(true, Ordering::Release);
        self.command_capacity.notify();
        let waiter = {
            let mut waiter = lock_unpoisoned(&self.waiter);
            waiter.take()
        };
        self.changed.notify_all();
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}

struct WorkerExit<'a> {
    lifecycle: &'a WorkerLifecycle,
}

impl Future for WorkerExit<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, task_cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.lifecycle.finished.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        // `RawWaker::clone` is user-defined and may be reentrant. Clone before
        // taking `waiter` so a custom waker cannot re-lock this mutex.
        let replacement_waker = task_cx.waker().clone();
        let (poll, displaced_waker, unused_replacement_waker) = {
            let mut waiter = lock_unpoisoned(&self.lifecycle.waiter);
            if self.lifecycle.finished.load(Ordering::Acquire) {
                (Poll::Ready(()), None, Some(replacement_waker))
            } else if waiter
                .as_ref()
                .is_none_or(|registered| !registered.will_wake(&replacement_waker))
            {
                (
                    Poll::Pending,
                    std::mem::replace(&mut *waiter, Some(replacement_waker)),
                    None,
                )
            } else {
                (Poll::Pending, None, Some(replacement_waker))
            }
        };
        drop(displaced_waker);
        drop(unused_replacement_waker);
        poll
    }
}

struct WorkerExitGuard {
    lifecycle: Arc<WorkerLifecycle>,
}

impl Drop for WorkerExitGuard {
    fn drop(&mut self) {
        self.lifecycle.finish();
    }
}

/// Blocking-side notification for capacity released by the async command
/// mailbox.
///
/// Async callers use the channel's native reserve future. Synchronous callers
/// cannot manufacture an effect-capable native `Cx` merely to wait for
/// capacity, so they pair `try_reserve` with this condition variable instead.
struct CommandCapacitySignal {
    epoch: Mutex<u64>,
    changed: Condvar,
}

impl CommandCapacitySignal {
    fn new() -> Self {
        Self {
            epoch: Mutex::new(0),
            changed: Condvar::new(),
        }
    }

    fn notify(&self) {
        let mut epoch = lock_unpoisoned(&self.epoch);
        *epoch = epoch.wrapping_add(1);
        drop(epoch);
        self.changed.notify_all();
    }

    fn reserve_blocking<'a>(
        &self,
        tx: &'a async_mpsc::Sender<CommandEnvelope>,
    ) -> Result<async_mpsc::SendPermit<'a, CommandEnvelope>, async_mpsc::SendError<()>> {
        loop {
            match tx.try_reserve() {
                Ok(permit) => return Ok(permit),
                Err(async_mpsc::SendError::Full(())) => {
                    let epoch = lock_unpoisoned(&self.epoch);
                    // Close the gap between the first Full observation and
                    // entering the condition-variable wait.
                    match tx.try_reserve() {
                        Ok(permit) => return Ok(permit),
                        Err(async_mpsc::SendError::Full(())) => {
                            let observed = *epoch;
                            let (epoch, _) = self
                                .changed
                                .wait_timeout_while(
                                    epoch,
                                    std::time::Duration::from_millis(10),
                                    |current| *current == observed,
                                )
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            drop(epoch);
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
}

struct CommandSender {
    tx: Option<async_mpsc::Sender<CommandEnvelope>>,
    worker_thread: Thread,
    command_capacity: Arc<CommandCapacitySignal>,
}

impl CommandSender {
    fn tx(&self) -> Result<&async_mpsc::Sender<CommandEnvelope>, FrankenError> {
        self.tx.as_ref().ok_or_else(worker_dead_err)
    }

    async fn send_async<Caps, F>(
        &self,
        cx: &Cx<Caps>,
        cancellation: Option<OperationCancellationToken>,
        build: F,
    ) -> Result<NativeCx, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        F: FnOnce() -> Command,
    {
        checkpoint_or_interrupt(cx)?;
        let polling_native_cx = native_cx_for_polling_task()?;
        if polling_native_cx.checkpoint().is_err() {
            return Err(FrankenError::Interrupt);
        }
        let tx = self.tx()?;

        let permit = match tx.try_reserve() {
            Ok(permit) => permit,
            Err(async_mpsc::SendError::Full(())) => {
                let local_native_cx = detached_native_cancel_cx();
                // Construct this child without ever copying the ambient task's
                // native handle. It exists only while reserving capacity and
                // bridges project-Cx cancellation into an independent native
                // waiter that cannot poison sibling operations.
                let admission_cx = cx.create_native_free_child();
                admission_cx.set_native_cancel_relay(local_native_cx.clone());
                let local_cancel_sentinel = NativeOnceCell::<()>::new();
                let polling_cancel_sentinel = NativeOnceCell::<()>::new();
                let mut local_cancel = std::pin::pin!(local_cancel_sentinel.wait(&local_native_cx));
                let mut polling_cancel =
                    std::pin::pin!(polling_cancel_sentinel.wait(&polling_native_cx));
                let mut reserve = std::pin::pin!(tx.reserve(&polling_native_cx));

                std::future::poll_fn(|task_cx| {
                    if matches!(local_cancel.as_mut().poll(task_cx), Poll::Ready(Err(_)))
                        || matches!(polling_cancel.as_mut().poll(task_cx), Poll::Ready(Err(_)))
                    {
                        return Poll::Ready(Err(FrankenError::Interrupt));
                    }
                    match reserve.as_mut().poll(task_cx) {
                        Poll::Ready(Ok(permit)) => {
                            if checkpoint_or_interrupt(cx).is_err()
                                || polling_native_cx.checkpoint().is_err()
                            {
                                Poll::Ready(Err(FrankenError::Interrupt))
                            } else {
                                Poll::Ready(Ok(permit))
                            }
                        }
                        Poll::Ready(Err(error)) => Poll::Ready(Err(send_err(error))),
                        Poll::Pending => Poll::Pending,
                    }
                })
                .await?
            }
            Err(error) => return Err(send_err(error)),
        };

        // Reservation owns capacity but has not admitted an actor effect.
        // Re-check both cancellation planes immediately before the single
        // linearizing enqueue. After `try_send` succeeds we never report an
        // interrupt for the same operation.
        checkpoint_or_interrupt(cx)?;
        if polling_native_cx.checkpoint().is_err() {
            return Err(FrankenError::Interrupt);
        }
        if cancellation
            .as_ref()
            .is_some_and(OperationCancellationToken::is_cancel_requested)
        {
            return Err(FrankenError::Interrupt);
        }
        permit
            .try_send(CommandEnvelope {
                cancellation,
                command: build(),
            })
            .map_err(send_err)?;
        self.worker_thread.unpark();
        Ok(polling_native_cx)
    }

    fn send_sync<F>(&self, build: F) -> Result<(), FrankenError>
    where
        F: FnOnce() -> Command,
    {
        if NativeCx::current().is_some() {
            return Err(sync_on_runtime_err("synchronous AsyncConnection methods"));
        }
        let permit = self
            .command_capacity
            .reserve_blocking(self.tx()?)
            .map_err(send_err)?;
        permit
            .try_send(CommandEnvelope {
                cancellation: None,
                command: build(),
            })
            .map_err(send_err)?;
        self.worker_thread.unpark();
        Ok(())
    }
}

impl Drop for CommandSender {
    fn drop(&mut self) {
        // Disconnect first, then wake. A wake before dropping the final sender
        // can be consumed before the actor observes its terminal receiver
        // state and leave an idle worker parked forever.
        drop(self.tx.take());
        self.worker_thread.unpark();
    }
}

fn publish_transaction_state(conn: &Connection, in_txn: &AtomicBool) {
    in_txn.store(conn.in_transaction(), Ordering::Release);
}

fn normalize_operation_error(
    cancellation: Option<&OperationCancellationToken>,
    error: FrankenError,
) -> FrankenError {
    if matches!(error, FrankenError::Abort)
        && cancellation.is_some_and(OperationCancellationToken::cancellation_was_observed)
    {
        FrankenError::Interrupt
    } else {
        error
    }
}

fn respond<T>(
    conn: &Connection,
    in_txn: &AtomicBool,
    cancellation: Option<&OperationCancellationToken>,
    tx: Responder<T>,
    result: Result<T, FrankenError>,
) {
    // The worker is authoritative. This includes raw SQL BEGIN/COMMIT/
    // ROLLBACK and failed statements, not only public transaction wrappers.
    publish_transaction_state(conn, in_txn);
    tx.send(result.map_err(|error| normalize_operation_error(cancellation, error)));
}

fn close_connection_once(
    conn: &mut Connection,
    close_succeeded: &AtomicBool,
    lifecycle: &WorkerLifecycle,
) -> Result<(), FrankenError> {
    #[cfg(test)]
    lifecycle
        .close_connection_calls
        .fetch_add(1, Ordering::AcqRel);
    if close_succeeded.load(Ordering::Acquire) {
        return Err(FrankenError::Internal(
            "async worker attempted to close its connection more than once".to_owned(),
        ));
    }
    let result = future::block_on(conn.close_in_place());
    if result.is_ok() {
        close_succeeded.store(true, Ordering::Release);
    }
    result
}

impl Command {
    fn respond_cancelled(self) -> bool {
        let interrupted = || FrankenError::Interrupt;
        match self {
            Self::Prepare { tx, .. }
            | Self::ExecuteBatch { tx, .. }
            | Self::BeginTransaction { tx }
            | Self::CommitTransaction { tx }
            | Self::RollbackTransaction { tx } => tx.send(Err(interrupted())),
            Self::Query { tx, .. } | Self::QueryWithParams { tx, .. } => {
                tx.send(Err(interrupted()));
            }
            Self::QueryWithParamsStream { tx, .. } => {
                let _ = tx.send(Err(interrupted()));
            }
            Self::QueryRow { tx, .. } | Self::QueryRowWithParams { tx, .. } => {
                tx.send(Err(interrupted()));
            }
            Self::Execute { tx, .. }
            | Self::ExecuteWithParams { tx, .. }
            | Self::ExecuteManyWithParamsInTransaction { tx, .. } => {
                tx.send(Err(interrupted()));
            }
            Self::LastInsertRowid { tx } => tx.send(Err(interrupted())),
            Self::Close { tx } => {
                tx.send(Err(FrankenError::Internal(
                    "close commands must not carry operation cancellation".to_owned(),
                )));
                return false;
            }
            #[cfg(test)]
            Self::TestBlockActor { tx, .. } => tx.send(Err(interrupted())),
            #[cfg(test)]
            Self::TestActorContext { tx } => tx.send(Err(interrupted())),
            #[cfg(test)]
            Self::TestFailClose { tx } => tx.send(Err(FrankenError::Internal(
                "test close failure must not carry operation cancellation".to_owned(),
            ))),
        }
        true
    }
}

fn process_command(
    conn: &mut Connection,
    in_txn: &AtomicBool,
    close_succeeded: &AtomicBool,
    lifecycle: &WorkerLifecycle,
    envelope: CommandEnvelope,
) -> bool {
    let CommandEnvelope {
        cancellation,
        command: cmd,
    } = envelope;
    if cancellation
        .as_ref()
        .is_some_and(|operation| operation.checkpoint().is_err())
    {
        return cmd.respond_cancelled();
    }

    // The guard derives engine operation contexts from the connection root;
    // the opaque token contributes cancellation only. It owns its stack
    // registration rather than borrowing `conn`, so it remains live across
    // the command's async engine execution and restores nesting on Drop.
    let _operation_cancellation = cancellation
        .as_ref()
        .map(|operation| conn.enter_operation_cancellation(operation));
    match cmd {
        Command::Prepare { sql, tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(conn.prepare(&sql)).map(drop),
            );
        }
        Command::Query { sql, tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(conn.query(&sql)),
            );
        }
        Command::QueryWithParams { sql, params, tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(conn.query_with_params(&sql, &params)),
            );
        }
        Command::QueryWithParamsStream { sql, params, tx } => {
            let result = future::block_on(conn.query_with_params_for_each(&sql, &params, |row| {
                tx.send(Ok(Some(row.clone())))
                    .map_err(|_| stream_consumer_dead_err())
            }));
            publish_transaction_state(conn, in_txn);
            match result {
                Ok(()) => {
                    let _ = tx.send(Ok(None));
                }
                Err(error) => {
                    let _ = tx.send(Err(normalize_operation_error(cancellation.as_ref(), error)));
                }
            }
        }
        Command::QueryRow { sql, tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(conn.query_row(&sql)),
            );
        }
        Command::QueryRowWithParams { sql, params, tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(conn.query_row_with_params(&sql, &params)),
            );
        }
        Command::Execute { sql, tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(conn.execute(&sql)),
            );
        }
        Command::ExecuteWithParams { sql, params, tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(conn.execute_with_params(&sql, &params)),
            );
        }
        Command::ExecuteManyWithParamsInTransaction {
            sql,
            parameter_sets,
            tx,
        } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(
                    conn.execute_many_with_params_skip_statement_savepoint_in_explicit_txn(
                        &sql,
                        &parameter_sets,
                    ),
                ),
            );
        }
        Command::ExecuteBatch { sql, tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(conn.execute_batch(&sql)),
            );
        }
        Command::BeginTransaction { tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(conn.begin_transaction()).map(drop),
            );
        }
        Command::CommitTransaction { tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(conn.commit_transaction()),
            );
        }
        Command::RollbackTransaction { tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                future::block_on(conn.rollback_transaction()),
            );
        }
        Command::LastInsertRowid { tx } => {
            respond(
                conn,
                in_txn,
                cancellation.as_ref(),
                tx,
                Ok(conn.last_insert_rowid()),
            );
        }
        Command::Close { tx } => {
            let result = close_connection_once(conn, close_succeeded, lifecycle);
            let closed = result.is_ok();
            if closed {
                in_txn.store(false, Ordering::Release);
            } else {
                publish_transaction_state(conn, in_txn);
            }
            tx.send(result);
            return !closed;
        }
        #[cfg(test)]
        Command::TestBlockActor {
            entered,
            release,
            tx,
        } => {
            let _ = entered.send(());
            let _ = release.recv();
            respond(conn, in_txn, cancellation.as_ref(), tx, Ok(()));
        }
        #[cfg(test)]
        Command::TestActorContext { tx } => {
            let snapshot = ActorContextSnapshot {
                task_native_cx_present: NativeCx::current().is_some(),
                root_native_cx_present: conn.root_cx().attached_native_cx().is_some(),
            };
            respond(conn, in_txn, cancellation.as_ref(), tx, Ok(snapshot));
        }
        #[cfg(test)]
        Command::TestFailClose { tx } => {
            tx.send(Err(FrankenError::Internal(
                "injected retryable close failure".to_owned(),
            )));
        }
    }
    true
}

fn worker_loop(
    conn: &mut Connection,
    in_txn: &AtomicBool,
    mut rx: async_mpsc::Receiver<CommandEnvelope>,
    close_succeeded: &AtomicBool,
    lifecycle: &WorkerLifecycle,
) {
    loop {
        let cmd = match rx.try_recv() {
            Ok(cmd) => {
                lifecycle.command_capacity.notify();
                cmd
            }
            Err(async_mpsc::RecvError::Empty | async_mpsc::RecvError::Cancelled) => {
                thread::park();
                continue;
            }
            Err(async_mpsc::RecvError::Disconnected) => return,
        };
        if !process_command(conn, in_txn, close_succeeded, lifecycle, cmd) {
            return;
        }
    }
}

enum OpenRequest {
    Create {
        path: String,
        env: ConnectionEnv,
    },
    Existing {
        path: String,
        env: ConnectionEnv,
    },
    ReadOnly {
        path: String,
        env: ConnectionEnv,
    },
    Flags {
        path: String,
        flags: crate::compat::OpenFlags,
        env: ConnectionEnv,
    },
    #[cfg(test)]
    TestBlocked {
        env: ConnectionEnv,
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
        observed_engine_cancellation: std::sync::mpsc::Sender<bool>,
    },
}

impl OpenRequest {
    fn with_operation_cancellation(self, operation: OperationCancellationToken) -> Self {
        match self {
            Self::Create { path, env } => Self::Create {
                path,
                env: env.with_dedicated_worker_open_operation(operation),
            },
            Self::Existing { path, env } => Self::Existing {
                path,
                env: env.with_dedicated_worker_open_operation(operation),
            },
            Self::ReadOnly { path, env } => Self::ReadOnly {
                path,
                env: env.with_dedicated_worker_open_operation(operation),
            },
            Self::Flags { path, flags, env } => Self::Flags {
                path,
                flags,
                env: env.with_dedicated_worker_open_operation(operation),
            },
            #[cfg(test)]
            Self::TestBlocked {
                env,
                entered,
                release,
                observed_engine_cancellation,
            } => Self::TestBlocked {
                env: env.with_dedicated_worker_open_operation(operation),
                entered,
                release,
                observed_engine_cancellation,
            },
        }
    }

    #[cfg(test)]
    fn runtime_id(&self) -> u64 {
        let env = match self {
            Self::Create { env, .. }
            | Self::Existing { env, .. }
            | Self::ReadOnly { env, .. }
            | Self::Flags { env, .. }
            | Self::TestBlocked { env, .. } => env,
        };
        env.runtime().runtime_id()
    }

    fn ensure_actor_compatible(&self) -> Result<(), FrankenError> {
        let env = match self {
            Self::Create { env, .. }
            | Self::Existing { env, .. }
            | Self::ReadOnly { env, .. }
            | Self::Flags { env, .. } => env,
            #[cfg(test)]
            Self::TestBlocked { env, .. } => env,
        };
        if !env.runtime().is_detached_for_dedicated_worker() {
            return Err(actor_incompatible_env_err());
        }
        Ok(())
    }

    fn open(self) -> Result<Connection, FrankenError> {
        future::block_on(async move {
            match self {
                Self::Create { path, env } => Connection::open_with_env(path, env).await,
                Self::Existing { path, env } => Connection::open_existing_with_env(path, env).await,
                Self::ReadOnly { path, env } => {
                    Connection::open_schema_only_with_env(path, env).await
                }
                Self::Flags { path, flags, env } => open_with_flags_and_env(path, flags, env).await,
                #[cfg(test)]
                Self::TestBlocked {
                    env,
                    entered,
                    release,
                    observed_engine_cancellation,
                } => {
                    let _ = entered.send(());
                    let _ = release.recv();
                    let result = Connection::open_with_env(":memory:", env).await;
                    let _ = observed_engine_cancellation
                        .send(matches!(&result, Err(FrankenError::Abort)));
                    result
                }
            }
        })
    }
}

async fn open_with_flags_and_env(
    path: String,
    flags: crate::compat::OpenFlags,
    env: ConnectionEnv,
) -> Result<Connection, FrankenError> {
    use crate::compat::OpenFlags;

    let read_only = flags.contains(OpenFlags::SQLITE_OPEN_READ_ONLY);
    let read_write = flags.contains(OpenFlags::SQLITE_OPEN_READ_WRITE);
    let create = flags.contains(OpenFlags::SQLITE_OPEN_CREATE);
    let no_mutex = flags.contains(OpenFlags::SQLITE_OPEN_NO_MUTEX);
    let full_mutex = flags.contains(OpenFlags::SQLITE_OPEN_FULL_MUTEX);
    let shared_cache = flags.contains(OpenFlags::SQLITE_OPEN_SHARED_CACHE);
    let private_cache = flags.contains(OpenFlags::SQLITE_OPEN_PRIVATE_CACHE);

    if no_mutex && full_mutex {
        return Err(FrankenError::TypeMismatch {
            expected: "at most one of SQLITE_OPEN_NO_MUTEX or SQLITE_OPEN_FULL_MUTEX".into(),
            actual: "conflicting SQLite-compatible mutex open flags".to_owned(),
        });
    }
    if shared_cache && private_cache {
        return Err(FrankenError::TypeMismatch {
            expected: "at most one of SQLITE_OPEN_SHARED_CACHE or SQLITE_OPEN_PRIVATE_CACHE".into(),
            actual: "conflicting SQLite-compatible cache open flags".to_owned(),
        });
    }

    match (read_only, read_write, create) {
        (true, false, false) if path != ":memory:" => {
            Connection::open_schema_only_with_env(path, env).await
        }
        (true, false, false) => Err(FrankenError::NotImplemented(
            "read-only :memory: connections are not supported".to_owned(),
        )),
        (false, true, false) if path == ":memory:" => Connection::open_with_env(path, env).await,
        (false, true, false) => Connection::open_existing_with_env(path, env).await,
        (false, true, true) => Connection::open_with_env(path, env).await,
        _ => Err(FrankenError::TypeMismatch {
            expected:
                "one of SQLITE_OPEN_READ_ONLY, SQLITE_OPEN_READ_WRITE, or SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE"
                    .into(),
            actual: "invalid SQLite-compatible access-mode flags".to_owned(),
        }),
    }
}

struct WorkerHandle {
    join: Option<JoinHandle<()>>,
    lifecycle: Arc<WorkerLifecycle>,
}

impl WorkerHandle {
    fn wait_sync(mut self) {
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }

    async fn wait_async_ref(&self) {
        WorkerExit {
            lifecycle: &self.lifecycle,
        }
        .await;
    }

    async fn join_async_ref(&mut self) {
        while self.join.as_ref().is_some_and(|join| !join.is_finished()) {
            future::yield_now().await;
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn spawn_worker_thread(
    request: OpenRequest,
    cmd_rx: async_mpsc::Receiver<CommandEnvelope>,
    open_tx: ResponseSender<Result<(), FrankenError>>,
    in_txn: Arc<AtomicBool>,
    lifecycle: Arc<WorkerLifecycle>,
) -> Result<JoinHandle<()>, FrankenError> {
    thread::Builder::new()
        .name("fsqlite-worker".to_owned())
        .stack_size(WORKER_STACK_BYTES)
        .spawn(move || {
            let _exit = WorkerExitGuard {
                lifecycle: Arc::clone(&lifecycle),
            };
            match catch_unwind(AssertUnwindSafe(|| request.open())) {
                Ok(Ok(mut conn)) => {
                    let close_succeeded = AtomicBool::new(false);
                    open_tx.send(Ok(()));
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        worker_loop(&mut conn, &in_txn, cmd_rx, &close_succeeded, &lifecycle);
                    }));
                    in_txn.store(false, Ordering::Release);
                    if !close_succeeded.load(Ordering::Acquire) {
                        let _ = catch_unwind(AssertUnwindSafe(|| {
                            let _ = close_connection_once(&mut conn, &close_succeeded, &lifecycle);
                        }));
                    }
                }
                Ok(Err(error)) => open_tx.send(Err(error)),
                Err(_) => {
                    // Dropping the unresolved opener responder reports worker
                    // death to the opener without fabricating a source error.
                }
            }
        })
        .map_err(worker_thread_spawn_err)
}

fn start_worker(
    request: OpenRequest,
) -> Result<
    (
        CommandSender,
        WorkerHandle,
        Arc<AtomicBool>,
        ResponseReceiver<Result<(), FrankenError>>,
    ),
    FrankenError,
> {
    let (cmd_tx, cmd_rx) = async_mpsc::channel(COMMAND_CAPACITY);
    let (open_tx, open_rx) = response_channel();
    let in_txn = Arc::new(AtomicBool::new(false));
    let command_capacity = Arc::new(CommandCapacitySignal::new());
    let lifecycle = Arc::new(WorkerLifecycle::new(Arc::clone(&command_capacity)));
    let join = spawn_worker_thread(
        request,
        cmd_rx,
        open_tx,
        Arc::clone(&in_txn),
        Arc::clone(&lifecycle),
    )?;
    let worker_thread = join.thread().clone();
    Ok((
        CommandSender {
            tx: Some(cmd_tx),
            worker_thread,
            command_capacity,
        },
        WorkerHandle {
            join: Some(join),
            lifecycle,
        },
        in_txn,
        open_rx,
    ))
}

fn wait_for_worker_open(
    open_rx: ResponseReceiver<Result<(), FrankenError>>,
) -> Result<(), FrankenError> {
    open_rx.recv_blocking().map_err(|_| worker_open_err())?
}

async fn wait_for_worker_open_async<Caps>(
    cx: &Cx<Caps>,
    open_rx: ResponseReceiver<Result<(), FrankenError>>,
    mut cancellation_source: OperationCancellationSource,
    cancellation_observation: OperationCancellationToken,
) -> Result<(), FrankenError>
where
    Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
{
    let polling_native_cx = native_cx_for_polling_task()?;
    let project_native_cx = detached_native_cancel_cx();
    let project_wait_cx = cx.create_native_free_child();
    project_wait_cx.set_native_cancel_relay(project_native_cx.clone());

    let project_sentinel = NativeOnceCell::<()>::new();
    let polling_sentinel = NativeOnceCell::<()>::new();
    let mut project_cancel = std::pin::pin!(project_sentinel.wait(&project_native_cx));
    let mut polling_cancel = std::pin::pin!(polling_sentinel.wait(&polling_native_cx));
    let mut response = std::pin::pin!(open_rx);
    let mut cancellation_forwarded = false;

    std::future::poll_fn(|task_cx| {
        // Once the actor publishes a terminal open result, that publication is
        // authoritative for the publication-vs-cancellation ordering. If
        // cancellation was forwarded on an earlier turn, drain this one
        // response before reporting Interrupt so the caller can disconnect
        // the mailbox and join deterministic worker cleanup.
        match response.as_mut().poll(task_cx) {
            Poll::Ready(Ok(result)) => {
                cancellation_source.disarm();
                return Poll::Ready(if cancellation_forwarded {
                    Err(FrankenError::Interrupt)
                } else {
                    result.map_err(|error| {
                        normalize_operation_error(Some(&cancellation_observation), error)
                    })
                });
            }
            Poll::Ready(Err(_)) => {
                cancellation_source.disarm();
                return Poll::Ready(Err(
                    if cancellation_forwarded
                        || cancellation_observation.cancellation_was_observed()
                    {
                        FrankenError::Interrupt
                    } else {
                        worker_open_err()
                    },
                ));
            }
            Poll::Pending => {}
        }
        if !cancellation_forwarded {
            let project_cancelled = project_wait_cx.checkpoint().is_err()
                || matches!(project_cancel.as_mut().poll(task_cx), Poll::Ready(Err(_)));
            if project_cancelled {
                cancellation_source.cancel_from_cx(&project_wait_cx);
                cancellation_forwarded = true;
            } else {
                let native_cancelled = polling_native_cx.checkpoint().is_err()
                    || matches!(polling_cancel.as_mut().poll(task_cx), Poll::Ready(Err(_)));
                if native_cancelled {
                    cancellation_source.cancel_from_native_cx(&polling_native_cx);
                    cancellation_forwarded = true;
                }
            }
        }
        Poll::Pending
    })
    .await
}

/// Select the canonical process-shared runtime for ordinary actor opens.
///
/// Caller project cancellation and the ambient native task context govern the
/// public open future, but neither becomes the database runtime identity. This
/// preserves the same `(path, runtime_id)` MVCC topology as raw
/// `Connection::open` and sibling `AsyncConnection`s.
fn default_worker_env<Caps>(cx: &Cx<Caps>) -> Result<ConnectionEnv, FrankenError>
where
    Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
{
    checkpoint_or_interrupt(cx)?;
    native_cx_for_polling_task()?;
    Ok(ConnectionEnv::default())
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

/// Map bounded actor-admission failures to public cancellation/worker errors.
fn send_err<T>(error: async_mpsc::SendError<T>) -> FrankenError {
    match error {
        async_mpsc::SendError::Cancelled(_) => FrankenError::Interrupt,
        async_mpsc::SendError::Disconnected(_) | async_mpsc::SendError::Full(_) => {
            worker_dead_err()
        }
    }
}

// ---------------------------------------------------------------------------
// AsyncConnection
// ---------------------------------------------------------------------------

/// Async-native wrapper around [`Connection`] for use with asupersync's `Cx`
/// capability context.
///
/// All methods accept a `&Cx` and call `cx.checkpoint()` before dispatching,
/// providing structural cancel-correctness. If the context is cancelled, the
/// method returns `FrankenError::Interrupt` immediately without touching the
/// underlying connection.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsyncConnectionState {
    Open,
    Closing,
    Closed,
}

struct StreamCallbackGuard<'a> {
    active: &'a AtomicBool,
}

impl<'a> StreamCallbackGuard<'a> {
    fn enter(active: &'a AtomicBool) -> Self {
        let was_active = active.swap(true, Ordering::AcqRel);
        debug_assert!(
            !was_active,
            "stream callback guard entered recursively without admission check"
        );
        Self { active }
    }
}

impl Drop for StreamCallbackGuard<'_> {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

pub struct AsyncConnection {
    cmd_tx: Option<CommandSender>,
    worker: Option<WorkerHandle>,
    /// Retained across a dropped `close()` future after Close admission. A
    /// later close call observes this exact terminal result rather than
    /// enqueueing a second close or losing a committed cleanup outcome.
    close_response: Option<ResponseReceiver<Result<(), FrankenError>>>,
    /// Retained while joining an actor that died before resolving Close. This
    /// survives a dropped close future exactly like `close_response`.
    close_terminal_error: Option<FrankenError>,
    state: AsyncConnectionState,
    /// The actor publishes this after every command response. It therefore
    /// reflects raw SQL transaction commands as well as public wrappers.
    in_txn: Arc<AtomicBool>,
    /// Set only while the synchronous streaming API is invoking its callback
    /// on the caller thread. Re-entering this same actor would deadlock behind
    /// its bounded row acknowledgement, so all same-connection admissions fail
    /// promptly with `SQLITE_BUSY` during that window.
    stream_callback_active: AtomicBool,
    #[cfg(test)]
    runtime_id: u64,
}

impl AsyncConnection {
    /// Open a connection in the canonical process-shared runtime. The caller's
    /// project Cx and ambient native task context govern this open operation
    /// without becoming the database runtime identity or crossing to the actor.
    pub async fn open<Caps>(cx: &Cx<Caps>, path: impl Into<String>) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let env = default_worker_env(cx)?;
        Self::open_request_async(
            cx,
            OpenRequest::Create {
                path: path.into(),
                env,
            },
        )
        .await
    }

    /// Open an existing file-backed connection for reading and writing.
    pub async fn open_existing<Caps>(
        cx: &Cx<Caps>,
        path: impl Into<String>,
    ) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let env = default_worker_env(cx)?;
        Self::open_request_async(
            cx,
            OpenRequest::Existing {
                path: path.into(),
                env,
            },
        )
        .await
    }

    /// Open an existing file-backed connection in read-only mode.
    pub async fn open_read_only<Caps>(
        cx: &Cx<Caps>,
        path: impl Into<String>,
    ) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let env = default_worker_env(cx)?;
        Self::open_request_async(
            cx,
            OpenRequest::ReadOnly {
                path: path.into(),
                env,
            },
        )
        .await
    }

    /// Open with SQLite-compatible flags.
    pub async fn open_with_flags<Caps>(
        cx: &Cx<Caps>,
        path: impl Into<String>,
        flags: crate::compat::OpenFlags,
    ) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let env = default_worker_env(cx)?;
        Self::open_request_async(
            cx,
            OpenRequest::Flags {
                path: path.into(),
                flags,
                env,
            },
        )
        .await
    }

    /// Open a database connection without a capability context.
    pub fn open_sync(path: impl Into<String>) -> Result<Self, FrankenError> {
        Self::open_sync_with_env(path, ConnectionEnv::default())
    }

    pub fn open_existing_sync(path: impl Into<String>) -> Result<Self, FrankenError> {
        Self::open_existing_sync_with_env(path, ConnectionEnv::default())
    }

    pub fn open_read_only_sync(path: impl Into<String>) -> Result<Self, FrankenError> {
        Self::open_read_only_sync_with_env(path, ConnectionEnv::default())
    }

    pub fn open_sync_with_flags(
        path: impl Into<String>,
        flags: crate::compat::OpenFlags,
    ) -> Result<Self, FrankenError> {
        Self::open_sync_with_flags_and_env(path, flags, ConnectionEnv::default())
    }

    pub fn open_sync_with_flags_and_env(
        path: impl Into<String>,
        flags: crate::compat::OpenFlags,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError> {
        Self::open_request_sync(OpenRequest::Flags {
            path: path.into(),
            flags,
            env,
        })
    }

    /// Open a connection without a project Cx using an authoritative explicit
    /// environment. The supplied runtime lineage is left unchanged.
    pub fn open_sync_with_env(
        path: impl Into<String>,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError> {
        Self::open_request_sync(OpenRequest::Create {
            path: path.into(),
            env,
        })
    }

    pub fn open_existing_sync_with_env(
        path: impl Into<String>,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError> {
        Self::open_request_sync(OpenRequest::Existing {
            path: path.into(),
            env,
        })
    }

    pub fn open_read_only_sync_with_env(
        path: impl Into<String>,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError> {
        Self::open_request_sync(OpenRequest::ReadOnly {
            path: path.into(),
            env,
        })
    }

    /// Open with an authoritative explicit environment. `cx` contributes
    /// cancellation and budget constraints for the open operation without
    /// replacing the environment's runtime lineage.
    pub async fn open_with_env<Caps>(
        cx: &Cx<Caps>,
        path: impl Into<String>,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        Self::open_request_async(
            cx,
            OpenRequest::Create {
                path: path.into(),
                env,
            },
        )
        .await
    }

    pub async fn open_existing_with_env<Caps>(
        cx: &Cx<Caps>,
        path: impl Into<String>,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        Self::open_request_async(
            cx,
            OpenRequest::Existing {
                path: path.into(),
                env,
            },
        )
        .await
    }

    pub async fn open_read_only_with_env<Caps>(
        cx: &Cx<Caps>,
        path: impl Into<String>,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        Self::open_request_async(
            cx,
            OpenRequest::ReadOnly {
                path: path.into(),
                env,
            },
        )
        .await
    }

    pub async fn open_with_flags_and_env<Caps>(
        cx: &Cx<Caps>,
        path: impl Into<String>,
        flags: crate::compat::OpenFlags,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        Self::open_request_async(
            cx,
            OpenRequest::Flags {
                path: path.into(),
                flags,
                env,
            },
        )
        .await
    }

    fn open_request_sync(request: OpenRequest) -> Result<Self, FrankenError> {
        if NativeCx::current().is_some() {
            return Err(sync_on_runtime_err("open_sync"));
        }
        request.ensure_actor_compatible()?;
        #[cfg(test)]
        let runtime_id = request.runtime_id();
        let (cmd_tx, worker, in_txn, open_rx) = start_worker(request)?;
        match wait_for_worker_open(open_rx) {
            Ok(()) => Ok(Self {
                cmd_tx: Some(cmd_tx),
                worker: Some(worker),
                close_response: None,
                close_terminal_error: None,
                state: AsyncConnectionState::Open,
                in_txn,
                stream_callback_active: AtomicBool::new(false),
                #[cfg(test)]
                runtime_id,
            }),
            Err(error) => {
                drop(cmd_tx);
                worker.wait_sync();
                Err(error)
            }
        }
    }

    async fn open_request_async<Caps>(
        cx: &Cx<Caps>,
        request: OpenRequest,
    ) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        if let Err(error) = checkpoint_or_interrupt(cx) {
            return Err(error);
        }
        if let Err(error) = native_cx_for_polling_task() {
            return Err(error);
        }
        if let Err(error) = request.ensure_actor_compatible() {
            return Err(error);
        }
        #[cfg(test)]
        let runtime_id = request.runtime_id();
        let (cancellation_source, cancellation_token) = cx.operation_cancellation();
        let cancellation_observation = cancellation_token.clone();
        let request = request.with_operation_cancellation(cancellation_token);
        let (cmd_tx, mut worker, in_txn, open_rx) = match start_worker(request) {
            Ok(started) => started,
            Err(error) => return Err(error),
        };
        match wait_for_worker_open_async(cx, open_rx, cancellation_source, cancellation_observation)
            .await
        {
            Ok(()) => Ok(Self {
                cmd_tx: Some(cmd_tx),
                worker: Some(worker),
                close_response: None,
                close_terminal_error: None,
                state: AsyncConnectionState::Open,
                in_txn,
                stream_callback_active: AtomicBool::new(false),
                #[cfg(test)]
                runtime_id,
            }),
            Err(error) => {
                drop(cmd_tx);
                worker.wait_async_ref().await;
                worker.join_async_ref().await;
                Err(error)
            }
        }
    }

    /// Return a reference to the command sender while this connection remains
    /// in the explicitly open state.
    fn sender(&self) -> Result<&CommandSender, FrankenError> {
        if self.stream_callback_active.load(Ordering::Acquire) {
            return Err(FrankenError::Busy);
        }
        if self.state != AsyncConnectionState::Open {
            return Err(FrankenError::Internal(
                "AsyncConnection has been closed".to_owned(),
            ));
        }
        self.cmd_tx
            .as_ref()
            .ok_or_else(|| FrankenError::Internal("async worker sender is unavailable".to_owned()))
    }

    fn request_sync<T>(
        &self,
        build: impl FnOnce(Responder<T>) -> Command,
    ) -> Result<T, FrankenError> {
        let (tx, rx) = response_channel();
        self.sender()?.send_sync(move || build(tx))?;
        recv_worker_response(rx)
    }

    async fn request_async<Caps, T>(
        &self,
        cx: &Cx<Caps>,
        build: impl FnOnce(Responder<T>) -> Command,
    ) -> Result<T, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let (tx, rx) = response_channel();
        let (cancellation_source, cancellation_token) = cx.operation_cancellation();
        let polling_native_cx = self
            .sender()?
            .send_async(cx, Some(cancellation_token), move || build(tx))
            .await?;
        // Admission is the linearization point. Cancellation now signals only
        // this engine operation; the caller drains the actor's one terminal
        // response, and dropping this future cancels via the armed source.
        recv_authoritative_worker_response(rx, cancellation_source, polling_native_cx).await
    }

    /// Validate and prepare one SQL statement on the dedicated worker.
    pub fn prepare_sync(&self, sql: &str) -> Result<(), FrankenError> {
        self.request_sync(|tx| Command::Prepare {
            sql: sql.to_owned(),
            tx,
        })
    }

    /// Execute a query through the dedicated worker and block for all rows.
    pub fn query_sync(&self, sql: &str) -> Result<Vec<Row>, FrankenError> {
        self.request_sync(|tx| Command::Query {
            sql: sql.to_owned(),
            tx,
        })
    }

    /// Execute a parameterized query through the dedicated worker.
    pub fn query_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, FrankenError> {
        self.request_sync(|tx| Command::QueryWithParams {
            sql: sql.to_owned(),
            params: params.to_vec(),
            tx,
        })
    }

    /// Stream a parameterized query through a one-row bounded worker channel.
    ///
    /// The callback runs on the caller thread. Its bounded acknowledgement is
    /// deliberate backpressure for the synchronous API: the worker never
    /// accumulates an unbounded result set while a consumer is slow. Returning
    /// an error drops the receiver, releases the worker, and returns that
    /// callback error.
    pub fn query_with_params_for_each_sync<F>(
        &self,
        sql: &str,
        params: &[SqliteValue],
        mut f: F,
    ) -> Result<(), FrankenError>
    where
        F: FnMut(&Row) -> Result<(), FrankenError>,
    {
        let (tx, rx) = sync_mpsc::sync_channel(1);
        self.sender()?
            .send_sync(move || Command::QueryWithParamsStream {
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })?;
        loop {
            match rx.recv().map_err(|_| worker_dead_err())?? {
                Some(row) => {
                    let _callback = StreamCallbackGuard::enter(&self.stream_callback_active);
                    f(&row)?;
                }
                None => return Ok(()),
            }
        }
    }

    /// Execute a query through the dedicated worker and return exactly one row.
    pub fn query_row_sync(&self, sql: &str) -> Result<Row, FrankenError> {
        self.request_sync(|tx| Command::QueryRow {
            sql: sql.to_owned(),
            tx,
        })
    }

    /// Execute a parameterized query and return exactly one row.
    pub fn query_row_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Row, FrankenError> {
        self.request_sync(|tx| Command::QueryRowWithParams {
            sql: sql.to_owned(),
            params: params.to_vec(),
            tx,
        })
    }

    /// Execute SQL through the dedicated worker.
    pub fn execute_sync(&self, sql: &str) -> Result<usize, FrankenError> {
        self.request_sync(|tx| Command::Execute {
            sql: sql.to_owned(),
            tx,
        })
    }

    /// Execute parameterized SQL through the dedicated worker.
    pub fn execute_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        self.request_sync(|tx| Command::ExecuteWithParams {
            sql: sql.to_owned(),
            params: params.to_vec(),
            tx,
        })
    }

    /// Execute parameter sets as one worker command inside an explicit transaction.
    pub fn execute_many_with_params_in_transaction_sync(
        &self,
        sql: &str,
        parameter_sets: &[Vec<SqliteValue>],
    ) -> Result<usize, FrankenError> {
        self.request_sync(|tx| Command::ExecuteManyWithParamsInTransaction {
            sql: sql.to_owned(),
            parameter_sets: parameter_sets.to_vec(),
            tx,
        })
    }

    /// Execute zero or more SQL statements through the dedicated worker.
    pub fn execute_batch_sync(&self, sql: &str) -> Result<(), FrankenError> {
        self.request_sync(|tx| Command::ExecuteBatch {
            sql: sql.to_owned(),
            tx,
        })
    }

    /// Begin a transaction through the dedicated worker.
    pub fn begin_transaction_sync(&self) -> Result<(), FrankenError> {
        self.request_sync(|tx| Command::BeginTransaction { tx })
    }

    /// Commit the active transaction through the dedicated worker.
    pub fn commit_transaction_sync(&self) -> Result<(), FrankenError> {
        self.request_sync(|tx| Command::CommitTransaction { tx })
    }

    /// Roll back the active transaction through the dedicated worker.
    pub fn rollback_transaction_sync(&self) -> Result<(), FrankenError> {
        self.request_sync(|tx| Command::RollbackTransaction { tx })
    }

    /// Return the worker-owned connection's last inserted row identifier.
    pub fn last_insert_rowid_sync(&self) -> Result<i64, FrankenError> {
        self.request_sync(|tx| Command::LastInsertRowid { tx })
    }

    /// Execute a SQL query and return all result rows.
    pub async fn query<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<Vec<Row>, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.request_async(cx, |tx| Command::Query {
            sql: sql.to_owned(),
            tx,
        })
        .await
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
    {
        self.request_async(cx, |tx| Command::QueryWithParams {
            sql: sql.to_owned(),
            params: params.to_vec(),
            tx,
        })
        .await
    }

    /// Execute a query and return exactly one row.
    pub async fn query_row<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<Row, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.request_async(cx, |tx| Command::QueryRow {
            sql: sql.to_owned(),
            tx,
        })
        .await
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
    {
        self.request_async(cx, |tx| Command::QueryRowWithParams {
            sql: sql.to_owned(),
            params: params.to_vec(),
            tx,
        })
        .await
    }

    /// Execute SQL and return the number of affected/output rows.
    pub async fn execute<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<usize, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.request_async(cx, |tx| Command::Execute {
            sql: sql.to_owned(),
            tx,
        })
        .await
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
    {
        self.request_async(cx, |tx| Command::ExecuteWithParams {
            sql: sql.to_owned(),
            params: params.to_vec(),
            tx,
        })
        .await
    }

    /// Execute zero or more SQL statements separated by semicolons.
    pub async fn execute_batch<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.request_async(cx, |tx| Command::ExecuteBatch {
            sql: sql.to_owned(),
            tx,
        })
        .await
    }

    /// Begin a transaction.
    pub async fn begin_transaction<Caps>(&self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.request_async(cx, |tx| Command::BeginTransaction { tx })
            .await
    }

    /// Commit the active transaction.
    pub async fn commit_transaction<Caps>(&self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.request_async(cx, |tx| Command::CommitTransaction { tx })
            .await
    }

    /// Roll back the active transaction.
    pub async fn rollback_transaction<Caps>(&self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.request_async(cx, |tx| Command::RollbackTransaction { tx })
            .await
    }

    /// Returns `true` if an explicit transaction is currently active.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.in_txn.load(Ordering::Acquire)
    }

    async fn finish_close_async(&mut self) -> Result<(), FrankenError> {
        if self.close_response.is_some() {
            let response = std::future::poll_fn(|task_cx| {
                let response = self
                    .close_response
                    .as_mut()
                    .expect("closing state must retain its admitted response");
                Pin::new(response).poll(task_cx)
            })
            .await;
            drop(self.close_response.take());
            match response {
                Ok(Ok(())) => {
                    // Successful Close makes actor termination authoritative.
                    // Disconnect any redundant producer handle before joining.
                    drop(self.cmd_tx.take());
                }
                Ok(Err(error)) => {
                    // The worker deliberately remains alive after a failed
                    // engine close. Restore Open so the exact same handle can
                    // retry instead of being stranded in Closing.
                    self.state = AsyncConnectionState::Open;
                    return Err(error);
                }
                Err(_) => {
                    drop(self.cmd_tx.take());
                    self.close_terminal_error = Some(worker_dead_err());
                }
            }
        }

        if let Some(worker) = self.worker.as_ref() {
            worker.wait_async_ref().await;
        }
        if let Some(worker) = self.worker.as_mut() {
            worker.join_async_ref().await;
        }
        drop(self.worker.take());

        self.state = AsyncConnectionState::Closed;
        match self.close_terminal_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn finish_close_sync(&mut self) -> Result<(), FrankenError> {
        if NativeCx::current().is_some() {
            return Err(sync_on_runtime_err("close_sync"));
        }
        if let Some(response) = self.close_response.take() {
            match response.recv_blocking() {
                Ok(Ok(())) => drop(self.cmd_tx.take()),
                Ok(Err(error)) => {
                    // A resolved engine close failure is retryable. A worker
                    // remains alive and no hidden teardown retry is triggered.
                    self.state = AsyncConnectionState::Open;
                    return Err(error);
                }
                Err(_) => {
                    drop(self.cmd_tx.take());
                    self.close_terminal_error = Some(worker_dead_err());
                }
            }
        }

        if let Some(worker) = self.worker.take() {
            worker.wait_sync();
        }
        self.state = AsyncConnectionState::Closed;
        match self.close_terminal_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Explicitly close the connection. Once Close is admitted, caller
    /// cancellation cannot alter the committed cleanup outcome; a later call
    /// observes the same terminal response if this future is dropped.
    pub async fn close<Caps>(&mut self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        match self.state {
            AsyncConnectionState::Closed => return Ok(()),
            AsyncConnectionState::Open => {
                checkpoint_or_interrupt(cx)?;
                drop(self.close_response.take());
                let (tx, rx) = response_channel();
                match self
                    .sender()?
                    .send_async(cx, None, move || Command::Close { tx })
                    .await
                {
                    Ok(_) => {
                        self.close_response = Some(rx);
                        self.state = AsyncConnectionState::Closing;
                    }
                    Err(FrankenError::Interrupt) => {
                        drop(rx);
                        return Err(FrankenError::Interrupt);
                    }
                    Err(error) => {
                        drop(rx);
                        self.close_terminal_error = Some(error);
                        self.state = AsyncConnectionState::Closing;
                        drop(self.cmd_tx.take());
                    }
                }
            }
            AsyncConnectionState::Closing => {}
        }
        self.finish_close_async().await
    }

    /// Explicitly close a synchronously used connection and join its worker.
    pub fn close_sync(&mut self) -> Result<(), FrankenError> {
        match self.state {
            AsyncConnectionState::Closed => return Ok(()),
            AsyncConnectionState::Open => {
                if NativeCx::current().is_some() {
                    return Err(sync_on_runtime_err("close_sync"));
                }
                drop(self.close_response.take());
                let (tx, rx) = response_channel();
                match self.sender()?.send_sync(move || Command::Close { tx }) {
                    Ok(()) => {
                        self.close_response = Some(rx);
                        self.state = AsyncConnectionState::Closing;
                    }
                    Err(error) => {
                        drop(rx);
                        self.close_terminal_error = Some(error);
                        self.state = AsyncConnectionState::Closing;
                        drop(self.cmd_tx.take());
                    }
                }
            }
            AsyncConnectionState::Closing => {}
        }
        self.finish_close_sync()
    }
}

impl Drop for AsyncConnection {
    fn drop(&mut self) {
        // Drop is deliberately signal-only. Disconnecting the sender wakes
        // the actor, while dropping JoinHandle detaches rather than waiting.
        // The actor owns all engine cleanup.
        drop(self.cmd_tx.take());
        drop(self.worker.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::cx::Cx as NativeCx;
    use asupersync::runtime::{Runtime, RuntimeBuilder};
    use fsqlite_types::cx::Cx;
    use std::task::Wake;

    fn test_runtime() -> Runtime {
        RuntimeBuilder::current_thread()
            .blocking_threads(2, 2)
            .build()
            .expect("test runtime should build")
    }

    fn env_with_attached_native_cx(native_cx: NativeCx) -> ConnectionEnv {
        let root_cx = Cx::new();
        root_cx.set_native_cx(native_cx);
        ConnectionEnv::new(Arc::new(RuntimeContext::new_with_root_cx(
            RuntimeConfig::default(),
            &root_cx,
        )))
    }

    async fn enqueue_actor_block(
        conn: &AsyncConnection,
        cx: &Cx,
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> ResponseReceiver<Result<(), FrankenError>> {
        let (tx, rx) = response_channel();
        conn.sender()
            .expect("open connection must retain a sender")
            .send_async(cx, None, move || Command::TestBlockActor {
                entered,
                release,
                tx,
            })
            .await
            .expect("test block command must be admitted");
        rx
    }

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}

        fn wake_by_ref(self: &Arc<Self>) {}
    }

    /// Records whether a registered waker was destroyed after the lock that
    /// owned its slot was released. `RawWaker` clone/drop callbacks are
    /// user-defined and can re-enter these primitives.
    struct LockProbeWake {
        on_drop: Box<dyn Fn() + Send + Sync>,
    }

    impl Wake for LockProbeWake {
        fn wake(self: Arc<Self>) {}

        fn wake_by_ref(self: &Arc<Self>) {}
    }

    impl Drop for LockProbeWake {
        fn drop(&mut self) {
            (self.on_drop)();
        }
    }

    fn lock_probe_waker(on_drop: impl Fn() + Send + Sync + 'static) -> Waker {
        Waker::from(Arc::new(LockProbeWake {
            on_drop: Box::new(on_drop),
        }))
    }

    fn poll_once<F>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output>
    where
        F: Future,
    {
        let mut task_cx = Context::from_waker(waker);
        future.poll(&mut task_cx)
    }

    #[test]
    fn test_response_waker_replacement_drops_after_status_unlock() {
        let (_sender, mut response) = response_channel::<u8>();
        let state = Arc::downgrade(&response.state);
        let lock_was_available = Arc::new(AtomicBool::new(false));
        let lock_was_available_on_drop = Arc::clone(&lock_was_available);
        let first_waker = lock_probe_waker(move || {
            if state
                .upgrade()
                .is_some_and(|state| state.status.try_lock().is_ok())
            {
                lock_was_available_on_drop.store(true, Ordering::Release);
            }
        });
        assert!(matches!(
            poll_once(Pin::new(&mut response), &first_waker),
            Poll::Pending
        ));
        drop(first_waker);

        let replacement = Waker::from(Arc::new(NoopWake));
        assert!(matches!(
            poll_once(Pin::new(&mut response), &replacement),
            Poll::Pending
        ));
        assert!(
            lock_was_available.load(Ordering::Acquire),
            "replaced response waker must be destroyed after status unlock"
        );
    }

    #[test]
    fn test_response_terminalization_drops_registered_waker_after_status_unlock() {
        let (sender, mut response) = response_channel::<u8>();
        let state = Arc::downgrade(&response.state);
        let lock_was_available = Arc::new(AtomicBool::new(false));
        let lock_was_available_on_drop = Arc::clone(&lock_was_available);
        let registered_waker = lock_probe_waker(move || {
            if state
                .upgrade()
                .is_some_and(|state| state.status.try_lock().is_ok())
            {
                lock_was_available_on_drop.store(true, Ordering::Release);
            }
        });
        assert!(matches!(
            poll_once(Pin::new(&mut response), &registered_waker),
            Poll::Pending
        ));
        drop(registered_waker);
        drop(sender);

        assert!(
            lock_was_available.load(Ordering::Acquire),
            "terminal response waker must be destroyed after status unlock"
        );
        let replacement = Waker::from(Arc::new(NoopWake));
        assert!(matches!(
            poll_once(Pin::new(&mut response), &replacement),
            Poll::Ready(Err(ResponseDisconnected))
        ));
    }

    #[test]
    fn test_worker_exit_waker_replacement_drops_after_waiter_unlock() {
        let lifecycle = Arc::new(WorkerLifecycle::new(Arc::new(CommandCapacitySignal::new())));
        let lifecycle_weak = Arc::downgrade(&lifecycle);
        let lock_was_available = Arc::new(AtomicBool::new(false));
        let lock_was_available_on_drop = Arc::clone(&lock_was_available);
        let first_waker = lock_probe_waker(move || {
            if lifecycle_weak
                .upgrade()
                .is_some_and(|lifecycle| lifecycle.waiter.try_lock().is_ok())
            {
                lock_was_available_on_drop.store(true, Ordering::Release);
            }
        });
        let mut exit = WorkerExit {
            lifecycle: lifecycle.as_ref(),
        };
        assert!(matches!(
            poll_once(Pin::new(&mut exit), &first_waker),
            Poll::Pending
        ));
        drop(first_waker);

        let replacement = Waker::from(Arc::new(NoopWake));
        assert!(matches!(
            poll_once(Pin::new(&mut exit), &replacement),
            Poll::Pending
        ));
        assert!(
            lock_was_available.load(Ordering::Acquire),
            "replaced worker-exit waker must be destroyed after waiter unlock"
        );
    }

    #[test]
    fn test_worker_exit_terminalization_drops_registered_waker_after_waiter_unlock() {
        let lifecycle = Arc::new(WorkerLifecycle::new(Arc::new(CommandCapacitySignal::new())));
        let lifecycle_weak = Arc::downgrade(&lifecycle);
        let lock_was_available = Arc::new(AtomicBool::new(false));
        let lock_was_available_on_drop = Arc::clone(&lock_was_available);
        let registered_waker = lock_probe_waker(move || {
            if lifecycle_weak
                .upgrade()
                .is_some_and(|lifecycle| lifecycle.waiter.try_lock().is_ok())
            {
                lock_was_available_on_drop.store(true, Ordering::Release);
            }
        });
        let mut exit = WorkerExit {
            lifecycle: lifecycle.as_ref(),
        };
        assert!(matches!(
            poll_once(Pin::new(&mut exit), &registered_waker),
            Poll::Pending
        ));
        drop(registered_waker);
        lifecycle.finish();

        assert!(
            lock_was_available.load(Ordering::Acquire),
            "terminal worker-exit waker must be destroyed after waiter unlock"
        );
        let replacement = Waker::from(Arc::new(NoopWake));
        assert!(matches!(
            poll_once(Pin::new(&mut exit), &replacement),
            Poll::Ready(())
        ));
    }

    #[test]
    fn test_authoritative_worker_publication_wins_same_turn_cancellation() {
        test_runtime().block_on(async {
            let caller = Cx::new();
            let (source, _token) = caller.operation_cancellation();
            let (tx, rx) = response_channel();
            tx.send(Ok::<u64, FrankenError>(41));

            // The result is already published when cancellation becomes
            // visible. The response lane is polled first and must remain the
            // sole terminal outcome.
            caller.cancel();
            let polling_native_cx =
                NativeCx::current().expect("test must run inside the native runtime");
            assert_eq!(
                recv_authoritative_worker_response(rx, source, polling_native_cx).await,
                Ok(41)
            );
        });
    }

    #[test]
    fn test_abort_normalization_requires_exact_checkpoint_observation() {
        let caller = Cx::new();
        let (source, token) = caller.operation_cancellation();

        source.cancel();
        assert!(matches!(
            normalize_operation_error(Some(&token), FrankenError::Abort),
            FrankenError::Abort
        ));

        assert!(token.checkpoint().is_err());
        assert!(matches!(
            normalize_operation_error(Some(&token), FrankenError::Abort),
            FrankenError::Interrupt
        ));
        assert!(matches!(
            normalize_operation_error(
                Some(&token),
                FrankenError::Internal("unrelated failure".to_owned())
            ),
            FrankenError::Internal(detail) if detail == "unrelated failure"
        ));
    }

    #[test]
    fn test_default_async_opens_accept_capability_empty_contexts() {
        let full = Cx::new();
        let restricted = full.restrict::<fsqlite_types::cx::cap::None>();
        let _ = AsyncConnection::open(&restricted, ":memory:");
        let _ = AsyncConnection::open_existing(&restricted, "existing.db");
        let _ = AsyncConnection::open_read_only(&restricted, "readonly.db");
        let _ = AsyncConnection::open_with_flags(
            &restricted,
            ":memory:",
            crate::compat::OpenFlags::SQLITE_OPEN_READ_WRITE
                | crate::compat::OpenFlags::SQLITE_OPEN_CREATE,
        );
    }

    #[test]
    fn test_open_publication_wins_same_turn_cancellation() {
        test_runtime().block_on(async {
            let caller = Cx::new();
            let (source, token) = caller.operation_cancellation();
            let (tx, rx) = response_channel();
            tx.send(Ok(()));

            caller.cancel();
            assert_eq!(
                wait_for_worker_open_async(&caller, rx, source, token).await,
                Ok(()),
                "an already-published open result must win the same-turn cancellation race"
            );
        });
    }

    #[test]
    fn test_full_mailbox_cancellation_does_not_require_capacity_to_free() {
        test_runtime().block_on(async {
            let (tx, _rx) = async_mpsc::channel(COMMAND_CAPACITY);
            let sender = CommandSender {
                tx: Some(tx),
                worker_thread: thread::current(),
                command_capacity: Arc::new(CommandCapacitySignal::new()),
            };

            for _ in 0..COMMAND_CAPACITY {
                let (response_tx, _response_rx) = response_channel();
                let permit = sender
                    .tx()
                    .expect("sender must remain open")
                    .try_reserve()
                    .expect("test setup must fill exactly the bounded mailbox");
                assert!(
                    permit
                        .try_send(CommandEnvelope {
                            cancellation: None,
                            command: Command::TestActorContext { tx: response_tx },
                        })
                        .is_ok(),
                    "test setup must retain every reserved mailbox slot"
                );
            }

            let caller = Cx::new();
            let (response_tx, _response_rx) = response_channel();
            let mut send =
                Box::pin(
                    sender.send_async(&caller, None, move || Command::TestActorContext {
                        tx: response_tx,
                    }),
                );
            assert!(
                future::poll_once(send.as_mut()).await.is_none(),
                "the next admission must wait while capacity never frees"
            );

            caller.cancel();
            assert!(matches!(send.await, Err(FrankenError::Interrupt)));
        });
    }

    #[test]
    fn test_open_cancellation_disconnects_and_joins_blocked_worker_cleanup() {
        test_runtime().block_on(async {
            let caller = Cx::new();
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (observed_tx, observed_rx) = std::sync::mpsc::channel();
            let request = OpenRequest::TestBlocked {
                env: ConnectionEnv::default(),
                entered: entered_tx,
                release: release_rx,
                observed_engine_cancellation: observed_tx,
            };
            let mut open = Box::pin(AsyncConnection::open_request_async(&caller, request));

            assert!(
                future::poll_once(open.as_mut()).await.is_none(),
                "blocked open must remain pending"
            );
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("the worker must enter the deterministic open barrier");

            caller.cancel();
            assert!(
                future::poll_once(open.as_mut()).await.is_none(),
                "cancellation must wait for worker cleanup rather than detach it"
            );
            release_tx
                .send(())
                .expect("the blocked worker must remain joinable");
            assert!(matches!(open.await, Err(FrankenError::Interrupt)));
            assert!(
                observed_rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .expect("the worker must report its linked open outcome"),
                "caller cancellation must reach the root-derived open context"
            );
        });
    }

    #[test]
    fn test_open_cancellation_before_worker_publication_wins_without_intermediate_poll() {
        test_runtime().block_on(async {
            let caller = Cx::new();
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (observed_tx, observed_rx) = std::sync::mpsc::channel();
            let request = OpenRequest::TestBlocked {
                env: ConnectionEnv::default(),
                entered: entered_tx,
                release: release_rx,
                observed_engine_cancellation: observed_tx,
            };
            let mut open = Box::pin(AsyncConnection::open_request_async(&caller, request));

            assert!(
                future::poll_once(open.as_mut()).await.is_none(),
                "blocked open must remain pending"
            );
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("the worker must enter the deterministic open barrier");

            // Do not poll `open` between publishing cancellation and allowing
            // the worker to finish. The engine's exact operation token, rather
            // than public-future polling order, must prove that cancellation
            // preceded the worker's terminal Abort.
            caller.cancel();
            release_tx
                .send(())
                .expect("the blocked worker must remain joinable");

            assert!(matches!(open.await, Err(FrankenError::Interrupt)));
            assert!(
                observed_rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .expect("the worker must report its linked open outcome"),
                "the worker must observe cancellation before publishing its open result"
            );
        });
    }

    #[test]
    fn test_dropped_open_future_cancels_engine_open_and_worker_exits() {
        test_runtime().block_on(async {
            let caller = Cx::new();
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (observed_tx, observed_rx) = std::sync::mpsc::channel();
            let request = OpenRequest::TestBlocked {
                env: ConnectionEnv::default(),
                entered: entered_tx,
                release: release_rx,
                observed_engine_cancellation: observed_tx,
            };
            let mut open = Box::pin(AsyncConnection::open_request_async(&caller, request));

            assert!(
                future::poll_once(open.as_mut()).await.is_none(),
                "blocked open must remain pending"
            );
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("the worker must enter the deterministic open barrier");

            // Dropping the admitted public future drops its armed cancellation
            // source and its command sender. The source must interrupt engine
            // open; sender disconnect then lets any later-opened connection
            // close on the worker rather than leak detached state.
            drop(open);
            release_tx
                .send(())
                .expect("the blocked worker must remain releasable after future Drop");
            assert!(
                observed_rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .expect("the worker must complete after dropped-open cancellation"),
                "dropping open must reach the root-derived engine open context"
            );
        });
    }

    #[test]
    fn test_dropped_admitted_future_cancels_queued_actor_effect() {
        test_runtime().block_on(async {
            let caller = Cx::new();
            let mut conn = AsyncConnection::open(&caller, ":memory:")
                .await
                .expect("open should succeed");
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = enqueue_actor_block(&conn, &caller, entered_tx, release_rx).await;
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("actor must enter the deterministic command barrier");

            let operation_cx = caller.create_child();
            let mut operation =
                Box::pin(conn.execute(&operation_cx, "CREATE TABLE dropped_effect (id INTEGER)"));
            assert!(
                future::poll_once(operation.as_mut()).await.is_none(),
                "the operation must be admitted behind the actor barrier"
            );
            drop(operation);

            release_tx.send(()).expect("actor barrier must release");
            assert_eq!(blocker.await, Ok(Ok(())));
            let rows = conn
                .query(
                    &caller,
                    "SELECT name FROM sqlite_master WHERE name = 'dropped_effect'",
                )
                .await
                .expect("schema inspection should succeed");
            assert!(
                rows.is_empty(),
                "dropping an admitted future must cancel its queued engine effect"
            );
            conn.close(&caller).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_cancelled_admitted_future_drains_actor_interrupt() {
        test_runtime().block_on(async {
            let caller = Cx::new();
            let mut conn = AsyncConnection::open(&caller, ":memory:")
                .await
                .expect("open should succeed");
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = enqueue_actor_block(&conn, &caller, entered_tx, release_rx).await;
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("actor must enter the deterministic command barrier");

            let operation_cx = caller.create_child();
            let mut operation =
                Box::pin(conn.execute(&operation_cx, "CREATE TABLE cancelled_effect (id INTEGER)"));
            assert!(
                future::poll_once(operation.as_mut()).await.is_none(),
                "the operation must be admitted behind the actor barrier"
            );
            operation_cx.cancel();
            release_tx.send(()).expect("actor barrier must release");
            assert_eq!(blocker.await, Ok(Ok(())));
            assert!(matches!(operation.await, Err(FrankenError::Interrupt)));

            let rows = conn
                .query(
                    &caller,
                    "SELECT name FROM sqlite_master WHERE name = 'cancelled_effect'",
                )
                .await
                .expect("schema inspection should succeed");
            assert!(
                rows.is_empty(),
                "actor-boundary cancellation must reject the queued engine effect"
            );
            conn.close(&caller).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_default_actor_opens_share_canonical_runtime_topology() {
        test_runtime().block_on(async {
            let directory = tempfile::tempdir().expect("temporary database directory");
            let path = directory.path().join("shared-runtime.db");
            let path = path.to_string_lossy().into_owned();
            let caller = Cx::new();
            let mut first = AsyncConnection::open(&caller, path.clone())
                .await
                .expect("first connection should open");
            first
                .execute(
                    &caller,
                    "CREATE TABLE shared_runtime (id INTEGER PRIMARY KEY)",
                )
                .await
                .expect("schema creation should succeed");
            first
                .execute(&caller, "INSERT INTO shared_runtime VALUES (1)")
                .await
                .expect("insert should succeed");

            let mut second = AsyncConnection::open_existing(&caller, path)
                .await
                .expect("second connection should share the same file topology");
            let global_runtime_id = RuntimeContext::global().runtime_id();
            assert_eq!(first.runtime_id, global_runtime_id);
            assert_eq!(second.runtime_id, global_runtime_id);
            let rows = second
                .query(&caller, "SELECT id FROM shared_runtime")
                .await
                .expect("same-runtime sibling must observe committed state");
            assert_eq!(rows.len(), 1);

            first.close(&caller).await.expect("first close");
            second.close(&caller).await.expect("second close");
        });
    }

    #[test]
    fn test_failed_close_restores_open_state_for_retry() {
        test_runtime().block_on(async {
            let caller = Cx::new();
            let mut conn = AsyncConnection::open(&caller, ":memory:")
                .await
                .expect("open should succeed");
            let (tx, rx) = response_channel();
            conn.sender()
                .expect("open connection must retain its sender")
                .send_async(&caller, None, move || Command::TestFailClose { tx })
                .await
                .expect("injected close command must be admitted");
            conn.close_response = Some(rx);
            conn.state = AsyncConnectionState::Closing;

            let error = conn
                .finish_close_async()
                .await
                .expect_err("injected engine close failure must surface");
            assert!(matches!(
                error,
                FrankenError::Internal(detail) if detail.contains("injected retryable close failure")
            ));
            assert_eq!(conn.state, AsyncConnectionState::Open);
            conn.query(&caller, "SELECT 1")
                .await
                .expect("the same handle must remain usable after close failure");
            conn.close(&caller)
                .await
                .expect("a later close attempt must succeed");
        });
    }

    #[test]
    fn test_stream_callback_reentry_is_busy_but_other_connection_progresses() {
        let mut conn =
            AsyncConnection::open_sync(":memory:").expect("primary connection should open");
        conn.execute_sync("CREATE TABLE stream_rows (id INTEGER PRIMARY KEY)")
            .expect("schema creation");
        conn.execute_sync("INSERT INTO stream_rows VALUES (1)")
            .expect("seed row");
        let mut other =
            AsyncConnection::open_sync(":memory:").expect("independent connection should open");
        let mut callback_ran = false;

        conn.query_with_params_for_each_sync("SELECT id FROM stream_rows", &[], |_| {
            assert!(matches!(
                conn.query_sync("SELECT 1"),
                Err(FrankenError::Busy)
            ));
            other
                .query_sync("SELECT 1")
                .expect("an independent actor must continue to make progress");
            callback_ran = true;
            Ok(())
        })
        .expect("stream should finish after callback");
        assert!(callback_ran);

        conn.close_sync().expect("primary close");
        other.close_sync().expect("independent close");
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
    fn test_async_connection_cancel() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");

            // Cancel the context — subsequent operations should fail.
            cx.cancel();

            let result = conn.execute(&cx, "SELECT 1").await;
            assert!(
                matches!(result, Err(FrankenError::Interrupt)),
                "operation should report Interrupt after cancellation"
            );
        });
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

    #[test]
    fn test_async_connection_explicit_close_waits_for_actor_exit() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let block_response = enqueue_actor_block(&conn, &cx, entered_tx, release_rx).await;
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("actor must begin the test command before close");

            let mut close = Box::pin(conn.close(&cx));
            assert!(
                future::poll_once(close.as_mut()).await.is_none(),
                "explicit close must remain pending while the actor is still blocked"
            );
            release_tx
                .send(())
                .expect("test actor must still be waiting for release");
            assert_eq!(close.await, Ok(()));
            assert_eq!(block_response.await, Ok(Ok(())));
        });
    }

    #[test]
    fn test_dropped_admitted_close_reuses_response_and_closes_once() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = Arc::clone(
                &conn
                    .worker
                    .as_ref()
                    .expect("open connection must own its worker")
                    .lifecycle,
            );
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let block_response = enqueue_actor_block(&conn, &cx, entered_tx, release_rx).await;
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("actor must enter the deterministic command barrier");

            let mut first_close = Box::pin(conn.close(&cx));
            assert!(
                future::poll_once(first_close.as_mut()).await.is_none(),
                "close must be admitted and wait behind the actor barrier"
            );
            drop(first_close);
            assert_eq!(conn.state, AsyncConnectionState::Closing);
            assert!(
                conn.close_response.is_some(),
                "dropping the public future must retain the admitted response"
            );

            release_tx
                .send(())
                .expect("the actor barrier must remain releasable");
            assert_eq!(block_response.await, Ok(Ok(())));
            conn.close(&cx)
                .await
                .expect("a later close must drain the retained response");
            assert_eq!(conn.state, AsyncConnectionState::Closed);
            assert_eq!(
                lifecycle.close_connection_calls.load(Ordering::Acquire),
                1,
                "the admitted Close command must finalize the engine exactly once"
            );
        });
    }

    #[test]
    fn test_async_connection_drop_is_signal_only_while_actor_is_blocked() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let block_response = enqueue_actor_block(&conn, &cx, entered_tx, release_rx).await;
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("actor must begin the test command before Drop");

            let completion = asupersync::test_utils::assert_completes_within(
                std::time::Duration::from_secs(2),
                "AsyncConnection Drop must not join a blocked actor",
                || {
                    Box::pin(async move {
                        drop(conn);
                        release_tx
                            .send(())
                            .expect("signal-only Drop must leave actor waiting");
                        block_response.await
                    })
                },
            )
            .await;
            assert_eq!(completion, Ok(Ok(())));
        });
    }

    #[test]
    fn test_async_connection_actor_never_receives_task_affine_native_cx() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let snapshot = conn
                .request_async(&cx, |tx| Command::TestActorContext { tx })
                .await
                .expect("context inspection command should succeed");

            assert!(!snapshot.task_native_cx_present);
            assert!(!snapshot.root_native_cx_present);
            conn.close(&cx).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_explicit_env_with_attached_native_cx_is_rejected_before_actor_spawn() {
        let sync_error = AsyncConnection::open_sync_with_env(
            ":memory:",
            env_with_attached_native_cx(NativeCx::for_testing()),
        )
        .expect_err("sync actor open must reject an attached native Cx");
        assert!(matches!(
            sync_error,
            FrankenError::Internal(detail)
                if detail.contains("incompatible") && detail.contains("task-affine native Cx")
        ));

        test_runtime().block_on(async {
            let caller_cx = Cx::new();
            let native_cx = NativeCx::current()
                .expect("the async explicit-env probe must run inside a native runtime");
            let async_error = AsyncConnection::open_with_env(
                &caller_cx,
                ":memory:",
                env_with_attached_native_cx(native_cx),
            )
            .await
            .expect_err("async actor open must reject an attached native Cx");
            assert!(matches!(
                async_error,
                FrankenError::Internal(detail)
                    if detail.contains("incompatible") && detail.contains("task-affine native Cx")
            ));
        });
    }

    #[test]
    fn test_explicit_detached_env_opens_on_dedicated_actor() {
        let mut sync_conn =
            AsyncConnection::open_sync_with_env(":memory:", ConnectionEnv::default())
                .expect("a detached explicit env should open synchronously on the actor");
        sync_conn
            .close_sync()
            .expect("synchronous detached-env actor connection should close");

        test_runtime().block_on(async {
            let caller_cx = Cx::new();
            let mut async_conn =
                AsyncConnection::open_with_env(&caller_cx, ":memory:", ConnectionEnv::default())
                    .await
                    .expect("a detached explicit env should open asynchronously on the actor");
            async_conn
                .close(&caller_cx)
                .await
                .expect("asynchronous detached-env actor connection should close");
        });
    }

    #[test]
    fn test_explicit_runtime_constructor_is_actor_safe_only_when_fully_detached() {
        let outside_runtime = Arc::new(RuntimeContext::new(RuntimeConfig::default()));
        assert!(outside_runtime.is_detached_for_dedicated_worker());
        let mut sync_conn =
            AsyncConnection::open_sync_with_env(":memory:", ConnectionEnv::new(outside_runtime))
                .expect("RuntimeContext::new outside a task must be fully detached");
        sync_conn.close_sync().expect("detached actor close");

        let parent = Cx::new();
        let rooted_outside_runtime = Arc::new(RuntimeContext::new_with_root_cx(
            RuntimeConfig::default(),
            &parent,
        ));
        assert!(rooted_outside_runtime.is_detached_for_dedicated_worker());
        let mut rooted_sync_conn = AsyncConnection::open_sync_with_env(
            ":memory:",
            ConnectionEnv::new(rooted_outside_runtime),
        )
        .expect("a native-free explicit root outside a task must be actor-safe");
        rooted_sync_conn
            .close_sync()
            .expect("rooted detached actor close");

        test_runtime().block_on(async {
            let caller = Cx::new();
            assert!(RuntimeContext::global().is_detached_for_dedicated_worker());

            let captured = Arc::new(RuntimeContext::new(RuntimeConfig::default()));
            assert!(!captured.is_detached_for_dedicated_worker());
            let error =
                AsyncConnection::open_with_env(&caller, ":memory:", ConnectionEnv::new(captured))
                    .await
                    .expect_err("an ambient runtime handle must not cross into the actor");
            assert!(matches!(
                error,
                FrankenError::Internal(detail)
                    if detail.contains("not fully detached")
                        && detail.contains("captured native runtime handle")
            ));

            let rooted_captured = Arc::new(RuntimeContext::new_with_root_cx(
                RuntimeConfig::default(),
                &caller,
            ));
            assert!(!rooted_captured.is_detached_for_dedicated_worker());
            let error = AsyncConnection::open_with_env(
                &caller,
                ":memory:",
                ConnectionEnv::new(rooted_captured),
            )
            .await
            .expect_err("a rooted context must still reject its captured runtime handle");
            assert!(matches!(
                error,
                FrankenError::Internal(detail) if detail.contains("not fully detached")
            ));
        });
    }

    #[test]
    fn test_sync_facade_methods_refuse_to_block_a_runtime_task() {
        test_runtime().block_on(async {
            assert!(matches!(
                AsyncConnection::open_sync(":memory:"),
                Err(FrankenError::Internal(detail)) if detail.contains("cannot block an asupersync runtime task")
            ));

            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("async open should succeed");
            assert!(matches!(
                conn.query_sync("SELECT 1"),
                Err(FrankenError::Internal(detail)) if detail.contains("cannot block an asupersync runtime task")
            ));
            assert!(matches!(
                conn.close_sync(),
                Err(FrankenError::Internal(detail)) if detail.contains("cannot block an asupersync runtime task")
            ));
            conn.close(&cx).await.expect("async close should succeed");
        });
    }
}
