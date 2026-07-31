//! Async-native wrapper around [`Connection`] for use with asupersync's `Cx` capability context.
//!
//! Because [`Connection`] is `!Send` (it uses `Rc<RefCell<..>>` internally), this module
//! provides an [`AsyncConnection`] that runs a dedicated worker thread owning the
//! `Connection`. All SQL operations are dispatched to the worker via a command channel
//! and results are returned through response channels.
//!
//! Every async method accepts a `&Cx`. Cancellation is clean while waiting for
//! mailbox capacity: no command has been admitted and no database effect can
//! occur. The successful `SendPermit::try_send` is the admission linearization
//! point. A cancellation racing that point may resolve on either side; once the
//! send succeeds, the worker owns the effect and the method waits for its
//! terminal result. This avoids reporting `Interrupt` while an admitted
//! mutation is still running.
//!
//! A FrankenSQLite-only cancellation never cancels the ambient native
//! asupersync task context. Admission gives each operation an independent,
//! capability-masked native cancellation sentinel and races that sentinel
//! against mailbox reservation. Local cancellation therefore wakes a
//! full-mailbox waiter promptly without poisoning the ambient task or sibling
//! operations.
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

use crate::{
    Connection, ConnectionEnv, FrankenError, Row, RuntimeConfig, RuntimeContext, SqliteValue,
};
use asupersync::channel::mpsc as async_mpsc;
use asupersync::cx::Cx as NativeCx;
use asupersync::cx::cap as native_cap;
use asupersync::runtime::Runtime as NativeRuntime;
use asupersync::sync::OnceCell as NativeOnceCell;
use fsqlite_types::cx::Cx;
use futures_lite::future;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
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

/// One terminal response owned by exactly one worker command.
///
/// This is deliberately independent of the caller's cancellation context.
/// Before command admission there is no responder in the mailbox. After
/// admission, dropping or panicking through the worker-side sender resolves the
/// response as disconnected, while a normal worker completion publishes one
/// value and wakes either an async or synchronous waiter.
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

#[derive(Debug, Clone, Copy)]
enum ResolvedResponseError {
    Disconnected,
    StillPending,
}

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

        let waker = {
            let mut status = lock_unpoisoned(&state.status);
            if !state.receiver_alive.load(Ordering::Acquire) {
                return;
            }
            match std::mem::replace(&mut *status, ResponseStatus::Ready(value)) {
                ResponseStatus::Pending(waker) => waker,
                ResponseStatus::Ready(_) | ResponseStatus::Disconnected => {
                    unreachable!("response sender must resolve exactly once")
                }
            }
        };
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
            match std::mem::replace(&mut *status, ResponseStatus::Disconnected) {
                ResponseStatus::Pending(waker) => waker,
                ResponseStatus::Ready(value) => {
                    *status = ResponseStatus::Ready(value);
                    None
                }
                ResponseStatus::Disconnected => None,
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

    /// Consume a response after its sender's owner has terminated.
    ///
    /// Worker exit is the synchronization proof used by explicit close: the
    /// close responder must have either published its value or been dropped
    /// before the worker's exit guard publishes completion. Consequently this
    /// method never waits and introduces no cancellation/yield window.
    fn recv_resolved(self) -> Result<T, ResolvedResponseError> {
        let mut status = lock_unpoisoned(&self.state.status);
        match std::mem::replace(&mut *status, ResponseStatus::Disconnected) {
            ResponseStatus::Ready(value) => Ok(value),
            ResponseStatus::Disconnected => Err(ResolvedResponseError::Disconnected),
            ResponseStatus::Pending(waker) => {
                *status = ResponseStatus::Pending(waker);
                Err(ResolvedResponseError::StillPending)
            }
        }
    }
}

impl<T> Future for ResponseReceiver<T> {
    type Output = Result<T, ResponseDisconnected>;

    fn poll(self: Pin<&mut Self>, task_cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut status = lock_unpoisoned(&self.state.status);
        match std::mem::replace(&mut *status, ResponseStatus::Disconnected) {
            ResponseStatus::Ready(value) => Poll::Ready(Ok(value)),
            ResponseStatus::Disconnected => Poll::Ready(Err(ResponseDisconnected)),
            ResponseStatus::Pending(mut waker) => {
                if waker
                    .as_ref()
                    .is_none_or(|registered| !registered.will_wake(task_cx.waker()))
                {
                    waker = Some(task_cx.waker().clone());
                }
                *status = ResponseStatus::Pending(waker);
                Poll::Pending
            }
        }
    }
}

impl<T> Drop for ResponseReceiver<T> {
    fn drop(&mut self) {
        self.state.receiver_alive.store(false, Ordering::Release);
        let mut status = lock_unpoisoned(&self.state.status);
        *status = ResponseStatus::Disconnected;
    }
}

type Responder<T> = ResponseSender<Result<T, FrankenError>>;

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
        #[cfg(test)]
        force_error: bool,
    },
    #[cfg(test)]
    TestBlock {
        entered: sync_mpsc::SyncSender<()>,
        release: sync_mpsc::Receiver<()>,
        tx: Responder<()>,
    },
    #[cfg(test)]
    TestExecuteAfterGate {
        sql: String,
        entered: sync_mpsc::SyncSender<()>,
        release: sync_mpsc::Receiver<()>,
        tx: Responder<usize>,
    },
    #[cfg(test)]
    TestNoop {
        tx: Responder<()>,
    },
    #[cfg(test)]
    TestPanic {
        tx: Responder<()>,
    },
    #[cfg(test)]
    TestRootHasNativeCx {
        tx: Responder<bool>,
    },
}

fn worker_open_err() -> FrankenError {
    FrankenError::Internal("async worker thread terminated during open".to_owned())
}

fn worker_dead_err() -> FrankenError {
    FrankenError::Internal("async worker thread terminated unexpectedly".to_owned())
}

fn requires_runtime_err() -> FrankenError {
    FrankenError::Internal(
        "AsyncConnection async methods require the ambient asupersync Cx of the task polling the future; an attached project Cx alone is insufficient"
            .to_owned(),
    )
}

fn worker_thread_spawn_err(error: std::io::Error) -> FrankenError {
    FrankenError::Internal(format!("failed to spawn async-api worker thread: {error}"))
}

fn native_cx_for_polling_task() -> Result<NativeCx, FrankenError> {
    // Asupersync 0.3.9 stores one cancellation waker per native Cx. An
    // attached project Cx can be shared by multiple tasks, so registering an
    // admission sentinel there would let their distinct wakers overwrite one
    // another. The runtime installs the polling task's own Cx as current for
    // every poll; require that task-local context instead of falling back to a
    // potentially shared attachment.
    NativeCx::current().ok_or_else(requires_runtime_err)
}

/// Build an independent native cancellation context without minting effects.
///
/// Asupersync deliberately exposes detached contexts only as `Cx<cap::None>`.
/// `set_current_restricted` followed by `current` is its public type-erasure
/// boundary: the returned `Cx<cap::All>` retains an empty runtime capability
/// mask and the same independent cancellation state. The guard is strictly
/// stack-local, so the caller's ambient context is restored before this
/// function returns.
fn detached_native_cancel_cx() -> Result<NativeCx, FrankenError> {
    let detached = NativeCx::<native_cap::None>::detached_cancel_context();
    let guard = detached.set_current_restricted();
    let erased = NativeCx::current().ok_or_else(|| {
        FrankenError::Internal(
            "failed to type-erase detached async admission cancellation context".to_owned(),
        )
    });
    drop(guard);
    erased
}

async fn recv_worker_response_async<T>(
    rx: ResponseReceiver<Result<T, FrankenError>>,
) -> Result<T, FrankenError> {
    rx.await.map_err(|_| worker_dead_err())?
}

fn recv_worker_response<T>(
    rx: ResponseReceiver<Result<T, FrankenError>>,
) -> Result<T, FrankenError> {
    rx.recv_blocking().map_err(|_| worker_dead_err())?
}

fn recv_resolved_worker_response<T>(
    rx: ResponseReceiver<Result<T, FrankenError>>,
) -> Result<T, FrankenError> {
    match rx.recv_resolved() {
        Ok(result) => result,
        Err(ResolvedResponseError::Disconnected) => Err(worker_dead_err()),
        Err(ResolvedResponseError::StillPending) => Err(FrankenError::Internal(
            "async worker exited without resolving its close response".to_owned(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Worker task
// ---------------------------------------------------------------------------

struct WorkerLifecycle {
    finished: AtomicBool,
    waiter: Mutex<Option<Waker>>,
    changed: Condvar,
    #[cfg(test)]
    cleanup_gate: Mutex<Option<(sync_mpsc::SyncSender<()>, sync_mpsc::Receiver<()>)>>,
    #[cfg(test)]
    close_attempts: AtomicUsize,
}

impl WorkerLifecycle {
    fn new() -> Self {
        Self {
            finished: AtomicBool::new(false),
            waiter: Mutex::new(None),
            changed: Condvar::new(),
            #[cfg(test)]
            cleanup_gate: Mutex::new(None),
            #[cfg(test)]
            close_attempts: AtomicUsize::new(0),
        }
    }

    fn finish(&self) {
        self.finished.store(true, Ordering::Release);
        let waker = lock_unpoisoned(&self.waiter).take();
        self.changed.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    #[cfg(test)]
    fn wait_finished(&self, timeout: std::time::Duration) -> bool {
        if self.finished.load(Ordering::Acquire) {
            return true;
        }
        let waiter = lock_unpoisoned(&self.waiter);
        let (_waiter, result) = self
            .changed
            .wait_timeout_while(waiter, timeout, |_| !self.finished.load(Ordering::Acquire))
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !result.timed_out() || self.finished.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn install_cleanup_gate(
        &self,
        entered: sync_mpsc::SyncSender<()>,
        release: sync_mpsc::Receiver<()>,
    ) {
        *lock_unpoisoned(&self.cleanup_gate) = Some((entered, release));
    }

    #[cfg(test)]
    fn wait_at_cleanup_gate(&self) {
        if let Some((entered, release)) = lock_unpoisoned(&self.cleanup_gate).take() {
            let _ = entered.send(());
            let _ = release.recv();
        }
    }

    #[cfg(test)]
    fn record_close_attempt(&self) {
        self.close_attempts.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    fn close_attempts_for_test(&self) -> usize {
        self.close_attempts.load(Ordering::Acquire)
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
        let mut waiter = lock_unpoisoned(&self.lifecycle.waiter);
        if self.lifecycle.finished.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        if waiter
            .as_ref()
            .is_none_or(|registered| !registered.will_wake(task_cx.waker()))
        {
            *waiter = Some(task_cx.waker().clone());
        }
        Poll::Pending
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

struct CommandSender {
    tx: Option<async_mpsc::Sender<Command>>,
    worker_thread: Thread,
}

impl CommandSender {
    fn tx(&self) -> Result<&async_mpsc::Sender<Command>, FrankenError> {
        self.tx.as_ref().ok_or_else(worker_dead_err)
    }

    async fn send_async<Caps, F>(&self, cx: &Cx<Caps>, build: F) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        F: FnOnce() -> Command,
    {
        self.send_async_inner(
            cx,
            build,
            #[cfg(test)]
            None,
            #[cfg(test)]
            None,
        )
        .await
    }

    async fn send_async_inner<Caps, F>(
        &self,
        cx: &Cx<Caps>,
        build: F,
        #[cfg(test)] reserved_gate: Option<(sync_mpsc::SyncSender<()>, sync_mpsc::Receiver<()>)>,
        #[cfg(test)] capacity_waiter_ready: Option<sync_mpsc::Sender<()>>,
    ) -> Result<(), FrankenError>
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

        // Avoid allocating a child cancellation context for the overwhelmingly
        // common uncontended path. try_reserve() still respects queued waiters,
        // so falling through to reserve() preserves MPSC admission ordering.
        let permit = match tx.try_reserve() {
            Ok(permit) => permit,
            Err(async_mpsc::SendError::Full(())) => {
                let local_native_cx = detached_native_cancel_cx()?;

                // The project Cx has its own parent/child cancellation tree,
                // but only an attached native Cx owns a task waker. Give this
                // operation a child whose native plane is independent from the
                // ambient task. Cancelling the public parent propagates into
                // this child and wakes the sentinel below without cancelling
                // ambient siblings.
                let operation_cx = cx.create_child();
                operation_cx.set_native_cx(local_native_cx.clone());

                // Asupersync 0.3.9's MPSC Reserve checks cancellation but
                // registers only a channel-capacity waker. OnceCell::wait is
                // the public primitive in this version that registers the
                // native cancellation waker, so an uninitialized cell serves
                // as a zero-effect wake sentinel. The ambient sentinel
                // preserves native runtime cancellation while the independent
                // sentinel bridges project-Cx cancellation.
                let local_cancel_sentinel = NativeOnceCell::<()>::new();
                let polling_cancel_sentinel = NativeOnceCell::<()>::new();
                let mut local_cancel = std::pin::pin!(local_cancel_sentinel.wait(&local_native_cx));
                let mut polling_cancel =
                    std::pin::pin!(polling_cancel_sentinel.wait(&polling_native_cx));
                let mut reserve = std::pin::pin!(tx.reserve(&polling_native_cx));
                #[cfg(test)]
                let mut capacity_waiter_ready = capacity_waiter_ready;

                std::future::poll_fn(|task_cx| {
                    if let Poll::Ready(Err(_)) = local_cancel.as_mut().poll(task_cx) {
                        return Poll::Ready(Err(FrankenError::Interrupt));
                    }
                    if let Poll::Ready(Err(_)) = polling_cancel.as_mut().poll(task_cx) {
                        return Poll::Ready(Err(FrankenError::Interrupt));
                    }

                    match reserve.as_mut().poll(task_cx) {
                        Poll::Ready(Ok(permit)) => {
                            // Close both races between the cancellation
                            // sentinel polls and capacity acquisition.
                            if checkpoint_or_interrupt(cx).is_err()
                                || polling_native_cx.checkpoint().is_err()
                            {
                                Poll::Ready(Err(FrankenError::Interrupt))
                            } else {
                                Poll::Ready(Ok(permit))
                            }
                        }
                        Poll::Ready(Err(error)) => Poll::Ready(Err(send_err(error))),
                        Poll::Pending => {
                            #[cfg(test)]
                            if let Some(ready) = capacity_waiter_ready.take() {
                                let _ = ready.send(());
                            }
                            Poll::Pending
                        }
                    }
                })
                .await?
            }
            Err(error) => return Err(send_err(error)),
        };
        #[cfg(test)]
        if let Some((entered, release)) = reserved_gate {
            let _ = entered.send(());
            release.recv().map_err(|_| worker_dead_err())?;
        }
        // Reservation owns capacity, but actor admission has not happened yet.
        // Re-check the public Cx to close the race with cross-thread
        // cancellation after reserve() becomes ready. Returning here drops the
        // permit and releases its capacity without enqueuing the command.
        checkpoint_or_interrupt(cx)?;
        if polling_native_cx.checkpoint().is_err() {
            return Err(FrankenError::Interrupt);
        }
        permit.try_send(build()).map_err(send_err)?;
        self.worker_thread.unpark();
        Ok(())
    }

    fn send_sync<F>(&self, build: F) -> Result<(), FrankenError>
    where
        F: FnOnce() -> Command,
    {
        let tx = self.tx()?;
        // Join the exact same FIFO reservation queue as asynchronous senders.
        // The fresh detached context is capability-empty and cannot be
        // cancelled by ambient runtime state. futures-lite's synchronous
        // driver parks on the channel waker, so receiver closure, a dropped
        // permit, or a cancelled head waiter all wake this reservation without
        // a private condition-variable side channel.
        let sync_native_cx = detached_native_cancel_cx()?;
        let permit = future::block_on(tx.reserve(&sync_native_cx)).map_err(send_err)?;
        permit.try_send(build()).map_err(send_err)?;
        self.worker_thread.unpark();
        Ok(())
    }

    #[cfg(test)]
    fn try_send_for_test(&self, command: Command) -> Result<(), Command> {
        match self.tx().expect("test worker sender").try_send(command) {
            Ok(()) => {
                self.worker_thread.unpark();
                Ok(())
            }
            Err(
                async_mpsc::SendError::Full(command)
                | async_mpsc::SendError::Disconnected(command)
                | async_mpsc::SendError::Cancelled(command),
            ) => Err(command),
        }
    }

    #[cfg(test)]
    fn queued_for_test(&self) -> usize {
        self.tx()
            .expect("test worker sender")
            .telemetry_snapshot(0)
            .queued_messages
    }

    #[cfg(test)]
    fn admission_counts_for_test(&self) -> (usize, usize, usize) {
        let snapshot = self.tx().expect("test worker sender").telemetry_snapshot(0);
        (
            snapshot.queued_messages,
            snapshot.reserved_uncommitted_obligations,
            snapshot.send_waiter_count,
        )
    }
}

impl Drop for CommandSender {
    fn drop(&mut self) {
        // Disconnect first, then wake. Waking before the last sender is dropped
        // can lose the only park token and strand an idle worker.
        drop(self.tx.take());
        self.worker_thread.unpark();
    }
}

fn publish_transaction_state(conn: &Connection, in_txn: &AtomicBool) {
    in_txn.store(conn.in_transaction(), Ordering::Release);
}

fn respond<T>(
    conn: &Connection,
    in_txn: &AtomicBool,
    tx: Responder<T>,
    result: Result<T, FrankenError>,
) {
    publish_transaction_state(conn, in_txn);
    tx.send(result);
}

fn close_connection_once(
    conn: &mut Connection,
    close_attempted: &AtomicBool,
    _lifecycle: &WorkerLifecycle,
) -> Result<(), FrankenError> {
    if close_attempted.swap(true, Ordering::AcqRel) {
        return Err(FrankenError::Internal(
            "async worker attempted to close its connection more than once".to_owned(),
        ));
    }
    #[cfg(test)]
    _lifecycle.record_close_attempt();
    future::block_on(conn.close_in_place())
}

fn process_command(
    conn: &mut Connection,
    in_txn: &AtomicBool,
    close_attempted: &AtomicBool,
    lifecycle: &WorkerLifecycle,
    cmd: Command,
) -> bool {
    match cmd {
        Command::Prepare { sql, tx } => {
            let result = future::block_on(conn.prepare(&sql)).map(drop);
            respond(conn, in_txn, tx, result);
        }
        Command::Query { sql, tx } => {
            let result = future::block_on(conn.query(&sql));
            respond(conn, in_txn, tx, result);
        }
        Command::QueryWithParams { sql, params, tx } => {
            let result = future::block_on(conn.query_with_params(&sql, &params));
            respond(conn, in_txn, tx, result);
        }
        Command::QueryRow { sql, tx } => {
            let result = future::block_on(conn.query_row(&sql));
            respond(conn, in_txn, tx, result);
        }
        Command::QueryRowWithParams { sql, params, tx } => {
            let result = future::block_on(conn.query_row_with_params(&sql, &params));
            respond(conn, in_txn, tx, result);
        }
        Command::Execute { sql, tx } => {
            let result = future::block_on(conn.execute(&sql));
            respond(conn, in_txn, tx, result);
        }
        Command::ExecuteWithParams { sql, params, tx } => {
            let result = future::block_on(conn.execute_with_params(&sql, &params));
            respond(conn, in_txn, tx, result);
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
            respond(conn, in_txn, tx, result);
        }
        Command::ExecuteBatch { sql, tx } => {
            let result = future::block_on(conn.execute_batch(&sql));
            respond(conn, in_txn, tx, result);
        }
        Command::BeginTransaction { tx } => {
            let result = future::block_on(conn.begin_transaction());
            respond(conn, in_txn, tx, result);
        }
        Command::CommitTransaction { tx } => {
            let result = future::block_on(conn.commit_transaction());
            respond(conn, in_txn, tx, result);
        }
        Command::RollbackTransaction { tx } => {
            let result = future::block_on(conn.rollback_transaction());
            respond(conn, in_txn, tx, result);
        }
        Command::LastInsertRowid { tx } => {
            respond(conn, in_txn, tx, Ok(conn.last_insert_rowid()));
        }
        Command::Close {
            tx,
            #[cfg(test)]
            force_error,
        } => {
            // Mark the close attempt before entering engine code. Even if
            // close_in_place panics, outer actor cleanup must not silently
            // invoke it a second time and mask the first terminal failure.
            let result = close_connection_once(conn, close_attempted, lifecycle);
            #[cfg(test)]
            let result = if force_error {
                Err(FrankenError::Internal(
                    "intentional async-api close failure".to_owned(),
                ))
            } else {
                result
            };
            in_txn.store(false, Ordering::Release);
            tx.send(result);
            return false;
        }
        #[cfg(test)]
        Command::TestBlock {
            entered,
            release,
            tx,
        } => {
            let _ = entered.send(());
            let result = release.recv().map_err(|_| worker_dead_err());
            respond(conn, in_txn, tx, result);
        }
        #[cfg(test)]
        Command::TestExecuteAfterGate {
            sql,
            entered,
            release,
            tx,
        } => {
            let _ = entered.send(());
            let result = release
                .recv()
                .map_err(|_| worker_dead_err())
                .and_then(|()| future::block_on(conn.execute(&sql)));
            respond(conn, in_txn, tx, result);
        }
        #[cfg(test)]
        Command::TestNoop { tx } => {
            respond(conn, in_txn, tx, Ok(()));
        }
        #[cfg(test)]
        Command::TestPanic { tx: _tx } => {
            panic!("intentional async-api worker panic");
        }
        #[cfg(test)]
        Command::TestRootHasNativeCx { tx } => {
            respond(
                conn,
                in_txn,
                tx,
                Ok(conn.root_cx().attached_native_cx().is_some()),
            );
        }
    }
    true
}

fn worker_loop(
    conn: &mut Connection,
    in_txn: &AtomicBool,
    mut rx: async_mpsc::Receiver<Command>,
    close_attempted: &AtomicBool,
    lifecycle: &WorkerLifecycle,
) {
    loop {
        let cmd = match rx.try_recv() {
            Ok(cmd) => cmd,
            Err(async_mpsc::RecvError::Empty | async_mpsc::RecvError::Cancelled) => {
                thread::park();
                continue;
            }
            Err(async_mpsc::RecvError::Disconnected) => return,
        };

        if !process_command(conn, in_txn, close_attempted, lifecycle, cmd) {
            return;
        }
    }
}

enum WorkerEnv {
    Explicit(ConnectionEnv),
    Rooted(Cx),
}

impl WorkerEnv {
    fn rooted(cx: &Cx) -> Self {
        let root_cx = cx.create_child();
        // A NativeCx belongs to the runtime task that owns and polls it.
        // Connection futures are polled on this dedicated OS actor instead,
        // so inheriting the opener's attachment would transplant a
        // task-affine cancellation/driver context across execution domains.
        // Keep the project-Cx cancellation/budget lineage, but make the actor
        // root explicitly native-detached.
        root_cx.clear_native_cx();
        Self::Rooted(root_cx)
    }

    fn resolve(self) -> ConnectionEnv {
        match self {
            Self::Explicit(env) => env,
            Self::Rooted(root_cx) => ConnectionEnv::new(Arc::new(
                RuntimeContext::new_with_root_cx(RuntimeConfig::default(), &root_cx),
            )),
        }
    }
}

struct NativeRootLease {
    lifecycle: Arc<WorkerLifecycle>,
}

impl NativeRootLease {
    async fn wait_async(self) {
        self.wait_async_ref().await;
    }

    async fn wait_async_ref(&self) {
        WorkerExit {
            lifecycle: &self.lifecycle,
        }
        .await;
    }

    #[cfg(test)]
    fn lifecycle_for_test(&self) -> Arc<WorkerLifecycle> {
        Arc::clone(&self.lifecycle)
    }
}

struct NativeRootBridge {
    shutdown: ResponseSender<()>,
    lease: NativeRootLease,
}

async fn rooted_worker_env(cx: &Cx) -> Result<(WorkerEnv, NativeRootBridge), FrankenError> {
    checkpoint_or_interrupt(cx)?;
    // Validate that this call is inside a native asupersync execution
    // context. Runtime::block_on's ambient Cx in asupersync 0.3.9 carries
    // drivers but no spawn gateway, so Cx::spawn is not universally available
    // here. The stack-local runtime handle is used only as the admission
    // gateway below; it is never stored in the connection or worker.
    native_cx_for_polling_task()?;
    let runtime = NativeRuntime::current_handle().ok_or_else(requires_runtime_err)?;
    let (root_tx, root_rx) = response_channel();
    let (shutdown, shutdown_rx) = response_channel();
    let lifecycle = Arc::new(WorkerLifecycle::new());
    let exit = WorkerExitGuard {
        lifecycle: Arc::clone(&lifecycle),
    };
    runtime
        .try_spawn_with_cx(move |native_child| async move {
            // If admission rejects the factory, dropping it also drops this
            // guard; after admission the future holds it through task exit.
            let _exit = exit;
            // Keep this task-owned Cx runtime-affine for the full lease. It is
            // deliberately never sent to the separately-polled OS actor.
            let _native_child = native_child;
            root_tx.send(());
            // Dropping the worker-side sender resolves this independent
            // lifetime obligation after engine use ends.
            let _ = shutdown_rx.await;
        })
        .map_err(|error| {
            FrankenError::Internal(format!(
                "failed to spawn async connection root task: {error}"
            ))
        })?;
    drop(runtime);
    root_rx
        .await
        .map_err(|_| FrankenError::Internal("async connection root task terminated".to_owned()))?;
    let env = WorkerEnv::rooted(cx);
    Ok((
        env,
        NativeRootBridge {
            shutdown,
            lease: NativeRootLease { lifecycle },
        },
    ))
}

enum OpenRequest {
    Create {
        path: String,
        env: WorkerEnv,
    },
    Existing {
        path: String,
        env: WorkerEnv,
    },
    ReadOnly {
        path: String,
        env: WorkerEnv,
    },
    Flags {
        path: String,
        flags: crate::compat::OpenFlags,
        env: WorkerEnv,
    },
}

impl OpenRequest {
    fn open(self) -> Result<Connection, FrankenError> {
        future::block_on(async move {
            match self {
                Self::Create { path, env } => Connection::open_with_env(path, env.resolve()).await,
                Self::Existing { path, env } => {
                    Connection::open_existing_with_env(path, env.resolve()).await
                }
                Self::ReadOnly { path, env } => {
                    Connection::open_schema_only_with_env(path, env.resolve()).await
                }
                Self::Flags { path, flags, env } => {
                    open_with_flags_and_env(path, flags, env.resolve()).await
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
        (true, false, false) => {
            if path == ":memory:" {
                Err(FrankenError::NotImplemented(
                    "read-only :memory: connections are not supported".to_owned(),
                ))
            } else {
                Connection::open_schema_only_with_env(path, env).await
            }
        }
        (false, true, false) => {
            if path == ":memory:" {
                Connection::open_with_env(path, env).await
            } else {
                Connection::open_existing_with_env(path, env).await
            }
        }
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

    async fn wait_async(mut self) {
        self.wait_async_ref().await;
        self.join_async_ref().await;
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

    #[cfg(test)]
    fn lifecycle_for_test(&self) -> Arc<WorkerLifecycle> {
        Arc::clone(&self.lifecycle)
    }
}

fn spawn_worker_thread(
    request: OpenRequest,
    cmd_rx: async_mpsc::Receiver<Command>,
    open_tx: ResponseSender<Result<(), FrankenError>>,
    in_txn: Arc<AtomicBool>,
    lifecycle: Arc<WorkerLifecycle>,
    bridge_shutdown: Option<ResponseSender<()>>,
    #[cfg(test)] open_gate: Option<(sync_mpsc::SyncSender<()>, sync_mpsc::Receiver<()>)>,
) -> Result<JoinHandle<()>, FrankenError> {
    thread::Builder::new()
        .name("fsqlite-worker".to_owned())
        .stack_size(WORKER_STACK_BYTES)
        .spawn(move || {
            let _exit = WorkerExitGuard {
                lifecycle: Arc::clone(&lifecycle),
            };
            // Keep the structured native root task alive for exactly as long
            // as the OS actor. It is a lifetime receipt, not the source of the
            // actor's polling context. Dropping this sender wakes the task
            // after actor-owned engine cleanup has ended.
            let _bridge_shutdown = bridge_shutdown;
            #[cfg(test)]
            if let Some((entered, release)) = open_gate {
                let _ = entered.send(());
                if release.recv().is_err() {
                    return;
                }
            }

            match catch_unwind(AssertUnwindSafe(|| request.open())) {
                Ok(Ok(mut conn)) => {
                    let close_attempted = AtomicBool::new(false);
                    open_tx.send(Ok(()));
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        // worker_loop owns the receiver. Return and unwind both
                        // drop it here, closing admission and waking every
                        // channel-native reserve waiter before cleanup begins.
                        worker_loop(&mut conn, &in_txn, cmd_rx, &close_attempted, &lifecycle);
                    }));
                    in_txn.store(false, Ordering::Release);
                    #[cfg(test)]
                    lifecycle.wait_at_cleanup_gate();
                    if !close_attempted.load(Ordering::Acquire) {
                        let _ = catch_unwind(AssertUnwindSafe(|| {
                            let _ = close_connection_once(&mut conn, &close_attempted, &lifecycle);
                        }));
                    }
                }
                Ok(Err(error)) => open_tx.send(Err(error)),
                Err(_) => {
                    // Dropping the unresolved open responder reports worker
                    // death to the opener.
                }
            }
        })
        .map_err(worker_thread_spawn_err)
}

fn start_worker(
    request: OpenRequest,
    bridge_shutdown: Option<ResponseSender<()>>,
    #[cfg(test)] open_gate: Option<(sync_mpsc::SyncSender<()>, sync_mpsc::Receiver<()>)>,
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
    let lifecycle = Arc::new(WorkerLifecycle::new());
    let join = spawn_worker_thread(
        request,
        cmd_rx,
        open_tx,
        Arc::clone(&in_txn),
        Arc::clone(&lifecycle),
        bridge_shutdown,
        #[cfg(test)]
        open_gate,
    )?;
    let worker_thread = join.thread().clone();
    Ok((
        CommandSender {
            tx: Some(cmd_tx),
            worker_thread,
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

// ---------------------------------------------------------------------------
// Cx → FrankenError bridge
// ---------------------------------------------------------------------------

/// Map a `Cx::checkpoint()` cancellation error to a `FrankenError::Interrupt`.
fn checkpoint_or_interrupt<Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>>(
    cx: &Cx<Caps>,
) -> Result<(), FrankenError> {
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)
}

/// Map command admission failures to the public cancellation/worker errors.
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
/// Async command admission uses asupersync's bounded two-phase MPSC protocol.
/// Cancellation before admission returns [`FrankenError::Interrupt`] without
/// touching the connection. The permit's successful `try_send` is the exact
/// admission point. A concurrent cancellation may win or lose that race; after
/// admission, the worker owns the effect and the method waits for the worker's
/// terminal result, even if cancellation is requested in the meantime.
///
/// Local FrankenSQLite cancellation is deliberately not bridged by cancelling
/// a clone of the ambient native `Cx`: native clones share cancellation state,
/// so that would poison sibling operations. Admission instead races mailbox
/// reservation against an independent, capability-masked native cancellation
/// sentinel. This wakes a full-mailbox operation promptly while leaving the
/// ambient native task and sibling operations live.
///
/// The connection itself lives on a dedicated large-stack worker thread (because
/// [`Connection`] is `!Send`). Commands are dispatched via an internal channel
/// and results flow back through single-resolution response obligations.
/// Capacity and result waits are waker-driven: async callers never enter a
/// condition-variable wait, a thread join, or the blocking pool. Polling and
/// response publication do take a short standard-mutex critical section to
/// exchange one value or waker; that lock never spans SQL work or an `.await`.
///
/// # Shutdown
///
/// Dropping `AsyncConnection` disconnects the mailbox, unparks the worker, and
/// detaches without waiting. The worker drains already-admitted commands and
/// then calls [`Connection::close_in_place`]. It also owns the shutdown end of
/// a structured native lifetime task, so that task cannot finish before the
/// worker. The task's native `Cx` remains task-owned and is never transplanted
/// onto the separately-polled OS actor. This makes `Drop` nonblocking while
/// preserving actor-owned effects and connection cleanup.
///
/// For explicit, error-checked shutdown use [`close`](Self::close) on the
/// async path or [`close_sync`](Self::close_sync) on the synchronous path.
pub struct AsyncConnection {
    cmd_tx: Option<CommandSender>,
    worker: Option<WorkerHandle>,
    native_root: Option<NativeRootLease>,
    /// Set once the close command is admitted. The response remains owned by
    /// the connection until worker and native-root exit have both been
    /// observed, so dropping a `close()` future cannot discard the terminal
    /// close result.
    close_response: Option<ResponseReceiver<Result<(), FrankenError>>>,
    close_admitted: bool,
    #[cfg(test)]
    force_close_error: bool,
    /// Tracks whether the worker thread's connection has an active transaction.
    /// The worker publishes this after every terminal command, including raw
    /// SQL `BEGIN`, `COMMIT`, and `ROLLBACK`.
    in_txn: Arc<AtomicBool>,
}

impl AsyncConnection {
    /// Open a database connection asynchronously with `Cx` integration.
    ///
    /// The opening `Cx` becomes the parent of the connection runtime so its
    /// budget and cancellation lineage flow into engine work. Rooted opens
    /// intentionally require a full-capability project `Cx`; callers with a
    /// narrower admission context can provide an authoritative
    /// [`ConnectionEnv`] through [`open_with_env`](Self::open_with_env).
    ///
    /// The structured native task is a lifetime receipt only. Its task-owned
    /// native `Cx` never crosses to the dedicated OS actor, which polls the
    /// `!Send` connection futures with a native-detached project root.
    ///
    /// ```compile_fail
    /// use fsqlite::AsyncConnection;
    /// use fsqlite_types::cx::{Cx, cap};
    ///
    /// async fn rooted_open_requires_full_caps(cx: &Cx<cap::None>) {
    ///     let _ = AsyncConnection::open(cx, ":memory:").await;
    /// }
    /// ```
    pub async fn open(cx: &Cx, path: impl Into<String>) -> Result<Self, FrankenError> {
        let (env, bridge) = rooted_worker_env(cx).await?;
        Self::open_request_async(
            cx,
            OpenRequest::Create {
                path: path.into(),
                env,
            },
            Some(bridge),
            #[cfg(test)]
            None,
        )
        .await
    }

    /// Open an existing file-backed database for reading and writing.
    ///
    /// Missing and zero-length files are refused rather than created.
    pub async fn open_existing(cx: &Cx, path: impl Into<String>) -> Result<Self, FrankenError> {
        let (env, bridge) = rooted_worker_env(cx).await?;
        Self::open_request_async(
            cx,
            OpenRequest::Existing {
                path: path.into(),
                env,
            },
            Some(bridge),
            #[cfg(test)]
            None,
        )
        .await
    }

    /// Open an existing file-backed database in true read-only mode.
    pub async fn open_read_only(cx: &Cx, path: impl Into<String>) -> Result<Self, FrankenError> {
        let (env, bridge) = rooted_worker_env(cx).await?;
        Self::open_request_async(
            cx,
            OpenRequest::ReadOnly {
                path: path.into(),
                env,
            },
            Some(bridge),
            #[cfg(test)]
            None,
        )
        .await
    }

    /// Open with the public SQLite-compatible flag surface.
    pub async fn open_with_flags(
        cx: &Cx,
        path: impl Into<String>,
        flags: crate::compat::OpenFlags,
    ) -> Result<Self, FrankenError> {
        let (env, bridge) = rooted_worker_env(cx).await?;
        Self::open_request_async(
            cx,
            OpenRequest::Flags {
                path: path.into(),
                flags,
                env,
            },
            Some(bridge),
            #[cfg(test)]
            None,
        )
        .await
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

    /// Open an existing file-backed database synchronously.
    pub fn open_existing_sync(path: impl Into<String>) -> Result<Self, FrankenError> {
        Self::open_existing_sync_with_env(path, ConnectionEnv::default())
    }

    /// Open an existing file-backed database in true read-only mode.
    pub fn open_read_only_sync(path: impl Into<String>) -> Result<Self, FrankenError> {
        Self::open_read_only_sync_with_env(path, ConnectionEnv::default())
    }

    /// Open synchronously with the public SQLite-compatible flag surface.
    pub fn open_sync_with_flags(
        path: impl Into<String>,
        flags: crate::compat::OpenFlags,
    ) -> Result<Self, FrankenError> {
        Self::open_sync_with_flags_and_env(path, flags, ConnectionEnv::default())
    }

    /// Open synchronously with SQLite-compatible flags and an explicit
    /// [`ConnectionEnv`].
    pub fn open_sync_with_flags_and_env(
        path: impl Into<String>,
        flags: crate::compat::OpenFlags,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError> {
        Self::open_request_sync(
            OpenRequest::Flags {
                path: path.into(),
                flags,
                env: WorkerEnv::Explicit(env),
            },
            #[cfg(test)]
            None,
        )
    }

    /// Open a database connection without a capability context, with a custom
    /// [`ConnectionEnv`].
    pub fn open_sync_with_env(
        path: impl Into<String>,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError> {
        Self::open_request_sync(
            OpenRequest::Create {
                path: path.into(),
                env: WorkerEnv::Explicit(env),
            },
            #[cfg(test)]
            None,
        )
    }

    /// Open an existing file-backed database with a custom [`ConnectionEnv`].
    pub fn open_existing_sync_with_env(
        path: impl Into<String>,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError> {
        Self::open_request_sync(
            OpenRequest::Existing {
                path: path.into(),
                env: WorkerEnv::Explicit(env),
            },
            #[cfg(test)]
            None,
        )
    }

    /// Open an existing file-backed database read-only with a custom
    /// [`ConnectionEnv`].
    pub fn open_read_only_sync_with_env(
        path: impl Into<String>,
        env: ConnectionEnv,
    ) -> Result<Self, FrankenError> {
        Self::open_request_sync(
            OpenRequest::ReadOnly {
                path: path.into(),
                env: WorkerEnv::Explicit(env),
            },
            #[cfg(test)]
            None,
        )
    }

    /// Open a database connection with an explicit [`ConnectionEnv`].
    ///
    /// The environment is authoritative for the connection's runtime lineage;
    /// `cx` governs only the pre-admission cancellation check for this open.
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
                env: WorkerEnv::Explicit(env),
            },
            None,
            #[cfg(test)]
            None,
        )
        .await
    }

    /// Open an existing file-backed database with an explicit
    /// [`ConnectionEnv`].
    ///
    /// The environment is authoritative for the connection's runtime lineage;
    /// `cx` governs only the pre-admission cancellation check for this open.
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
                env: WorkerEnv::Explicit(env),
            },
            None,
            #[cfg(test)]
            None,
        )
        .await
    }

    /// Open an existing file-backed database read-only with an explicit
    /// [`ConnectionEnv`].
    ///
    /// The environment is authoritative for the connection's runtime lineage;
    /// `cx` governs only the pre-admission cancellation check for this open.
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
                env: WorkerEnv::Explicit(env),
            },
            None,
            #[cfg(test)]
            None,
        )
        .await
    }

    /// Open with SQLite-compatible flags and an explicit [`ConnectionEnv`].
    ///
    /// The environment is authoritative for the connection's runtime lineage;
    /// `cx` governs only the pre-admission cancellation check for this open.
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
                env: WorkerEnv::Explicit(env),
            },
            None,
            #[cfg(test)]
            None,
        )
        .await
    }

    fn open_request_sync(
        request: OpenRequest,
        #[cfg(test)] open_gate: Option<(sync_mpsc::SyncSender<()>, sync_mpsc::Receiver<()>)>,
    ) -> Result<Self, FrankenError> {
        let (cmd_tx, worker, in_txn, open_rx) = start_worker(
            request,
            None,
            #[cfg(test)]
            open_gate,
        )?;
        match wait_for_worker_open(open_rx) {
            Ok(()) => Ok(Self {
                cmd_tx: Some(cmd_tx),
                worker: Some(worker),
                native_root: None,
                close_response: None,
                close_admitted: false,
                #[cfg(test)]
                force_close_error: false,
                in_txn,
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
        bridge: Option<NativeRootBridge>,
        #[cfg(test)] open_gate: Option<(sync_mpsc::SyncSender<()>, sync_mpsc::Receiver<()>)>,
    ) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let (mut bridge_shutdown, mut native_root) = match bridge {
            Some(bridge) => (Some(bridge.shutdown), Some(bridge.lease)),
            None => (None, None),
        };
        if let Err(error) = checkpoint_or_interrupt(cx) {
            drop(bridge_shutdown.take());
            if let Some(lease) = native_root.take() {
                lease.wait_async().await;
            }
            return Err(error);
        }
        if let Err(error) = native_cx_for_polling_task() {
            drop(bridge_shutdown.take());
            if let Some(lease) = native_root.take() {
                lease.wait_async().await;
            }
            return Err(error);
        }
        let (cmd_tx, worker, in_txn, open_rx) = match start_worker(
            request,
            bridge_shutdown.take(),
            #[cfg(test)]
            open_gate,
        ) {
            Ok(started) => started,
            Err(error) => {
                if let Some(lease) = native_root.take() {
                    lease.wait_async().await;
                }
                return Err(error);
            }
        };
        match open_rx.await {
            Ok(Ok(())) => Ok(Self {
                cmd_tx: Some(cmd_tx),
                worker: Some(worker),
                native_root,
                close_response: None,
                close_admitted: false,
                #[cfg(test)]
                force_close_error: false,
                in_txn,
            }),
            Ok(Err(error)) => {
                drop(cmd_tx);
                worker.wait_async().await;
                if let Some(lease) = native_root.take() {
                    lease.wait_async().await;
                }
                Err(error)
            }
            Err(_) => {
                drop(cmd_tx);
                worker.wait_async().await;
                if let Some(lease) = native_root.take() {
                    lease.wait_async().await;
                }
                Err(worker_open_err())
            }
        }
    }

    /// Return a reference to the command sender, or an error if the worker is gone.
    fn sender(&self) -> Result<&CommandSender, FrankenError> {
        self.cmd_tx
            .as_ref()
            .ok_or_else(|| FrankenError::Internal("AsyncConnection has been closed".to_owned()))
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
        self.sender()?.send_async(cx, move || build(tx)).await?;
        recv_worker_response_async(rx).await
    }

    /// Validate and prepare one SQL statement on the dedicated worker.
    ///
    /// This is the synchronous-consumer counterpart to the async methods
    /// below. It intentionally performs no cancellation check and blocks the
    /// caller until the worker responds.
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

    /// Visit every row from a parameterized query on the caller thread.
    ///
    /// The worker materializes the result and releases the connection before
    /// any callback runs. A callback may therefore re-enter this same
    /// `AsyncConnection` through synchronous methods without deadlocking.
    /// Returning an error stops callback iteration and returns that exact
    /// callback error; the already-materialized result is then dropped.
    pub fn query_with_params_for_each_sync<F>(
        &self,
        sql: &str,
        params: &[SqliteValue],
        mut f: F,
    ) -> Result<(), FrankenError>
    where
        F: FnMut(&Row) -> Result<(), FrankenError>,
    {
        let rows = self.query_with_params_sync(sql, params)?;
        for row in &rows {
            f(row)?;
        }
        Ok(())
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
    ///
    /// Inputs must be fully validated before this call. The worker deliberately
    /// skips per-statement savepoints; if one execution fails, earlier effects
    /// remain pending and the caller must roll back the enclosing transaction.
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
    ///
    /// This is a cheap local read — no round-trip to the worker thread.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.in_txn.load(Ordering::Acquire)
    }

    fn store_terminal_close_result(&mut self, result: Result<(), FrankenError>) {
        let (tx, rx) = response_channel();
        tx.send(result);
        self.close_response = Some(rx);
        self.close_admitted = true;
    }

    async fn finish_close_async(&mut self) -> Result<(), FrankenError> {
        if let Some(worker) = self.worker.as_ref() {
            worker.wait_async_ref().await;
        }
        if let Some(worker) = self.worker.as_mut() {
            worker.join_async_ref().await;
        }
        drop(self.worker.take());
        if let Some(lease) = self.native_root.as_ref() {
            lease.wait_async_ref().await;
        }
        drop(self.native_root.take());

        self.close_admitted = false;
        let Some(response) = self.close_response.take() else {
            return Ok(());
        };
        recv_resolved_worker_response(response)
    }

    fn finish_close_sync(&mut self) -> Result<(), FrankenError> {
        if let Some(worker) = self.worker.take() {
            worker.wait_sync();
        }
        if let Some(lease) = self.native_root.take() {
            // An asynchronously opened connection owns a structured native
            // lifetime task. A synchronous caller cannot drain that task without
            // risking deadlock on a current-thread runtime. Worker exit already
            // signalled its independent shutdown obligation, so release the
            // observation handle and let the runtime finish that ready task.
            drop(lease);
        }

        self.close_admitted = false;
        let Some(response) = self.close_response.take() else {
            return Ok(());
        };
        recv_resolved_worker_response(response)
    }

    /// Explicitly close the connection, returning any error from the close operation.
    ///
    /// After this call, all subsequent operations return an error. The method
    /// awaits both worker termination and the structured native-root task. It
    /// only performs the OS-thread `join` after `JoinHandle::is_finished`, so
    /// no runtime thread is blocked on that join. If the close future itself is
    /// dropped after command admission, a later call resumes observation of the
    /// same shutdown and returns that command's terminal close result.
    pub async fn close<Caps>(&mut self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        if !self.close_admitted {
            // A previously dropped future may have left an unadmitted
            // response obligation behind. Its command-side sender was dropped
            // with that future, so a retry replaces it before admission.
            drop(self.close_response.take());
            let Some(cmd_tx) = self.cmd_tx.as_ref() else {
                return self.finish_close_async().await;
            };
            let (tx, rx) = response_channel();
            self.close_response = Some(rx);
            let command = Command::Close {
                tx,
                #[cfg(test)]
                force_error: self.force_close_error,
            };
            match cmd_tx.send_async(cx, move || command).await {
                Ok(()) => {
                    self.close_admitted = true;
                    drop(self.cmd_tx.take());
                }
                Err(FrankenError::Interrupt) => {
                    drop(self.close_response.take());
                    return Err(FrankenError::Interrupt);
                }
                Err(error) => {
                    drop(self.close_response.take());
                    self.store_terminal_close_result(Err(error));
                    drop(self.cmd_tx.take());
                }
            }
        }
        self.finish_close_async().await
    }

    /// Explicitly close a synchronously used connection and join its worker.
    pub fn close_sync(&mut self) -> Result<(), FrankenError> {
        if !self.close_admitted {
            drop(self.close_response.take());
            let Some(cmd_tx) = self.cmd_tx.as_ref() else {
                return self.finish_close_sync();
            };
            let (tx, rx) = response_channel();
            self.close_response = Some(rx);
            let command = Command::Close {
                tx,
                #[cfg(test)]
                force_error: self.force_close_error,
            };
            match cmd_tx.send_sync(move || command) {
                Ok(()) => {
                    self.close_admitted = true;
                    drop(self.cmd_tx.take());
                }
                Err(error) => {
                    drop(self.close_response.take());
                    self.store_terminal_close_result(Err(error));
                    drop(self.cmd_tx.take());
                }
            }
        }
        self.finish_close_sync()
    }
}

impl Drop for AsyncConnection {
    fn drop(&mut self) {
        // CommandSender disconnects and unparks in Drop. JoinHandle Drop
        // detaches; the worker drains admitted commands and closes in place.
        // The worker owns the native-root shutdown sender, so its structured
        // lifetime task remains live through actor cleanup.
        drop(self.cmd_tx.take());
        drop(self.worker.take());
        drop(self.native_root.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::runtime::{Runtime, RuntimeBuilder};
    use fsqlite_types::cx::Cx;
    use std::task::Wake;
    use std::time::{Duration, Instant};

    fn test_runtime() -> Runtime {
        RuntimeBuilder::current_thread()
            .blocking_threads(2, 2)
            .build()
            .expect("test runtime should build")
    }

    async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        std::future::poll_fn(|task_cx| Poll::Ready(future.as_mut().poll(task_cx))).await
    }

    #[derive(Default)]
    struct CountingWake {
        count: AtomicUsize,
    }

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.count.fetch_add(1, Ordering::AcqRel);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.count.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn counting_waker(counter: &Arc<CountingWake>) -> Waker {
        Waker::from(Arc::clone(counter))
    }

    fn poll_with_native<F: Future>(
        mut future: Pin<&mut F>,
        native_cx: &NativeCx,
        waker: &Waker,
    ) -> Poll<F::Output> {
        let _current = NativeCx::set_current(Some(native_cx.clone()));
        let mut task_cx = Context::from_waker(waker);
        future.as_mut().poll(&mut task_cx)
    }

    fn independently_cancellable_cx() -> Cx {
        let cx = Cx::new();
        cx.set_native_cx(NativeCx::for_testing());
        cx
    }

    fn block_worker(
        conn: &AsyncConnection,
    ) -> (
        sync_mpsc::SyncSender<()>,
        ResponseReceiver<Result<(), FrankenError>>,
    ) {
        let (entered_tx, entered_rx) = sync_mpsc::sync_channel(1);
        let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
        let (response_tx, response_rx) = response_channel();
        let admitted = conn
            .sender()
            .expect("test connection sender")
            .try_send_for_test(Command::TestBlock {
                entered: entered_tx,
                release: release_rx,
                tx: response_tx,
            });
        assert!(admitted.is_ok(), "worker gate should be admitted");
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker should enter test gate");
        (release_tx, response_rx)
    }

    fn enqueue_worker_noops(
        conn: &AsyncConnection,
        count: usize,
    ) -> Vec<ResponseReceiver<Result<(), FrankenError>>> {
        let mut responses = Vec::with_capacity(count);
        for _ in 0..count {
            let (tx, rx) = response_channel();
            let admitted = conn
                .sender()
                .expect("test connection sender")
                .try_send_for_test(Command::TestNoop { tx });
            assert!(admitted.is_ok(), "test command should fill mailbox");
            responses.push(rx);
        }
        responses
    }

    fn fill_worker_queue(
        conn: &AsyncConnection,
    ) -> Vec<ResponseReceiver<Result<(), FrankenError>>> {
        let responses = enqueue_worker_noops(conn, COMMAND_CAPACITY);
        assert_eq!(
            conn.sender()
                .expect("test connection sender")
                .queued_for_test(),
            COMMAND_CAPACITY
        );
        responses
    }

    fn wait_for_send_waiters(sender: &CommandSender, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (_, _, waiters) = sender.admission_counts_for_test();
            if waiters == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "expected {expected} mailbox send waiters, observed {waiters}"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn test_sync_admission_wakes_when_held_native_permit_is_dropped() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        let (release, blocked_response) = block_worker(&conn);
        let queued_responses = enqueue_worker_noops(&conn, COMMAND_CAPACITY - 1);
        let sender = conn.sender().expect("test connection sender");
        let reserve_cx = detached_native_cancel_cx().expect("detached reserve Cx");
        let held_permit = future::block_on(sender.tx().expect("test sender").reserve(&reserve_cx))
            .expect("held permit should reserve the last mailbox slot");
        assert_eq!(
            sender.admission_counts_for_test(),
            (COMMAND_CAPACITY - 1, 1, 0)
        );

        let (response_tx, response_rx) = response_channel();
        let (done_tx, done_rx) = sync_mpsc::sync_channel(1);
        thread::scope(|scope| {
            scope.spawn(|| {
                let result = sender.send_sync(move || Command::TestNoop { tx: response_tx });
                let _ = done_tx.send(result);
            });
            wait_for_send_waiters(sender, 1);

            // The worker is still gated, so this is purely the native MPSC
            // permit-drop wake path. A private Condvar cannot satisfy it.
            drop(held_permit);
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("dropped permit must wake the synchronous FIFO waiter")
                .expect("synchronous waiter should acquire and commit the slot");
        });

        release.send(()).expect("release worker gate");
        recv_worker_response(blocked_response).expect("gate command should finish");
        for response in queued_responses {
            recv_worker_response(response).expect("queued no-op should finish");
        }
        recv_worker_response(response_rx).expect("synchronous no-op should finish");
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn test_sync_and_async_admission_share_native_fifo_order() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        let (release, blocked_response) = block_worker(&conn);
        let queued_responses = enqueue_worker_noops(&conn, COMMAND_CAPACITY - 2);
        let sender = conn.sender().expect("test connection sender");
        let reserve_cx = detached_native_cancel_cx().expect("detached reserve Cx");
        let held_first = future::block_on(sender.tx().expect("test sender").reserve(&reserve_cx))
            .expect("first held permit");
        let held_second = future::block_on(sender.tx().expect("test sender").reserve(&reserve_cx))
            .expect("second held permit");
        assert_eq!(
            sender.admission_counts_for_test(),
            (COMMAND_CAPACITY - 2, 2, 0)
        );

        let (order_tx, order_rx) = sync_mpsc::channel();
        let (sync_response_tx, sync_response_rx) = response_channel();
        let (sync_done_tx, sync_done_rx) = sync_mpsc::sync_channel(1);
        let async_cx = Cx::new();
        let async_native_cx = detached_native_cancel_cx().expect("async polling Cx");
        let async_wake = Arc::new(CountingWake::default());
        let async_waker = counting_waker(&async_wake);
        let (async_response_tx, async_response_rx) = response_channel();

        thread::scope(|scope| {
            let sync_order_tx = order_tx.clone();
            scope.spawn(|| {
                let result = sender.send_sync(move || {
                    sync_order_tx.send("sync").expect("record sync admission");
                    Command::TestNoop {
                        tx: sync_response_tx,
                    }
                });
                let _ = sync_done_tx.send(result);
            });
            wait_for_send_waiters(sender, 1);

            let mut async_send = Box::pin(sender.send_async_inner(
                &async_cx,
                move || {
                    order_tx.send("async").expect("record async admission");
                    Command::TestNoop {
                        tx: async_response_tx,
                    }
                },
                None,
                None,
            ));
            assert!(
                poll_with_native(async_send.as_mut(), &async_native_cx, &async_waker).is_pending(),
                "async sender should queue behind the synchronous sender"
            );
            wait_for_send_waiters(sender, 2);

            drop(held_first);
            sync_done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("head synchronous waiter should wake")
                .expect("head synchronous waiter should commit");
            assert_eq!(
                order_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("sync admission marker"),
                "sync"
            );
            assert!(
                poll_with_native(async_send.as_mut(), &async_native_cx, &async_waker).is_pending(),
                "later async waiter must not overtake while capacity remains full"
            );

            drop(held_second);
            assert!(
                matches!(
                    poll_with_native(async_send.as_mut(), &async_native_cx, &async_waker),
                    Poll::Ready(Ok(()))
                ),
                "second released slot should admit the async waiter"
            );
            assert_eq!(
                order_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("async admission marker"),
                "async"
            );
        });

        release.send(()).expect("release worker gate");
        recv_worker_response(blocked_response).expect("gate command should finish");
        for response in queued_responses {
            recv_worker_response(response).expect("queued no-op should finish");
        }
        recv_worker_response(sync_response_rx).expect("sync no-op should finish");
        recv_worker_response(async_response_rx).expect("async no-op should finish");
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn test_distinct_polling_task_wakers_survive_shared_attached_cx_cancellation() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        let (release, blocked_response) = block_worker(&conn);
        let queued_responses = fill_worker_queue(&conn);
        let sender = conn.sender().expect("test connection sender");

        let attached_owner = detached_native_cancel_cx().expect("attached owner Cx");
        let shared_project_cx = Cx::new();
        shared_project_cx.set_native_cx(attached_owner.clone());

        let owner_cell = NativeOnceCell::<()>::new();
        let mut owner_wait = Box::pin(owner_cell.wait(&attached_owner));
        let owner_wake = Arc::new(CountingWake::default());
        let owner_waker = counting_waker(&owner_wake);
        assert!(
            poll_with_native(owner_wait.as_mut(), &attached_owner, &owner_waker).is_pending(),
            "owner sentinel should register its original waker"
        );

        let task_a_native = detached_native_cancel_cx().expect("task A Cx");
        let task_b_native = detached_native_cancel_cx().expect("task B Cx");
        let task_a_wake = Arc::new(CountingWake::default());
        let task_b_wake = Arc::new(CountingWake::default());
        let task_a_waker = counting_waker(&task_a_wake);
        let task_b_waker = counting_waker(&task_b_wake);
        let built_a = Arc::new(AtomicBool::new(false));
        let built_b = Arc::new(AtomicBool::new(false));
        let built_a_command = Arc::clone(&built_a);
        let built_b_command = Arc::clone(&built_b);
        let (response_a_tx, response_a_rx) = response_channel();
        let (response_b_tx, response_b_rx) = response_channel();
        let mut operation_a = Box::pin(sender.send_async_inner(
            &shared_project_cx,
            move || {
                built_a_command.store(true, Ordering::Release);
                Command::TestNoop { tx: response_a_tx }
            },
            None,
            None,
        ));
        let mut operation_b = Box::pin(sender.send_async_inner(
            &shared_project_cx,
            move || {
                built_b_command.store(true, Ordering::Release);
                Command::TestNoop { tx: response_b_tx }
            },
            None,
            None,
        ));

        assert!(poll_with_native(operation_a.as_mut(), &task_a_native, &task_a_waker).is_pending());
        assert!(poll_with_native(operation_b.as_mut(), &task_b_native, &task_b_waker).is_pending());
        wait_for_send_waiters(sender, 2);

        shared_project_cx.cancel();
        assert!(
            owner_wake.count.load(Ordering::Acquire) > 0,
            "cancelling the shared project Cx must still wake its attached owner"
        );
        assert!(
            task_a_wake.count.load(Ordering::Acquire) > 0,
            "task A must retain an independent cancellation waker"
        );
        assert!(
            task_b_wake.count.load(Ordering::Acquire) > 0,
            "task B must retain an independent cancellation waker"
        );
        assert!(matches!(
            poll_with_native(operation_a.as_mut(), &task_a_native, &task_a_waker),
            Poll::Ready(Err(FrankenError::Interrupt))
        ));
        assert!(matches!(
            poll_with_native(operation_b.as_mut(), &task_b_native, &task_b_waker),
            Poll::Ready(Err(FrankenError::Interrupt))
        ));
        assert!(
            poll_with_native(owner_wait.as_mut(), &attached_owner, &owner_waker).is_ready(),
            "the original attached-owner waiter must remain registered"
        );
        assert!(!built_a.load(Ordering::Acquire));
        assert!(!built_b.load(Ordering::Acquire));
        assert!(response_a_rx.recv_blocking().is_err());
        assert!(response_b_rx.recv_blocking().is_err());
        drop(operation_a);
        drop(operation_b);
        drop(owner_wait);

        release.send(()).expect("release worker gate");
        recv_worker_response(blocked_response).expect("gate command should finish");
        for response in queued_responses {
            recv_worker_response(response).expect("queued no-op should finish");
        }
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn test_attached_project_cx_without_polling_task_cx_fails_explicitly() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        let project_cx = Cx::new();
        project_cx.set_native_cx(NativeCx::for_testing());

        let error = future::block_on(conn.execute(&project_cx, "CREATE TABLE t (id INTEGER)"))
            .expect_err("an attached project Cx is not a polling-task Cx");
        assert!(
            matches!(
                error,
                FrankenError::Internal(detail)
                    if detail.contains("ambient asupersync Cx of the task polling the future")
            ),
            "attached-only failure should explain the execution-context contract"
        );
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn test_materialized_row_callback_can_reenter_same_connection() {
        let conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_batch_sync(
            "CREATE TABLE source (id INTEGER PRIMARY KEY); \
             INSERT INTO source VALUES (1), (2), (3); \
             CREATE TABLE audit (id INTEGER);",
        )
        .expect("fixture setup should succeed");

        let (result_tx, result_rx) = sync_mpsc::sync_channel(1);
        let callback_thread = thread::spawn(move || {
            let mut callback_count = 0;
            let result = conn.query_with_params_for_each_sync(
                "SELECT id FROM source ORDER BY id",
                &[],
                |_row| {
                    let nested_rows = conn.query_sync("SELECT id FROM source")?;
                    if nested_rows.len() != 3 {
                        return Err(FrankenError::Internal(
                            "nested query observed incomplete source rows".to_owned(),
                        ));
                    }
                    conn.execute_sync("INSERT INTO audit VALUES (1)")?;
                    callback_count += 1;
                    Ok(())
                },
            );
            let _ = result_tx.send((conn, result, callback_count));
        });

        let (mut conn, result, callback_count) = result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("same-connection callback reentry must not deadlock");
        callback_thread
            .join()
            .expect("callback thread should not panic");
        result.expect("every callback and nested operation should succeed");
        assert_eq!(callback_count, 3, "all materialized rows must be visited");
        assert_eq!(
            conn.query_sync("SELECT id FROM audit")
                .expect("audit query should succeed")
                .len(),
            3,
            "each callback must complete its nested execute"
        );
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn test_materialized_row_callback_returns_exact_error_and_connection_reuses() {
        let conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_batch_sync(
            "CREATE TABLE source (id INTEGER PRIMARY KEY); \
             INSERT INTO source VALUES (1), (2), (3);",
        )
        .expect("fixture setup should succeed");

        let (result_tx, result_rx) = sync_mpsc::sync_channel(1);
        let callback_thread = thread::spawn(move || {
            let mut callback_count = 0;
            let result = conn.query_with_params_for_each_sync(
                "SELECT id FROM source ORDER BY id",
                &[],
                |_row| {
                    callback_count += 1;
                    let nested_rows = conn.query_sync("SELECT id FROM source")?;
                    if nested_rows.len() != 3 {
                        return Err(FrankenError::Internal(
                            "nested query observed incomplete source rows".to_owned(),
                        ));
                    }
                    if callback_count == 2 {
                        return Err(FrankenError::Internal("callback sentinel".to_owned()));
                    }
                    Ok(())
                },
            );
            let _ = result_tx.send((conn, result, callback_count));
        });

        let (mut conn, result, callback_count) = result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("callback error path must not deadlock");
        callback_thread
            .join()
            .expect("callback thread should not panic");
        assert_eq!(
            callback_count, 2,
            "callback iteration must stop at the first error"
        );
        assert!(
            matches!(
                result,
                Err(FrankenError::Internal(detail)) if detail == "callback sentinel"
            ),
            "the exact callback error must be returned"
        );
        assert_eq!(
            conn.query_sync("SELECT id FROM source")
                .expect("connection should remain reusable")
                .len(),
            3
        );
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn test_async_connection_basic() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
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
            conn.close(&cx).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_async_connection_transaction() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
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
            conn.close(&cx).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_async_connection_cancel() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
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
            let cleanup_cx = Cx::new();
            let _ = conn.close(&cleanup_cx).await;
        });
    }

    #[test]
    fn test_async_connection_execute_batch() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");

            conn.execute_batch(&cx, "CREATE TABLE a (x INTEGER); CREATE TABLE b (y TEXT);")
                .await
                .expect("batch should succeed");

            // Verify both tables exist.
            let _ = conn.query(&cx, "SELECT * FROM a").await.expect("table a");
            let _ = conn.query(&cx, "SELECT * FROM b").await.expect("table b");
            conn.close(&cx).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_async_connection_close() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = conn
                .worker
                .as_ref()
                .expect("test worker handle")
                .lifecycle_for_test();

            conn.close(&cx).await.expect("close should succeed");
            assert_eq!(
                lifecycle.close_attempts_for_test(),
                1,
                "explicit close must call close_in_place exactly once"
            );

            // After close, operations should fail.
            let result = conn.query(&cx, "SELECT 1").await;
            assert!(result.is_err(), "query after close should fail");
        });
    }

    #[test]
    fn test_default_async_open_keeps_task_native_cx_off_actor_root() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let native_parent = NativeCx::current().expect("test runtime Cx");
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("default async open should succeed");

            let has_native_cx = conn
                .request_async(&cx, |tx| Command::TestRootHasNativeCx { tx })
                .await
                .expect("root inspection should succeed");
            assert!(
                !has_native_cx,
                "a task-owned native Cx must not be polled by the separate OS actor"
            );

            conn.close(&cx).await.expect("close should succeed");
            assert!(
                native_parent.checkpoint().is_ok(),
                "connection teardown must not back-cancel the opening task"
            );
        });
    }

    #[test]
    fn test_opening_cx_cancellation_does_not_end_root_lease_before_worker() {
        test_runtime().block_on(async {
            let open_cx = Cx::new();
            let native_parent = NativeCx::current().expect("test runtime Cx");
            let conn = AsyncConnection::open(&open_cx, ":memory:")
                .await
                .expect("default async open should succeed");
            let worker_lifecycle = conn
                .worker
                .as_ref()
                .expect("test worker handle")
                .lifecycle_for_test();
            let root_lifecycle = conn
                .native_root
                .as_ref()
                .expect("test native-root lease")
                .lifecycle_for_test();

            open_cx.cancel();
            for _ in 0..8 {
                future::yield_now().await;
            }
            assert!(
                !root_lifecycle.finished.load(Ordering::Acquire),
                "connection-root cancellation must not discharge the worker-owned shutdown obligation"
            );
            assert!(
                native_parent.checkpoint().is_ok(),
                "connection-root cancellation must remain downward-only"
            );

            drop(conn);
            assert!(
                worker_lifecycle.wait_finished(Duration::from_secs(5)),
                "mailbox disconnect must let the worker close"
            );
            WorkerExit {
                lifecycle: &root_lifecycle,
            }
            .await;
            assert!(root_lifecycle.finished.load(Ordering::Acquire));
        });
    }

    #[test]
    fn test_explicit_env_remains_authoritative_for_async_open() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn =
                AsyncConnection::open_with_env(&cx, ":memory:", ConnectionEnv::default())
                    .await
                    .expect("explicit-env async open should succeed");

            let has_native_cx = conn
                .request_async(&cx, |tx| Command::TestRootHasNativeCx { tx })
                .await
                .expect("root inspection should succeed");
            assert!(
                !has_native_cx,
                "explicit ConnectionEnv must not be replaced by the admission Cx"
            );

            conn.close(&cx).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_queue_full_cancellation_is_clean_before_admission() {
        test_runtime().block_on(async {
            let open_cx = Cx::new();
            let mut conn = AsyncConnection::open(&open_cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&open_cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("create should succeed");

            let (release, blocked_response) = block_worker(&conn);
            let queued_responses = fill_worker_queue(&conn);

            let operation_cx = Cx::new();
            let cancellation_cx = operation_cx.clone();
            let ambient_native = NativeCx::current().expect("test runtime Cx");
            let (waiter_ready_tx, waiter_ready_rx) = sync_mpsc::channel();
            let canceller = thread::spawn(move || {
                waiter_ready_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("admission future should register its cancellation waiter");
                cancellation_cx.cancel();
            });
            let (response_tx, response_rx) = response_channel();
            let payload_built = Arc::new(AtomicBool::new(false));
            let payload_built_for_command = Arc::clone(&payload_built);
            let admission = conn
                .sender()
                .expect("test connection sender")
                .send_async_inner(
                    &operation_cx,
                    move || {
                        payload_built_for_command.store(true, Ordering::Release);
                        Command::Execute {
                            sql: "INSERT INTO t VALUES (1)".to_owned(),
                            tx: response_tx,
                        }
                    },
                    None,
                    Some(waiter_ready_tx),
                )
                .await;
            canceller.join().expect("canceller thread should finish");
            assert!(matches!(admission, Err(FrankenError::Interrupt)));
            drop(response_rx);
            assert!(
                !payload_built.load(Ordering::Acquire),
                "cancelled full-mailbox admission must not clone/build its payload"
            );
            assert!(
                ambient_native.checkpoint().is_ok(),
                "local cancellation must not cancel the runtime task"
            );
            assert_eq!(
                conn.sender()
                    .expect("test connection sender")
                    .queued_for_test(),
                COMMAND_CAPACITY,
                "cx.cancel alone must wake the runtime and return without freeing capacity or admitting the command"
            );

            release.send(()).expect("release worker gate");
            recv_worker_response_async(blocked_response)
                .await
                .expect("blocked command should finish");
            drop(queued_responses);

            let rows = conn
                .query(&open_cx, "SELECT * FROM t")
                .await
                .expect("query should succeed");
            assert!(rows.is_empty(), "cancelled admission must have no effect");
            conn.close(&open_cx).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_local_admission_cancellation_does_not_poison_native_siblings() {
        test_runtime().block_on(async {
            let open_cx = Cx::new();
            let mut conn = AsyncConnection::open(&open_cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&open_cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("create should succeed");

            let ambient_native = NativeCx::current().expect("test runtime Cx");
            let (release, blocked_response) = block_worker(&conn);
            let queued_responses = fill_worker_queue(&conn);
            let cancelled_cx = Cx::new();
            let sibling_cx = Cx::new();
            assert!(cancelled_cx.attached_native_cx().is_none());
            assert!(sibling_cx.attached_native_cx().is_none());

            let mut cancelled = Box::pin(conn.execute(&cancelled_cx, "INSERT INTO t VALUES (1)"));
            assert!(
                poll_once(cancelled.as_mut()).await.is_pending(),
                "first operation must be waiting for mailbox capacity"
            );
            let mut sibling = Box::pin(conn.query(&sibling_cx, "SELECT * FROM t"));
            assert!(
                poll_once(sibling.as_mut()).await.is_pending(),
                "concurrent sibling must also be waiting for capacity"
            );

            cancelled_cx.cancel();
            assert!(
                ambient_native.checkpoint().is_ok(),
                "local operation cancellation must not cancel the ambient native task"
            );
            assert!(
                sibling_cx.checkpoint().is_ok(),
                "local operation cancellation must not cancel a sibling public Cx"
            );

            release.send(()).expect("release worker gate");
            recv_worker_response_async(blocked_response)
                .await
                .expect("blocked command should finish");
            assert!(matches!(cancelled.await, Err(FrankenError::Interrupt)));
            let rows = sibling
                .await
                .expect("concurrent sibling must survive local cancellation");
            assert!(rows.is_empty());
            drop(queued_responses);

            conn.execute(&open_cx, "INSERT INTO t VALUES (2)")
                .await
                .expect("same ambient runtime must remain reusable");
            assert!(
                ambient_native.checkpoint().is_ok(),
                "later work must observe an uncancelled ambient native task"
            );
            let rows = conn
                .query(&open_cx, "SELECT * FROM t")
                .await
                .expect("later unrelated operation should succeed");
            assert_eq!(rows.len(), 1);
            conn.close(&open_cx).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_cancellation_after_reservation_releases_permit_without_admission() {
        test_runtime().block_on(async {
            let open_cx = Cx::new();
            let mut conn = AsyncConnection::open(&open_cx, ":memory:")
                .await
                .expect("open should succeed");
            let operation_cx = independently_cancellable_cx();
            let cancellation_cx = operation_cx.clone();
            let (reserved_tx, reserved_rx) = sync_mpsc::sync_channel(1);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            let canceller = thread::spawn(move || {
                reserved_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("sender should reserve mailbox capacity");
                cancellation_cx.cancel();
                release_tx.send(()).expect("release reserved sender");
            });
            let (response_tx, response_rx) = response_channel();
            let payload_built = Arc::new(AtomicBool::new(false));
            let payload_built_for_command = Arc::clone(&payload_built);

            let result = conn
                .sender()
                .expect("test connection sender")
                .send_async_inner(
                    &operation_cx,
                    move || {
                        payload_built_for_command.store(true, Ordering::Release);
                        Command::TestNoop { tx: response_tx }
                    },
                    Some((reserved_tx, release_rx)),
                    None,
                )
                .await;
            canceller.join().expect("canceller should finish");
            assert!(matches!(result, Err(FrankenError::Interrupt)));
            assert_eq!(
                conn.sender()
                    .expect("test connection sender")
                    .queued_for_test(),
                0,
                "post-reserve cancellation must release capacity without admission"
            );
            assert!(
                response_rx.await.is_err(),
                "unadmitted command must drop its response obligation"
            );
            assert!(
                !payload_built.load(Ordering::Acquire),
                "post-reserve cancellation must not build the command payload"
            );

            conn.close(&open_cx).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_admitted_effect_ignores_later_operation_cancellation() {
        test_runtime().block_on(async {
            let open_cx = Cx::new();
            let mut conn = AsyncConnection::open(&open_cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&open_cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("create should succeed");

            let operation_cx = independently_cancellable_cx();
            let (entered_tx, entered_rx) = sync_mpsc::sync_channel(1);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            let mut operation =
                Box::pin(
                    conn.request_async(&operation_cx, |tx| Command::TestExecuteAfterGate {
                        sql: "INSERT INTO t VALUES (1)".to_owned(),
                        entered: entered_tx,
                        release: release_rx,
                        tx,
                    }),
                );
            assert!(
                poll_once(operation.as_mut()).await.is_pending(),
                "admitted command should await its actor-owned response"
            );
            entered_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("worker should enter admitted command");
            operation_cx.cancel();
            release_tx.send(()).expect("release admitted command");
            assert_eq!(operation.await.expect("admitted effect should complete"), 1);

            let rows = conn
                .query(&open_cx, "SELECT id FROM t")
                .await
                .expect("query should succeed");
            assert_eq!(rows.len(), 1, "admitted mutation must remain observable");
            conn.close(&open_cx).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_raw_transaction_sql_updates_worker_authoritative_state() {
        test_runtime().block_on(async {
            let open_cx = Cx::new();
            let mut conn = AsyncConnection::open(&open_cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&open_cx, "BEGIN")
                .await
                .expect("raw begin should succeed");
            assert!(conn.in_transaction());

            let cancelled_cx = independently_cancellable_cx();
            cancelled_cx.cancel();
            assert!(matches!(
                conn.execute(&cancelled_cx, "COMMIT").await,
                Err(FrankenError::Interrupt)
            ));
            assert!(
                conn.in_transaction(),
                "unadmitted commit must not change worker-published state"
            );

            conn.execute(&open_cx, "COMMIT")
                .await
                .expect("raw commit should succeed");
            assert!(!conn.in_transaction());
            conn.execute(&open_cx, "BEGIN")
                .await
                .expect("second raw begin should succeed");
            assert!(conn.in_transaction());
            conn.execute(&open_cx, "ROLLBACK")
                .await
                .expect("raw rollback should succeed");
            assert!(!conn.in_transaction());

            conn.close(&open_cx).await.expect("close should succeed");
        });
    }

    #[test]
    fn test_drop_is_nonblocking_and_worker_closes_after_drain() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = conn
                .worker
                .as_ref()
                .expect("test worker handle")
                .lifecycle_for_test();
            let root_lifecycle = conn
                .native_root
                .as_ref()
                .expect("test native-root lease")
                .lifecycle_for_test();
            let (release, blocked_response) = block_worker(&conn);

            let (dropped_tx, dropped_rx) = sync_mpsc::sync_channel(1);
            let dropper = thread::spawn(move || {
                drop(conn);
                let _ = dropped_tx.send(());
            });
            dropped_rx
                .recv_timeout(Duration::from_millis(500))
                .expect("AsyncConnection::drop must not join a blocked worker");
            dropper.join().expect("dropper should finish");
            assert!(
                !root_lifecycle.wait_finished(Duration::from_millis(20)),
                "native root must remain live while the worker still uses its Cx"
            );

            release.send(()).expect("release worker gate");
            recv_worker_response_async(blocked_response)
                .await
                .expect("admitted command should drain");
            assert!(
                lifecycle.wait_finished(Duration::from_secs(5)),
                "detached worker should close after draining admitted commands"
            );
            WorkerExit {
                lifecycle: &root_lifecycle,
            }
            .await;
            assert!(
                root_lifecycle.finished.load(Ordering::Acquire),
                "native-root task should exit after worker cleanup releases its obligation"
            );
        });
    }

    #[test]
    fn test_drop_rolls_back_active_transaction_before_worker_exit() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp
            .path()
            .join("async-drop-rollback.db")
            .to_string_lossy()
            .into_owned();

        let conn = AsyncConnection::open_sync(path.clone()).expect("open should succeed");
        conn.execute_sync("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .expect("create should succeed");
        conn.execute_sync("BEGIN").expect("begin should succeed");
        conn.execute_sync("INSERT INTO t VALUES (1)")
            .expect("insert should succeed");
        assert!(conn.in_transaction());
        let lifecycle = conn
            .worker
            .as_ref()
            .expect("test worker handle")
            .lifecycle_for_test();

        drop(conn);
        assert!(
            lifecycle.wait_finished(Duration::from_secs(5)),
            "drop must let the worker run Connection::close_in_place"
        );
        assert_eq!(
            lifecycle.close_attempts_for_test(),
            1,
            "detached cleanup must call close_in_place exactly once"
        );

        let mut reopened =
            AsyncConnection::open_existing_sync(path).expect("reopen should succeed");
        let rows = reopened
            .query_sync("SELECT * FROM t")
            .expect("query should succeed");
        assert!(
            rows.is_empty(),
            "close must roll back the active transaction"
        );
        reopened.close_sync().expect("close should succeed");
    }

    #[test]
    fn test_worker_panic_disconnects_current_and_queued_responses() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = conn
                .worker
                .as_ref()
                .expect("test worker handle")
                .lifecycle_for_test();
            let root_lifecycle = conn
                .native_root
                .as_ref()
                .expect("test native-root lease")
                .lifecycle_for_test();
            let (cleanup_entered_tx, cleanup_entered_rx) = sync_mpsc::sync_channel(1);
            let (cleanup_release_tx, cleanup_release_rx) = sync_mpsc::sync_channel(1);
            lifecycle.install_cleanup_gate(cleanup_entered_tx, cleanup_release_rx);
            let (release, blocked_response) = block_worker(&conn);

            let (panic_tx, panic_rx) = response_channel();
            assert!(
                conn.sender()
                    .expect("test connection sender")
                    .try_send_for_test(Command::TestPanic { tx: panic_tx })
                    .is_ok()
            );
            let (queued_tx, queued_rx) = response_channel();
            assert!(
                conn.sender()
                    .expect("test connection sender")
                    .try_send_for_test(Command::TestNoop { tx: queued_tx })
                    .is_ok()
            );

            release.send(()).expect("release worker gate");
            recv_worker_response_async(blocked_response)
                .await
                .expect("gate command should finish");
            cleanup_entered_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("worker should pause after closing mailbox admission");
            assert!(
                recv_worker_response_async(panic_rx).await.is_err(),
                "panicking command response must resolve as disconnected"
            );
            assert!(
                recv_worker_response_async(queued_rx).await.is_err(),
                "queued response must resolve when worker unwinds"
            );

            let sender = conn.sender().expect("test connection sender");
            let late_payload_built = Arc::new(AtomicBool::new(false));
            let late_payload_built_for_command = Arc::clone(&late_payload_built);
            let (late_response_tx, late_response_rx) = response_channel();
            let (late_result_tx, late_result_rx) = sync_mpsc::sync_channel(1);
            let observed_before_cleanup = thread::scope(|scope| {
                scope.spawn(|| {
                    let result = sender.send_sync(move || {
                        late_payload_built_for_command.store(true, Ordering::Release);
                        Command::TestNoop {
                            tx: late_response_tx,
                        }
                    });
                    let _ = late_result_tx.send(result);
                });
                let observed = late_result_rx.recv_timeout(Duration::from_millis(500));
                cleanup_release_tx
                    .send(())
                    .expect("release worker cleanup gate");
                observed
            });
            let late_error = observed_before_cleanup
                .expect("mailbox admission must close before slow cleanup")
                .expect_err("late admission should observe worker death");
            assert!(
                matches!(late_error, FrankenError::Internal(detail) if detail.contains("worker thread terminated unexpectedly"))
            );
            assert!(
                !late_payload_built.load(Ordering::Acquire),
                "disconnected admission must not build its command payload"
            );
            assert!(late_response_rx.recv_blocking().is_err());
            assert!(
                lifecycle.wait_finished(Duration::from_secs(5)),
                "worker panic cleanup should terminate the actor"
            );
            WorkerExit {
                lifecycle: &root_lifecycle,
            }
            .await;
            assert!(
                root_lifecycle.finished.load(Ordering::Acquire),
                "worker panic cleanup should release the native-root task"
            );
            assert_eq!(
                lifecycle.close_attempts_for_test(),
                1,
                "panic cleanup must call close_in_place exactly once"
            );
            drop(conn);
        });
    }

    #[test]
    fn test_close_cancellation_after_admission_still_waits_for_shutdown() {
        test_runtime().block_on(async {
            let open_cx = Cx::new();
            let mut conn = AsyncConnection::open(&open_cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = conn
                .worker
                .as_ref()
                .expect("test worker handle")
                .lifecycle_for_test();
            let root_lifecycle = conn
                .native_root
                .as_ref()
                .expect("test native-root lease")
                .lifecycle_for_test();
            let (release, blocked_response) = block_worker(&conn);
            let close_cx = independently_cancellable_cx();
            let mut close = Box::pin(conn.close(&close_cx));
            assert!(
                poll_once(close.as_mut()).await.is_pending(),
                "close should be admitted behind the blocked command"
            );
            close_cx.cancel();
            release.send(()).expect("release worker gate");
            recv_worker_response_async(blocked_response)
                .await
                .expect("blocked command should finish");
            close
                .await
                .expect("admitted close must ignore later operation cancellation");
            assert!(
                lifecycle.wait_finished(Duration::from_secs(1)),
                "close must await worker termination"
            );
            assert!(
                root_lifecycle.wait_finished(Duration::from_secs(1)),
                "close must await native-root task termination"
            );
            assert!(conn.worker.is_none());
            assert!(conn.native_root.is_none());
        });
    }

    #[test]
    fn test_dropped_admitted_close_retry_observes_result_and_exit_receipts() {
        test_runtime().block_on(async {
            let open_cx = Cx::new();
            let mut conn = AsyncConnection::open(&open_cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.force_close_error = true;
            let worker_lifecycle = conn
                .worker
                .as_ref()
                .expect("test worker handle")
                .lifecycle_for_test();
            let root_lifecycle = conn
                .native_root
                .as_ref()
                .expect("test native-root lease")
                .lifecycle_for_test();
            let (release, blocked_response) = block_worker(&conn);
            let close_cx = independently_cancellable_cx();
            let mut first_close = Box::pin(conn.close(&close_cx));
            assert!(
                poll_once(first_close.as_mut()).await.is_pending(),
                "first close should be admitted behind the blocked command"
            );
            close_cx.cancel();
            drop(first_close);
            assert!(
                conn.close_admitted && conn.cmd_tx.is_none(),
                "dropping the future must retain the admitted close observation"
            );

            release.send(()).expect("release worker gate");
            recv_worker_response_async(blocked_response)
                .await
                .expect("blocked command should finish");
            let error = conn
                .close(&open_cx)
                .await
                .expect_err("retry must preserve the first close's terminal error");
            assert!(
                matches!(
                    &error,
                    FrankenError::Internal(detail)
                        if detail == "intentional async-api close failure"
                ),
                "retry returned a different close result: {error}"
            );
            assert!(
                worker_lifecycle.wait_finished(Duration::from_secs(1)),
                "retry must observe the worker exit receipt"
            );
            assert!(
                root_lifecycle.wait_finished(Duration::from_secs(1)),
                "retry must observe the native child-task exit receipt"
            );
            assert!(conn.worker.is_none());
            assert!(conn.native_root.is_none());
            assert!(conn.close_response.is_none());
            assert_eq!(
                worker_lifecycle.close_attempts_for_test(),
                1,
                "failed explicit close must not be retried during cleanup"
            );
        });
    }

    #[test]
    fn test_open_actor_cancellation_does_not_strand_worker() {
        test_runtime().block_on(async {
            let open_cx = Cx::new();
            let native_parent = NativeCx::current().expect("test runtime Cx");
            let (env, bridge) = rooted_worker_env(&open_cx)
                .await
                .expect("structured root should spawn");
            let (entered_tx, entered_rx) = sync_mpsc::sync_channel(1);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            let request = OpenRequest::Create {
                path: ":memory:".to_owned(),
                env,
            };
            let NativeRootBridge { shutdown, lease } = bridge;
            let root_lifecycle = lease.lifecycle_for_test();
            let (cmd_tx, worker, _in_txn, open_rx) =
                start_worker(request, Some(shutdown), Some((entered_tx, release_rx)))
                    .expect("start worker");
            let lifecycle = worker.lifecycle_for_test();

            entered_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("worker should enter open gate");
            open_cx.cancel();
            release_tx.send(()).expect("release open gate");
            let _terminal_open = open_rx
                .await
                .expect("open actor must publish one terminal result");
            drop(cmd_tx);
            worker.wait_async().await;
            lease.wait_async().await;
            assert!(
                lifecycle.wait_finished(Duration::from_secs(1)),
                "cancelled opening context must not strand the worker"
            );
            assert!(
                native_parent.checkpoint().is_ok(),
                "cancelled open must not back-cancel the opening task"
            );
            assert!(
                root_lifecycle.wait_finished(Duration::from_secs(1)),
                "open cleanup must receive native-root task exit"
            );
        });
    }

    #[test]
    fn test_cancel_between_root_spawn_and_worker_admission_reclaims_root_task() {
        test_runtime().block_on(async {
            let open_cx = Cx::new();
            let native_parent = NativeCx::current().expect("test runtime Cx");
            let (env, bridge) = rooted_worker_env(&open_cx)
                .await
                .expect("structured root should spawn");
            let root_lifecycle = bridge.lease.lifecycle_for_test();
            open_cx.cancel();

            let result = AsyncConnection::open_request_async(
                &open_cx,
                OpenRequest::Create {
                    path: ":memory:".to_owned(),
                    env,
                },
                Some(bridge),
                None,
            )
            .await;
            assert!(matches!(result, Err(FrankenError::Interrupt)));
            assert!(
                root_lifecycle.wait_finished(Duration::from_secs(1)),
                "pre-worker cancellation must reclaim the admitted native-root task"
            );
            assert!(
                native_parent.checkpoint().is_ok(),
                "reclaiming the connection child must not cancel the opening task"
            );
        });
    }
}
