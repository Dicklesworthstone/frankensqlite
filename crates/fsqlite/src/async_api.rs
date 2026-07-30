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
use asupersync::channel::oneshot;
use asupersync::cx::Cx as NativeCx;
use asupersync::runtime::Runtime;
use asupersync::runtime::blocking_pool::BlockingPoolHandle;
use fsqlite_types::cx::Cx;
use futures_lite::future;
use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Command protocol between async methods and the worker thread
// ---------------------------------------------------------------------------

type Responder<T> = std::sync::mpsc::SyncSender<Result<T, FrankenError>>;

const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(10);
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
}

impl WorkerState {
    fn new() -> Self {
        Self {
            phase: AtomicU8::new(WorkerPhase::Idle as u8),
            #[cfg(test)]
            cleanup_calls: AtomicUsize::new(0),
            #[cfg(test)]
            panic_on_cleanup: AtomicBool::new(false),
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
    fn phase(&self) -> WorkerPhase {
        match self.phase.load(Ordering::Acquire) {
            value if value == WorkerPhase::InTransaction as u8 => WorkerPhase::InTransaction,
            value if value == WorkerPhase::Closing as u8 => WorkerPhase::Closing,
            value if value == WorkerPhase::Terminal as u8 => WorkerPhase::Terminal,
            _ => WorkerPhase::Idle,
        }
    }
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
        "AsyncConnection async methods require an asupersync runtime with a blocking pool"
            .to_owned(),
    )
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

async fn recv_sync_response<
    Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    T: Send + 'static,
>(
    cx: &Cx<Caps>,
    rx: mpsc::Receiver<T>,
) -> Result<T, FrankenError> {
    let runtime = Runtime::current_handle().ok_or_else(requires_runtime_err)?;
    let pool = runtime.blocking_handle().ok_or_else(requires_runtime_err)?;
    let native_cx = native_cx_for_local(cx)?;
    let (result_tx, mut result_rx) = oneshot::channel::<Result<T, FrankenError>>();

    pool.spawn(move || {
        let result = rx.recv().map_err(|_| worker_dead_err());
        let _ = result_tx.send_blocking(result);
    });

    match result_rx.recv(&native_cx).await {
        Ok(result) => result,
        Err(oneshot::RecvError::Cancelled) => Err(FrankenError::Interrupt),
        Err(oneshot::RecvError::Closed | oneshot::RecvError::PolledAfterCompletion) => {
            Err(worker_dead_err())
        }
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
    let _ = tx.send(result);
}

fn worker_loop(conn: &Connection, rx: &mpsc::Receiver<Command>, state: &WorkerState) -> WorkerStop {
    loop {
        let cmd = match rx.recv_timeout(WORKER_POLL_INTERVAL) {
            Ok(cmd) => cmd,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
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
                let result =
                    future::block_on(conn.query_with_params_for_each(&sql, &params, |row| {
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
    rx: mpsc::Receiver<Command>,
    state: &WorkerState,
) -> Result<(), FrankenError> {
    let loop_result = catch_unwind(AssertUnwindSafe(|| worker_loop(&conn, &rx, state)));

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

    match (loop_result, cleanup_result) {
        (Ok(_), Ok(result)) => result,
        (Ok(_), Err(cleanup_panic)) => Err(FrankenError::Internal(format!(
            "async worker cleanup panicked: {}",
            panic_payload_text(cleanup_panic.as_ref())
        ))),
        (Err(worker_panic), Ok(Ok(()))) => Err(FrankenError::Internal(format!(
            "async worker command loop panicked: {}",
            panic_payload_text(worker_panic.as_ref())
        ))),
        (Err(worker_panic), Ok(Err(cleanup_error))) => Err(FrankenError::Internal(format!(
            "async worker command loop panicked: {}; close cleanup also failed: {cleanup_error}",
            panic_payload_text(worker_panic.as_ref())
        ))),
        (Err(worker_panic), Err(cleanup_panic)) => Err(FrankenError::Internal(format!(
            "async worker command loop panicked: {}; close cleanup also panicked: {}",
            panic_payload_text(worker_panic.as_ref()),
            panic_payload_text(cleanup_panic.as_ref())
        ))),
    }
}

struct WorkerHandle(JoinHandle<Result<(), FrankenError>>);

impl WorkerHandle {
    fn wait(self) -> Result<(), FrankenError> {
        self.0.join().map_err(|panic| {
            FrankenError::Internal(format!(
                "async worker thread panicked outside its terminal guard: {}",
                panic_payload_text(panic.as_ref())
            ))
        })?
    }
}

fn spawn_worker_thread(
    path: String,
    env: ConnectionEnv,
    cmd_rx: mpsc::Receiver<Command>,
    open_tx: mpsc::SyncSender<Result<(), FrankenError>>,
    state: Arc<WorkerState>,
) -> Result<WorkerHandle, FrankenError> {
    thread::Builder::new()
        .name("fsqlite-worker".to_owned())
        .stack_size(WORKER_STACK_BYTES)
        .spawn(
            move || match future::block_on(Connection::open_with_env(path, env)) {
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
                    let _ = open_tx.send(Ok(()));
                    run_worker_to_terminal(conn, cmd_rx, &state)
                }
                Err(error) => {
                    state.publish_phase(WorkerPhase::Terminal);
                    let _ = open_tx.send(Err(error));
                    Ok(())
                }
            },
        )
        .map(WorkerHandle)
        .map_err(worker_thread_spawn_err)
}

fn wait_for_worker_open(
    open_rx: mpsc::Receiver<Result<(), FrankenError>>,
) -> Result<(), FrankenError> {
    open_rx.recv().map_err(|_| worker_open_err())?
}

fn spawn_worker_join(
    pool: &BlockingPoolHandle,
    worker: WorkerHandle,
) -> mpsc::Receiver<Result<(), FrankenError>> {
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let _join_task = pool.spawn(move || {
        let _ = result_tx.send(worker.wait());
    });
    result_rx
}

async fn wait_for_worker_async(
    native_cx: &NativeCx,
    pool: &BlockingPoolHandle,
    worker: WorkerHandle,
) -> Result<(), FrankenError> {
    let (result_tx, mut result_rx) = oneshot::channel::<Result<(), FrankenError>>();

    // Join and publish from one blocking-pool job. Routing this through
    // `recv_sync_response` would enqueue a second blocking receiver behind the
    // join; a one-thread pool could then deadlock if that receiver ran first.
    pool.spawn(move || {
        let result = worker.wait();
        let _ = result_tx.send_blocking(result);
    });

    match result_rx.recv(native_cx).await {
        Ok(result) => result,
        Err(oneshot::RecvError::Cancelled) => Err(FrankenError::Interrupt),
        Err(oneshot::RecvError::Closed | oneshot::RecvError::PolledAfterCompletion) => {
            Err(worker_dead_err())
        }
    }
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

/// Map a send error (worker died) to a `FrankenError::Internal`.
fn send_err<T>(_: mpsc::SendError<T>) -> FrankenError {
    FrankenError::Internal("async worker thread is no longer running".to_owned())
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
pub struct AsyncConnection {
    cmd_tx: Option<mpsc::SyncSender<Command>>,
    worker: Option<WorkerHandle>,
    /// Worker-published transaction and terminal phase. The dedicated worker is
    /// the only writer, so cancellation cannot leave caller-maintained state
    /// behind the engine's actual transaction state.
    state: Arc<WorkerState>,
}

impl AsyncConnection {
    /// Open a database connection asynchronously with `Cx` integration.
    ///
    /// The `Cx` is checkpointed before the blocking open. On success, a
    /// dedicated large-stack worker thread is spawned to own the `Connection`.
    pub async fn open<Caps>(cx: &Cx<Caps>, path: impl Into<String>) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
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
        let (open_tx, open_rx) = mpsc::sync_channel::<Result<(), FrankenError>>(1);
        let (cmd_tx, cmd_rx) = mpsc::sync_channel::<Command>(32);
        let state = Arc::new(WorkerState::new());
        let worker = spawn_worker_thread(path, env, cmd_rx, open_tx, Arc::clone(&state))?;

        match wait_for_worker_open(open_rx) {
            Ok(()) => Ok(Self {
                cmd_tx: Some(cmd_tx),
                worker: Some(worker),
                state,
            }),
            Err(error) => match worker.wait() {
                Ok(()) => Err(error),
                Err(worker_error) => Err(worker_error),
            },
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
    {
        checkpoint_or_interrupt(cx)?;

        let path = path.into();

        // Response waiters need the caller runtime's blocking pool, but the
        // raw engine itself lives on a dedicated large-stack thread. The
        // connection is !Send, so it must be born on and stay on that thread.
        let (open_tx, open_rx) = mpsc::sync_channel::<Result<(), FrankenError>>(1);
        let (cmd_tx, cmd_rx) = mpsc::sync_channel::<Command>(32);
        let runtime = Runtime::current_handle().ok_or_else(requires_runtime_err)?;
        let pool = runtime.blocking_handle().ok_or_else(requires_runtime_err)?;
        let native_cx = native_cx_for_local(cx)?;
        let state = Arc::new(WorkerState::new());
        let worker = spawn_worker_thread(path, env, cmd_rx, open_tx, Arc::clone(&state))?;

        // Wait for the open result. On cancellation, dropping the command
        // sender releases a successfully opened worker from its receive loop,
        // while the blocking pool owns the OS-thread join to completion.
        match recv_sync_response(cx, open_rx).await {
            Ok(Ok(())) => Ok(Self {
                cmd_tx: Some(cmd_tx),
                worker: Some(worker),
                state,
            }),
            Ok(Err(error)) => {
                drop(cmd_tx);
                match wait_for_worker_async(&native_cx, &pool, worker).await {
                    Ok(()) | Err(FrankenError::Interrupt) => Err(error),
                    Err(worker_error) => Err(worker_error),
                }
            }
            Err(FrankenError::Interrupt) => {
                drop(cmd_tx);
                let _join_rx = spawn_worker_join(&pool, worker);
                Err(FrankenError::Interrupt)
            }
            Err(error) => {
                drop(cmd_tx);
                match wait_for_worker_async(&native_cx, &pool, worker).await {
                    Ok(()) | Err(FrankenError::Interrupt) => Err(error),
                    Err(worker_error) => Err(worker_error),
                }
            }
        }
    }

    /// Return a reference to the command sender, or an error if the worker is gone.
    fn sender(&self) -> Result<&mpsc::SyncSender<Command>, FrankenError> {
        self.cmd_tx
            .as_ref()
            .ok_or_else(|| FrankenError::Internal("AsyncConnection has been closed".to_owned()))
    }

    /// Validate and prepare one SQL statement on the dedicated worker.
    ///
    /// This is the synchronous-consumer counterpart to the async methods
    /// below. It intentionally performs no cancellation check and blocks the
    /// caller until the worker responds.
    pub fn prepare_sync(&self, sql: &str) -> Result<(), FrankenError> {
        let (tx, rx) = mpsc::sync_channel(1);
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
        let (tx, rx) = mpsc::sync_channel(1);
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
        let (tx, rx) = mpsc::sync_channel(1);
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
    /// stream, releases the worker, and returns that callback error.
    pub fn query_with_params_for_each_sync<F>(
        &self,
        sql: &str,
        params: &[SqliteValue],
        mut f: F,
    ) -> Result<(), FrankenError>
    where
        F: FnMut(&Row) -> Result<(), FrankenError>,
    {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
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
        let (tx, rx) = mpsc::sync_channel(1);
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
        let (tx, rx) = mpsc::sync_channel(1);
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
        let (tx, rx) = mpsc::sync_channel(1);
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
        let (tx, rx) = mpsc::sync_channel(1);
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
        let (tx, rx) = mpsc::sync_channel(1);
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
        let (tx, rx) = mpsc::sync_channel(1);
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
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::BeginTransaction { tx })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Commit the active transaction through the dedicated worker.
    pub fn commit_transaction_sync(&self) -> Result<(), FrankenError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::CommitTransaction { tx })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Roll back the active transaction through the dedicated worker.
    pub fn rollback_transaction_sync(&self) -> Result<(), FrankenError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::RollbackTransaction { tx })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Return the worker-owned connection's last inserted row identifier.
    pub fn last_insert_rowid_sync(&self) -> Result<i64, FrankenError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::LastInsertRowid { tx })
            .map_err(send_err)?;
        recv_worker_response(rx)
    }

    /// Execute a SQL query and return all result rows.
    pub async fn query<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<Vec<Row>, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        checkpoint_or_interrupt(cx)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::Query {
                sql: sql.to_owned(),
                tx,
            })
            .map_err(send_err)?;
        recv_sync_response(cx, rx).await?
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
        checkpoint_or_interrupt(cx)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::QueryWithParams {
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
            .map_err(send_err)?;
        recv_sync_response(cx, rx).await?
    }

    /// Execute a query and return exactly one row.
    pub async fn query_row<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<Row, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        checkpoint_or_interrupt(cx)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::QueryRow {
                sql: sql.to_owned(),
                tx,
            })
            .map_err(send_err)?;
        recv_sync_response(cx, rx).await?
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
        checkpoint_or_interrupt(cx)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::QueryRowWithParams {
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
            .map_err(send_err)?;
        recv_sync_response(cx, rx).await?
    }

    /// Execute SQL and return the number of affected/output rows.
    pub async fn execute<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<usize, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        checkpoint_or_interrupt(cx)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::Execute {
                sql: sql.to_owned(),
                tx,
            })
            .map_err(send_err)?;
        recv_sync_response(cx, rx).await?
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
        checkpoint_or_interrupt(cx)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::ExecuteWithParams {
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
            .map_err(send_err)?;
        recv_sync_response(cx, rx).await?
    }

    /// Execute zero or more SQL statements separated by semicolons.
    pub async fn execute_batch<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        checkpoint_or_interrupt(cx)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::ExecuteBatch {
                sql: sql.to_owned(),
                tx,
            })
            .map_err(send_err)?;
        recv_sync_response(cx, rx).await?
    }

    /// Begin a transaction.
    pub async fn begin_transaction<Caps>(&self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        checkpoint_or_interrupt(cx)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::BeginTransaction { tx })
            .map_err(send_err)?;
        recv_sync_response(cx, rx).await?
    }

    /// Commit the active transaction.
    pub async fn commit_transaction<Caps>(&self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        checkpoint_or_interrupt(cx)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::CommitTransaction { tx })
            .map_err(send_err)?;
        recv_sync_response(cx, rx).await?
    }

    /// Roll back the active transaction.
    pub async fn rollback_transaction<Caps>(&self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        checkpoint_or_interrupt(cx)?;
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender()?
            .send(Command::RollbackTransaction { tx })
            .map_err(send_err)?;
        recv_sync_response(cx, rx).await?
    }

    /// Returns `true` if an explicit transaction is currently active.
    ///
    /// This is a cheap local read — no round-trip to the worker thread.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.state.in_transaction()
    }

    /// Explicitly close the connection, returning any error from the close operation.
    ///
    /// After this call, all subsequent operations will return an error.
    /// The worker thread is joined before returning.
    pub async fn close<Caps>(&mut self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        checkpoint_or_interrupt(cx)?;
        let runtime = Runtime::current_handle().ok_or_else(requires_runtime_err)?;
        let pool = runtime.blocking_handle().ok_or_else(requires_runtime_err)?;
        let native_cx = native_cx_for_local(cx)?;

        let cmd_tx = self.cmd_tx.take();
        let worker = self.worker.take();
        if cmd_tx.is_none() && worker.is_none() {
            return Ok(());
        }

        if let Some(cmd_tx) = cmd_tx {
            let _ = cmd_tx.try_send(Command::Close);
            drop(cmd_tx);
        }

        let worker = worker.ok_or_else(worker_dead_err)?;
        wait_for_worker_async(&native_cx, &pool, worker).await
    }

    /// Explicitly close a synchronously used connection and join its worker.
    pub fn close_sync(&mut self) -> Result<(), FrankenError> {
        let cmd_tx = self.cmd_tx.take();
        let worker = self.worker.take();
        if cmd_tx.is_none() && worker.is_none() {
            return Ok(());
        }

        if let Some(cmd_tx) = cmd_tx {
            let _ = cmd_tx.try_send(Command::Close);
            drop(cmd_tx);
        }

        worker.ok_or_else(worker_dead_err)?.wait()
    }
}

impl Drop for AsyncConnection {
    fn drop(&mut self) {
        if let Some(cmd_tx) = self.cmd_tx.take() {
            let _ = cmd_tx.try_send(Command::Shutdown);
            drop(cmd_tx);
        }
        if let Some(handle) = self.worker.take() {
            let _ = handle.wait();
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
    fn transaction_state_is_worker_published_when_response_is_abandoned() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");

        let (begin_tx, begin_rx) = mpsc::sync_channel(1);
        drop(begin_rx);
        conn.sender()
            .expect("worker sender")
            .send(Command::BeginTransaction { tx: begin_tx })
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
            .send(Command::RollbackTransaction { tx: rollback_tx })
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
            let sender = conn.cmd_tx.take().expect("shutdown sender");
            sender
                .try_send(Command::Shutdown)
                .expect("shutdown command should fit");
            drop(sender);
            conn.worker
                .take()
                .expect("shutdown worker handle")
                .wait()
                .expect("shutdown cleanup should succeed");
            assert_terminal_cleanup_once(&state);
        }

        {
            let mut conn =
                AsyncConnection::open_sync(":memory:").expect("disconnect worker should open");
            let state = Arc::clone(&conn.state);
            drop(conn.cmd_tx.take().expect("disconnect sender"));
            conn.worker
                .take()
                .expect("disconnect worker handle")
                .wait()
                .expect("disconnect cleanup should succeed");
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
