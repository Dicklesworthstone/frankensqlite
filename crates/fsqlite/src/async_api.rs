//! Async-native wrapper around [`Connection`] for use with asupersync's `Cx` capability context.
//!
//! Because [`Connection`] is `!Send` (it uses `Rc<RefCell<..>>` internally), this module
//! provides an [`AsyncConnection`] that runs a dedicated worker task owning the
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
use asupersync::runtime::{BlockingTaskHandle, Runtime, RuntimeBuilder, RuntimeHandle};
use fsqlite_types::cx::Cx;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Command protocol between async methods and the worker task
// ---------------------------------------------------------------------------

type Responder<T> = std::sync::mpsc::SyncSender<Result<T, FrankenError>>;

const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A command sent from an async method to the worker task.
enum Command {
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
    Close {
        tx: Responder<()>,
    },
    Shutdown,
}

fn worker_open_err() -> FrankenError {
    FrankenError::Internal("async worker task terminated during open".to_owned())
}

fn worker_dead_err() -> FrankenError {
    FrankenError::Internal("async worker task terminated unexpectedly".to_owned())
}

fn requires_runtime_err() -> FrankenError {
    FrankenError::Internal(
        "AsyncConnection async methods require an asupersync runtime with a blocking pool"
            .to_owned(),
    )
}

fn worker_spawn_err() -> FrankenError {
    FrankenError::Internal(
        "failed to spawn async worker task: runtime has no blocking pool".to_owned(),
    )
}

fn blocking_wait_send_err<T>(_: oneshot::SendError<Result<T, FrankenError>>) {}

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
    let waiter_cx = native_cx.clone();
    let (result_tx, mut result_rx) = oneshot::channel::<Result<T, FrankenError>>();

    pool.spawn(move || {
        let result = rx.recv().map_err(|_| worker_dead_err());
        let _ = result_tx
            .send(&waiter_cx, result)
            .map_err(blocking_wait_send_err);
    });

    match result_rx.recv(&native_cx).await {
        Ok(result) => result,
        Err(oneshot::RecvError::Cancelled) => Err(FrankenError::Interrupt),
        Err(oneshot::RecvError::Closed | oneshot::RecvError::PolledAfterCompletion) => {
            Err(worker_dead_err())
        }
    }
}

// ---------------------------------------------------------------------------
// Worker task
// ---------------------------------------------------------------------------

fn worker_loop(mut conn: Connection, rx: mpsc::Receiver<Command>) {
    loop {
        let cmd = match rx.recv_timeout(WORKER_POLL_INTERVAL) {
            Ok(cmd) => cmd,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        };

        match cmd {
            Command::Query { sql, tx } => {
                let _ = tx.send(conn.query(&sql));
            }
            Command::QueryWithParams { sql, params, tx } => {
                let _ = tx.send(conn.query_with_params(&sql, &params));
            }
            Command::QueryRow { sql, tx } => {
                let _ = tx.send(conn.query_row(&sql));
            }
            Command::QueryRowWithParams { sql, params, tx } => {
                let _ = tx.send(conn.query_row_with_params(&sql, &params));
            }
            Command::Execute { sql, tx } => {
                let _ = tx.send(conn.execute(&sql));
            }
            Command::ExecuteWithParams { sql, params, tx } => {
                let _ = tx.send(conn.execute_with_params(&sql, &params));
            }
            Command::ExecuteBatch { sql, tx } => {
                let _ = tx.send(conn.execute_batch(&sql));
            }
            Command::BeginTransaction { tx } => {
                let _ = tx.send(conn.begin_transaction());
            }
            Command::CommitTransaction { tx } => {
                let _ = tx.send(conn.commit_transaction());
            }
            Command::RollbackTransaction { tx } => {
                let _ = tx.send(conn.rollback_transaction());
            }
            Command::Close { tx } => {
                // Close the connection explicitly (rolls back any active txn,
                // runs a passive WAL checkpoint).
                let _ = tx.send(conn.close_in_place());
                return;
            }
            Command::Shutdown => {
                return;
            }
        }
    }
}

fn spawn_worker_task(
    runtime: &RuntimeHandle,
    path: String,
    env: ConnectionEnv,
    cmd_rx: mpsc::Receiver<Command>,
    open_tx: mpsc::SyncSender<Result<(), FrankenError>>,
) -> Result<BlockingTaskHandle, FrankenError> {
    runtime
        .spawn_blocking(move || match Connection::open_with_env(path, env) {
            Ok(conn) => {
                let _ = open_tx.send(Ok(()));
                worker_loop(conn, cmd_rx);
            }
            Err(error) => {
                let _ = open_tx.send(Err(error));
            }
        })
        .ok_or_else(worker_spawn_err)
}

fn build_owned_runtime() -> Result<Runtime, FrankenError> {
    RuntimeBuilder::current_thread()
        .blocking_threads(1, 1)
        .build()
        .map_err(|error| {
            FrankenError::Internal(format!("failed to build async-api runtime: {error}"))
        })
}

fn current_or_owned_runtime() -> Result<(Option<Runtime>, RuntimeHandle), FrankenError> {
    if let Some(handle) = Runtime::current_handle()
        && handle.blocking_handle().is_some()
    {
        return Ok((None, handle));
    }

    let runtime = build_owned_runtime()?;
    let handle = runtime.handle();
    Ok((Some(runtime), handle))
}

fn wait_for_worker_open(
    open_rx: mpsc::Receiver<Result<(), FrankenError>>,
) -> Result<(), FrankenError> {
    open_rx.recv().map_err(|_| worker_open_err())?
}

fn join_worker_task(handle: BlockingTaskHandle) {
    handle.wait();
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
    FrankenError::Internal("async worker task is no longer running".to_owned())
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
/// The connection itself lives on a dedicated worker task (because
/// [`Connection`] is `!Send`). Commands are dispatched via an internal channel
/// and results flow back through response waiters owned by the caller runtime.
///
/// # Shutdown
///
/// When `AsyncConnection` is dropped, the worker task is signalled to shut
/// down. The underlying [`Connection`] is closed on the worker task as part
/// of its normal drop sequence.
///
/// For explicit, error-checked shutdown use [`close`](Self::close).
pub struct AsyncConnection {
    cmd_tx: Option<mpsc::SyncSender<Command>>,
    worker: Option<BlockingTaskHandle>,
    owned_runtime: Option<Runtime>,
    /// Tracks whether the worker task's connection has an active transaction.
    /// Updated by `begin_transaction`, `commit_transaction`, and
    /// `rollback_transaction` to allow `in_transaction()` to be a cheap local
    /// read without a round-trip to the worker.
    in_txn: Arc<AtomicBool>,
}

impl AsyncConnection {
    /// Open a database connection asynchronously with `Cx` integration.
    ///
    /// The `Cx` is checkpointed before the blocking open. On success, a
    /// dedicated worker task is spawned to own the `Connection`.
    pub async fn open<Caps>(cx: &Cx<Caps>, path: impl Into<String>) -> Result<Self, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        Self::open_with_env(cx, path, ConnectionEnv::default()).await
    }

    /// Open a database connection without a capability context (convenience).
    ///
    /// Equivalent to calling [`Connection::open`] and wrapping the result.
    /// No cancellation check is performed.
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
        let (owned_runtime, runtime_handle) = current_or_owned_runtime()?;
        let worker = spawn_worker_task(&runtime_handle, path, env, cmd_rx, open_tx)?;

        match wait_for_worker_open(open_rx) {
            Ok(()) => Ok(Self {
                cmd_tx: Some(cmd_tx),
                worker: Some(worker),
                owned_runtime,
                in_txn: Arc::new(AtomicBool::new(false)),
            }),
            Err(error) => {
                join_worker_task(worker);
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
    {
        checkpoint_or_interrupt(cx)?;

        let path = path.into();

        // Open the connection on a runtime-owned blocking task (it is !Send,
        // so it must be born on and stay on the worker task's thread).
        let (open_tx, open_rx) = mpsc::sync_channel::<Result<(), FrankenError>>(1);
        let (cmd_tx, cmd_rx) = mpsc::sync_channel::<Command>(32);
        let runtime = Runtime::current_handle().ok_or_else(requires_runtime_err)?;
        let worker = spawn_worker_task(&runtime, path, env, cmd_rx, open_tx)?;

        // Wait for the open result.
        if let Err(error) = recv_sync_response(cx, open_rx).await? {
            join_worker_task(worker);
            return Err(error);
        }

        Ok(Self {
            cmd_tx: Some(cmd_tx),
            worker: Some(worker),
            owned_runtime: None,
            in_txn: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Return a reference to the command sender, or an error if the worker is gone.
    fn sender(&self) -> Result<&mpsc::SyncSender<Command>, FrankenError> {
        self.cmd_tx
            .as_ref()
            .ok_or_else(|| FrankenError::Internal("AsyncConnection has been closed".to_owned()))
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
        let result: Result<(), FrankenError> = recv_sync_response(cx, rx).await?;
        if result.is_ok() {
            self.in_txn.store(true, Ordering::Release);
        }
        result
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
        let result: Result<(), FrankenError> = recv_sync_response(cx, rx).await?;
        if result.is_ok() {
            self.in_txn.store(false, Ordering::Release);
        }
        result
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
        let result: Result<(), FrankenError> = recv_sync_response(cx, rx).await?;
        if result.is_ok() {
            self.in_txn.store(false, Ordering::Release);
        }
        result
    }

    /// Returns `true` if an explicit transaction is currently active.
    ///
    /// This is a cheap local read — no round-trip to the worker task.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.in_txn.load(Ordering::Acquire)
    }

    /// Explicitly close the connection, returning any error from the close operation.
    ///
    /// After this call, all subsequent operations will return an error.
    /// The worker task is joined before returning.
    pub async fn close<Caps>(&mut self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        checkpoint_or_interrupt(cx)?;

        if let Some(cmd_tx) = self.cmd_tx.take() {
            let (tx, rx) = mpsc::sync_channel(1);
            cmd_tx.send(Command::Close { tx }).map_err(send_err)?;
            let result = recv_sync_response(cx, rx).await?;

            if let Some(handle) = self.worker.take() {
                join_worker_task(handle);
            }
            self.owned_runtime = None;

            result
        } else {
            // Already closed.
            Ok(())
        }
    }
}

impl Drop for AsyncConnection {
    fn drop(&mut self) {
        if let Some(cmd_tx) = self.cmd_tx.take() {
            let _ = cmd_tx.send(Command::Shutdown);
        }
        if let Some(handle) = self.worker.take() {
            join_worker_task(handle);
        }
        self.owned_runtime = None;
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
