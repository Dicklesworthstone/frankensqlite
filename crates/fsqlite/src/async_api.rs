//! Async-native wrapper around [`Connection`] for use with asupersync's `Cx` capability context.
//!
//! Because [`Connection`] is `!Send` (it uses `Rc<RefCell<..>>` internally), this module
//! provides an [`AsyncConnection`] that runs a dedicated worker thread owning the
//! `Connection`. All SQL operations are dispatched to the worker via a command channel
//! and results are returned through response channels.
//!
//! Every async method accepts a `&Cx`. Cancellation before mailbox admission
//! fails without touching the connection. After admission, a capability-free
//! relay asks the worker-owned, root-derived operation to stop while the caller
//! continues draining the worker's one authoritative result.
//!
//! # Feature gate
//!
//! This module is only available when the `async-api` feature is enabled on `fsqlite`.
//!
//! # Example
//!
//! ```
//! use fsqlite::{AsyncConnection, SqliteValue};
//! use fsqlite_types::cx::Cx;
//!
//! async fn example(cx: &Cx) -> Result<(), fsqlite::FrankenError> {
//!     let mut conn = AsyncConnection::open(cx, ":memory:").await?;
//!     conn.execute(cx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").await?;
//!     let mut transaction = conn.begin_transaction(cx).await?;
//!     transaction.execute_with_params(
//!         cx,
//!         "INSERT INTO t VALUES (?1, ?2)",
//!         &[SqliteValue::Integer(1), SqliteValue::Text("hello".into())],
//!     ).await?;
//!     transaction.commit(cx).await?;
//!     drop(transaction);
//!     let rows = conn.query(cx, "SELECT * FROM t").await?;
//!     assert_eq!(rows.len(), 1);
//!     conn.close(cx).await?;
//!     Ok(())
//! }
//! ```

use crate::{Connection, ConnectionEnv, FrankenError, Row, SqliteValue};
use asupersync::channel::mpsc as async_mpsc;
use asupersync::cx::{Cx as NativeCx, cap as native_cap};
use asupersync::sync::OnceCell as NativeOnceCell;
use fsqlite_ast::Statement;
use fsqlite_parser::Parser;
use fsqlite_types::cx::{CancellationRelay, Cx};
use futures_lite::future;
use std::cell::RefCell;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc as sync_mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};

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

const ADMISSION_PENDING: u8 = 0;
const ADMISSION_ACCEPTED: u8 = 1;
const ADMISSION_FAILED: u8 = 2;
const CONSUMER_ACTIVE: u8 = 0;
const CONSUMER_COMPLETE: u8 = 1;
const CONSUMER_ABANDONED: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TransactionToken {
    connection_id: u64,
    generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TransactionTerminalOutcome {
    Committed,
    RolledBack {
        reason: Arc<str>,
    },
    Unknown {
        operation: &'static str,
        detail: Arc<str>,
    },
}

impl TransactionTerminalOutcome {
    fn nonterminal_error(&self) -> FrankenError {
        match self {
            Self::Committed => FrankenError::NoActiveTransaction,
            Self::RolledBack { reason } => FrankenError::TransactionRolledBack {
                reason: reason.to_string(),
            },
            Self::Unknown { operation, detail } => FrankenError::TransactionOutcomeUnknown {
                operation,
                detail: detail.to_string(),
            },
        }
    }

    fn commit_result(&self) -> Result<(), FrankenError> {
        match self {
            Self::Committed => Ok(()),
            Self::RolledBack { reason } => Err(FrankenError::TransactionRolledBack {
                reason: reason.to_string(),
            }),
            Self::Unknown { operation, detail } => Err(FrankenError::TransactionOutcomeUnknown {
                operation,
                detail: detail.to_string(),
            }),
        }
    }

    fn rollback_result(&self) -> Result<(), FrankenError> {
        match self {
            Self::RolledBack { .. } => Ok(()),
            Self::Committed => Err(FrankenError::NoActiveTransaction),
            Self::Unknown { operation, detail } => Err(FrankenError::TransactionOutcomeUnknown {
                operation,
                detail: detail.to_string(),
            }),
        }
    }
}

/// Per-generation terminal fact written only by the connection actor.
///
/// The transaction handle retains this bounded cell, so a terminal fact
/// survives a dropped response, a later transaction generation, and worker
/// teardown without an unbounded actor-side tombstone map.
struct TransactionTerminalReceipt {
    token: TransactionToken,
    outcome: Mutex<Option<TransactionTerminalOutcome>>,
}

impl TransactionTerminalReceipt {
    fn new(token: TransactionToken) -> Self {
        Self {
            token,
            outcome: Mutex::new(None),
        }
    }

    fn publish(&self, token: TransactionToken, outcome: TransactionTerminalOutcome) {
        assert_eq!(
            self.token, token,
            "a transaction terminal receipt must be generation exact"
        );
        let mut published = lock_unpoisoned(&self.outcome);
        if let Some(existing) = published.as_ref() {
            assert_eq!(
                existing, &outcome,
                "a transaction generation cannot publish conflicting terminal outcomes"
            );
        } else {
            *published = Some(outcome);
        }
    }

    fn outcome(&self) -> Option<TransactionTerminalOutcome> {
        lock_unpoisoned(&self.outcome).clone()
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Lossless actor-side rollback obligation created when [`Transaction`] is
/// dropped unfinished.
///
/// The obligation lives independently of the bounded ordinary command
/// mailbox. The capacity-one wake channel is only an edge notification:
/// `Full` means an earlier wake is already queued, while the mutex-protected
/// token remains the authoritative cleanup state.
struct TransactionDropCleanup {
    connection_id: u64,
    state: Mutex<TransactionDropCleanupState>,
    wake_tx: async_mpsc::Sender<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionDropCleanupAttempt {
    Fresh,
    Retry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionDropCleanupState {
    Idle,
    Pending {
        token: TransactionToken,
        attempt: TransactionDropCleanupAttempt,
    },
    Settling {
        token: TransactionToken,
        attempt: TransactionDropCleanupAttempt,
    },
    Poisoned {
        token: TransactionToken,
    },
}

impl TransactionDropCleanup {
    fn new(connection_id: u64, wake_tx: async_mpsc::Sender<()>) -> Self {
        Self {
            connection_id,
            state: Mutex::new(TransactionDropCleanupState::Idle),
            wake_tx,
        }
    }

    fn request(&self, token: TransactionToken) {
        if token.connection_id != self.connection_id {
            return;
        }
        let should_wake = {
            let mut state = lock_unpoisoned(&self.state);
            match *state {
                TransactionDropCleanupState::Idle => {
                    *state = TransactionDropCleanupState::Pending {
                        token,
                        attempt: TransactionDropCleanupAttempt::Fresh,
                    };
                    true
                }
                TransactionDropCleanupState::Pending {
                    token: existing, ..
                } if existing.generation >= token.generation => false,
                TransactionDropCleanupState::Pending { .. } => {
                    *state = TransactionDropCleanupState::Pending {
                        token,
                        attempt: TransactionDropCleanupAttempt::Fresh,
                    };
                    false
                }
                TransactionDropCleanupState::Settling {
                    token: existing, ..
                }
                | TransactionDropCleanupState::Poisoned { token: existing } => {
                    debug_assert!(
                        existing.generation >= token.generation,
                        "a newer transaction generation cannot exist while an older generation owns cleanup"
                    );
                    false
                }
            }
        };
        if should_wake {
            self.wake();
        }
    }

    fn wake(&self) {
        match self.wake_tx.try_send(()) {
            Ok(()) | Err(async_mpsc::SendError::Full(())) => {}
            Err(async_mpsc::SendError::Disconnected(()) | async_mpsc::SendError::Cancelled(())) => {
                // The worker is already exiting. Its explicit connection close
                // is then the terminal owner of any live transaction.
            }
        }
    }

    fn begin_attempt(&self) -> Option<(TransactionToken, TransactionDropCleanupAttempt)> {
        let mut state = lock_unpoisoned(&self.state);
        let TransactionDropCleanupState::Pending { token, attempt } = *state else {
            return None;
        };
        *state = TransactionDropCleanupState::Settling { token, attempt };
        Some((token, attempt))
    }

    fn resolve(&self, token: TransactionToken) {
        let mut state = lock_unpoisoned(&self.state);
        if matches!(
            *state,
            TransactionDropCleanupState::Settling {
                token: settling,
                ..
            } if settling == token
        ) {
            *state = TransactionDropCleanupState::Idle;
        }
    }

    fn schedule_one_retry(&self, token: TransactionToken, attempt: TransactionDropCleanupAttempt) {
        let should_wake = {
            let mut state = lock_unpoisoned(&self.state);
            if !matches!(
                *state,
                TransactionDropCleanupState::Settling {
                    token: settling,
                    attempt: settling_attempt,
                } if settling == token && settling_attempt == attempt
            ) {
                false
            } else if attempt == TransactionDropCleanupAttempt::Fresh {
                *state = TransactionDropCleanupState::Pending {
                    token,
                    attempt: TransactionDropCleanupAttempt::Retry,
                };
                true
            } else {
                *state = TransactionDropCleanupState::Poisoned { token };
                false
            }
        };
        if should_wake {
            self.wake();
        }
    }

    fn poison(&self, token: TransactionToken) {
        let mut state = lock_unpoisoned(&self.state);
        if matches!(
            *state,
            TransactionDropCleanupState::Settling {
                token: settling,
                ..
            } if settling == token
        ) {
            *state = TransactionDropCleanupState::Poisoned { token };
        }
    }

    fn poisoned_token(&self) -> Option<TransactionToken> {
        match *lock_unpoisoned(&self.state) {
            TransactionDropCleanupState::Poisoned { token } => Some(token),
            TransactionDropCleanupState::Idle
            | TransactionDropCleanupState::Pending { .. }
            | TransactionDropCleanupState::Settling { .. } => None,
        }
    }
}

struct OwnedTransactionReceipt {
    token: TransactionToken,
    cleanup: Arc<TransactionDropCleanup>,
    terminal: Arc<TransactionTerminalReceipt>,
    armed: bool,
}

impl OwnedTransactionReceipt {
    fn new(
        token: TransactionToken,
        cleanup: Arc<TransactionDropCleanup>,
        terminal: Arc<TransactionTerminalReceipt>,
    ) -> Self {
        Self {
            token,
            cleanup,
            terminal,
            armed: true,
        }
    }

    fn into_parts(mut self) -> (TransactionToken, Arc<TransactionTerminalReceipt>) {
        self.armed = false;
        (self.token, Arc::clone(&self.terminal))
    }
}

impl Drop for OwnedTransactionReceipt {
    fn drop(&mut self) {
        if self.armed {
            self.cleanup.request(self.token);
        }
    }
}

struct TransactionTerminalResponse {
    result: Result<(), FrankenError>,
    ownership_ended: bool,
}

struct TransactionOperationResponse<T> {
    result: Result<T, FrankenError>,
    ownership_ended: bool,
}

/// One request's independently tracked admission, cancellation, publication,
/// and consumer state.
///
/// The worker owns the only response sender after mailbox admission. A caller
/// may abandon its receiver, but that requests cancellation rather than
/// revoking the admitted command or fabricating a second terminal result.
enum ResponseStatus<T> {
    Pending(Option<Waker>),
    Ready(T),
    Disconnected,
}

struct RequestControl<T> {
    status: Mutex<ResponseStatus<T>>,
    ready: Condvar,
    admission: AtomicU8,
    consumer: AtomicU8,
    cancellation: Option<CancellationRelay>,
}

struct ResponseSender<T> {
    control: Option<Arc<RequestControl<T>>>,
}

struct ResponseReceiver<T> {
    control: Arc<RequestControl<T>>,
    complete: bool,
}

#[derive(Debug, Clone, Copy)]
struct ResponseDisconnected;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponsePublication {
    Primary,
    Cancellation,
    Disconnected,
}

fn response_channel<T>(
    cancellation: Option<CancellationRelay>,
) -> (ResponseSender<T>, ResponseReceiver<T>) {
    let control = Arc::new(RequestControl {
        status: Mutex::new(ResponseStatus::Pending(None)),
        ready: Condvar::new(),
        admission: AtomicU8::new(ADMISSION_PENDING),
        consumer: AtomicU8::new(CONSUMER_ACTIVE),
        cancellation,
    });
    (
        ResponseSender {
            control: Some(Arc::clone(&control)),
        },
        ResponseReceiver {
            control,
            complete: false,
        },
    )
}

impl<T> ResponseSender<T> {
    fn send(self, value: T) {
        let _ = self.publish(value, None);
    }

    fn send_prefer_cancellation(self, value: T, cancelled_value: T) -> ResponsePublication {
        self.publish(value, Some(cancelled_value))
    }

    fn publish(mut self, value: T, cancelled_value: Option<T>) -> ResponsePublication {
        let Some(control) = self.control.take() else {
            return ResponsePublication::Disconnected;
        };
        let mut value = Some(value);
        let mut cancelled_value = cancelled_value;
        let mut discarded = None;
        let (waker, publication) = {
            let mut status = lock_unpoisoned(&control.status);
            match &mut *status {
                ResponseStatus::Pending(waker) => {
                    let waker = waker.take();
                    let cancellation_won = cancelled_value.is_some()
                        && control
                            .cancellation
                            .as_ref()
                            .is_some_and(CancellationRelay::is_requested);
                    let publication = if cancellation_won {
                        discarded = value.take();
                        ResponsePublication::Cancellation
                    } else {
                        discarded = cancelled_value.take();
                        ResponsePublication::Primary
                    };
                    let published_value = if cancellation_won {
                        cancelled_value
                            .take()
                            .expect("cancellation publication retains its value")
                    } else {
                        value.take().expect("primary publication retains its value")
                    };
                    *status = ResponseStatus::Ready(published_value);
                    (waker, publication)
                }
                ResponseStatus::Ready(_) | ResponseStatus::Disconnected => {
                    (None, ResponsePublication::Disconnected)
                }
            }
        };
        drop(discarded);
        drop(value);
        drop(cancelled_value);
        if publication == ResponsePublication::Disconnected {
            return publication;
        }
        control.ready.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
        publication
    }
}

impl<T> Drop for ResponseSender<T> {
    fn drop(&mut self) {
        let Some(control) = self.control.take() else {
            return;
        };
        let waker = {
            let mut status = lock_unpoisoned(&control.status);
            match &mut *status {
                ResponseStatus::Pending(waker) => {
                    let waker = waker.take();
                    *status = ResponseStatus::Disconnected;
                    waker
                }
                ResponseStatus::Ready(_) | ResponseStatus::Disconnected => None,
            }
        };
        control.ready.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> ResponseReceiver<T> {
    fn mark_admitted(&self) {
        self.control
            .admission
            .store(ADMISSION_ACCEPTED, Ordering::Release);
    }

    fn mark_admission_failed(&self) {
        self.control
            .admission
            .store(ADMISSION_FAILED, Ordering::Release);
    }

    fn recv_blocking(mut self) -> Result<T, ResponseDisconnected> {
        let control = Arc::clone(&self.control);
        let mut status = lock_unpoisoned(&control.status);
        loop {
            match std::mem::replace(&mut *status, ResponseStatus::Disconnected) {
                ResponseStatus::Ready(value) => {
                    self.complete = true;
                    control.consumer.store(CONSUMER_COMPLETE, Ordering::Release);
                    return Ok(value);
                }
                ResponseStatus::Disconnected => return Err(ResponseDisconnected),
                ResponseStatus::Pending(waker) => {
                    *status = ResponseStatus::Pending(waker);
                    status = control
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

    fn poll(mut self: Pin<&mut Self>, task_cx: &mut Context<'_>) -> Poll<Self::Output> {
        let replacement_waker = task_cx.waker().clone();
        let control = Arc::clone(&self.control);
        let (poll, completed, displaced_waker, unused_waker) = {
            let mut status = lock_unpoisoned(&control.status);
            match std::mem::replace(&mut *status, ResponseStatus::Disconnected) {
                ResponseStatus::Ready(value) => {
                    (Poll::Ready(Ok(value)), true, None, Some(replacement_waker))
                }
                ResponseStatus::Disconnected => (
                    Poll::Ready(Err(ResponseDisconnected)),
                    false,
                    None,
                    Some(replacement_waker),
                ),
                ResponseStatus::Pending(mut registered) => {
                    let (displaced, unused) = if registered
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
                    (Poll::Pending, false, displaced, unused)
                }
            }
        };
        if completed {
            self.complete = true;
            control.consumer.store(CONSUMER_COMPLETE, Ordering::Release);
        }
        // Waker clone/drop callbacks may be reentrant; never run them while
        // the response mutex is held.
        drop(displaced_waker);
        drop(unused_waker);
        poll
    }
}

impl<T> Drop for ResponseReceiver<T> {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let previous = self
            .control
            .consumer
            .swap(CONSUMER_ABANDONED, Ordering::AcqRel);
        if previous == CONSUMER_ACTIVE
            && self.control.admission.load(Ordering::Acquire) == ADMISSION_ACCEPTED
        {
            if let Some(cancellation) = &self.control.cancellation {
                cancellation.request();
            }
        }
        let previous = {
            let mut status = lock_unpoisoned(&self.control.status);
            std::mem::replace(&mut *status, ResponseStatus::Disconnected)
        };
        drop(previous);
    }
}

type Responder<T> = ResponseSender<Result<T, FrankenError>>;

struct CommandEnvelope {
    cancellation: Option<CancellationRelay>,
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
    ExecuteBatch {
        sql: String,
        tx: Responder<()>,
    },
    BeginOwnedTransaction {
        connection_id: u64,
        tx: Responder<OwnedTransactionReceipt>,
    },
    TransactionPrepare {
        token: TransactionToken,
        sql: String,
        tx: Responder<TransactionOperationResponse<()>>,
    },
    TransactionQuery {
        token: TransactionToken,
        sql: String,
        params: Vec<SqliteValue>,
        tx: Responder<TransactionOperationResponse<Vec<Row>>>,
    },
    TransactionQueryWithParamsStream {
        token: TransactionToken,
        sql: String,
        params: Vec<SqliteValue>,
        row_tx: sync_mpsc::SyncSender<Option<Row>>,
        terminal_tx: Responder<TransactionOperationResponse<()>>,
    },
    TransactionQueryRow {
        token: TransactionToken,
        sql: String,
        params: Vec<SqliteValue>,
        tx: Responder<TransactionOperationResponse<Row>>,
    },
    TransactionExecute {
        token: TransactionToken,
        sql: String,
        params: Vec<SqliteValue>,
        tx: Responder<TransactionOperationResponse<usize>>,
    },
    TransactionExecuteMany {
        token: TransactionToken,
        sql: String,
        parameter_sets: Vec<Vec<SqliteValue>>,
        tx: Responder<TransactionOperationResponse<usize>>,
    },
    TransactionExecuteBatch {
        token: TransactionToken,
        sql: String,
        tx: Responder<TransactionOperationResponse<()>>,
    },
    TransactionSavepoint {
        token: TransactionToken,
        action: TransactionSavepointAction,
        name: String,
        tx: Responder<TransactionOperationResponse<()>>,
    },
    TransactionCommit {
        token: TransactionToken,
        tx: Responder<TransactionTerminalResponse>,
    },
    TransactionRollback {
        token: TransactionToken,
        tx: Responder<TransactionTerminalResponse>,
    },
    TransactionLastInsertRowid {
        token: TransactionToken,
        tx: Responder<TransactionOperationResponse<i64>>,
    },
    LastInsertRowid {
        tx: Responder<i64>,
    },
    Close {
        tx: Responder<()>,
    },
    #[cfg(test)]
    TestBlockActor {
        entered: ResponseSender<()>,
        release: sync_mpsc::Receiver<()>,
        tx: Responder<()>,
    },
    #[cfg(test)]
    TestPanicActor {
        tx: Responder<()>,
    },
}

#[derive(Debug, Clone, Copy)]
enum TransactionSavepointAction {
    Create,
    Release,
    RollbackTo,
}

const WORKER_DEAD_DETAIL: &str = "async worker thread terminated unexpectedly";

fn worker_dead_err() -> FrankenError {
    FrankenError::Internal(WORKER_DEAD_DETAIL.to_owned())
}

fn is_worker_dead_error(error: &FrankenError) -> bool {
    matches!(error, FrankenError::Internal(detail) if detail == WORKER_DEAD_DETAIL)
}

fn stream_consumer_dead_err() -> FrankenError {
    FrankenError::Internal("synchronous query consumer stopped receiving rows".to_owned())
}

fn requires_runtime_err() -> FrankenError {
    FrankenError::Internal(
        "AsyncConnection async methods require the ambient asupersync task context".to_owned(),
    )
}

fn sync_on_runtime_err() -> FrankenError {
    FrankenError::Internal(
        "synchronous AsyncConnection methods cannot block an asupersync runtime task; use the async API"
            .to_owned(),
    )
}

fn worker_thread_spawn_err(error: std::io::Error) -> FrankenError {
    FrankenError::Internal(format!("failed to spawn async-api worker thread: {error}"))
}

fn native_cx_for_polling_task() -> Result<NativeCx, FrankenError> {
    NativeCx::current().ok_or_else(requires_runtime_err)
}

async fn recv_authoritative_worker_response<Caps, T>(
    cx: &Cx<Caps>,
    rx: ResponseReceiver<Result<T, FrankenError>>,
    cancellation: CancellationRelay,
    polling_native_cx: NativeCx,
    lifecycle: &WorkerLifecycle,
) -> Result<T, FrankenError>
where
    Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
{
    let native_sentinel = NativeOnceCell::<()>::new();
    let mut native_cancel = std::pin::pin!(native_sentinel.wait(&polling_native_cx));
    // The project Cx may deliberately carry a native context distinct from
    // the ambient task context. Keep it caller-side and register its own
    // cancellation wake; moving either context into the actor would
    // transplant task identity and capabilities.
    let attached_native_cx = cx.attached_native_cx();
    let attached_native_sentinel = NativeOnceCell::<()>::new();
    let mut attached_native_cancel = std::pin::pin!(
        attached_native_cx
            .as_ref()
            .map(|native_cx| attached_native_sentinel.wait(native_cx))
    );
    let cancellation_wait_owner = cancellation.clone();
    let mut project_cancel = std::pin::pin!(cancellation_wait_owner.cancelled());
    let mut response = std::pin::pin!(rx);
    let mut cancellation_forwarded = false;

    let response_result = std::future::poll_fn(|task_cx| {
        // Publication is the one terminal result. Check it before either
        // cancellation source so a publication already visible in this turn
        // cannot be overwritten with a synthetic interrupt.
        match response.as_mut().poll(task_cx) {
            Poll::Ready(Ok(result)) => return Poll::Ready(Ok(result)),
            Poll::Ready(Err(_)) => return Poll::Ready(Err(ResponseDisconnected)),
            Poll::Pending => {}
        }

        if !cancellation_forwarded {
            // This supplied Cx is the semantic cancellation authority for the
            // public operation. Its checkpoint also observes e-process state
            // on every natural poll. E-process currently exposes no
            // independent wake-registration API, so the attached native
            // sentinel below provides the prompt wake source available today.
            let supplied_cx_cancelled = checkpoint_or_interrupt(cx).is_err();
            let project_cancelled =
                matches!(project_cancel.as_mut().poll(task_cx), Poll::Ready(()));
            let native_cancelled = polling_native_cx.checkpoint().is_err()
                || matches!(native_cancel.as_mut().poll(task_cx), Poll::Ready(Err(_)));
            let attached_native_cancelled = attached_native_cx
                .as_ref()
                .is_some_and(|native_cx| native_cx.checkpoint().is_err())
                || attached_native_cancel
                    .as_mut()
                    .as_pin_mut()
                    .is_some_and(|wait| matches!(wait.poll(task_cx), Poll::Ready(Err(_))));
            if supplied_cx_cancelled
                || project_cancelled
                || native_cancelled
                || attached_native_cancelled
            {
                cancellation.request();
                cancellation_forwarded = true;
            }
        }

        // An admitted command remains owned by the worker. Cancellation asks
        // its root-derived operation context to stop; it does not create a
        // competing public result.
        Poll::Pending
    })
    .await;
    match response_result {
        Ok(result) => result,
        Err(ResponseDisconnected) => {
            WorkerExit::new(lifecycle).await;
            Err(lifecycle.terminal_error())
        }
    }
}

fn recv_worker_response<T>(
    rx: ResponseReceiver<Result<T, FrankenError>>,
    lifecycle: &WorkerLifecycle,
) -> Result<T, FrankenError> {
    match rx.recv_blocking() {
        Ok(result) => result,
        Err(ResponseDisconnected) => {
            lifecycle.wait_finished_sync();
            Err(lifecycle.terminal_error())
        }
    }
}

// ---------------------------------------------------------------------------
// Worker task
// ---------------------------------------------------------------------------

struct CommandCapacitySignal {
    epoch: Mutex<u64>,
    changed: Condvar,
    #[cfg(test)]
    wait_observer: Option<sync_mpsc::Sender<()>>,
    #[cfg(test)]
    post_reservation_cancellation: Option<CancellationRelay>,
}

impl CommandCapacitySignal {
    fn new() -> Self {
        Self {
            epoch: Mutex::new(0),
            changed: Condvar::new(),
            #[cfg(test)]
            wait_observer: None,
            #[cfg(test)]
            post_reservation_cancellation: None,
        }
    }

    #[cfg(test)]
    fn with_wait_observer(wait_observer: sync_mpsc::Sender<()>) -> Self {
        Self {
            epoch: Mutex::new(0),
            changed: Condvar::new(),
            wait_observer: Some(wait_observer),
            post_reservation_cancellation: None,
        }
    }

    #[cfg(test)]
    fn with_post_reservation_cancellation(
        wait_observer: sync_mpsc::Sender<()>,
        post_reservation_cancellation: CancellationRelay,
    ) -> Self {
        Self {
            epoch: Mutex::new(0),
            changed: Condvar::new(),
            wait_observer: Some(wait_observer),
            post_reservation_cancellation: Some(post_reservation_cancellation),
        }
    }

    fn observe_async_reservation(&self) {
        #[cfg(test)]
        if let Some(cancellation) = &self.post_reservation_cancellation {
            cancellation.request();
        }
    }

    fn notify(&self) {
        let mut epoch = lock_unpoisoned(&self.epoch);
        *epoch = epoch.wrapping_add(1);
        drop(epoch);
        self.changed.notify_all();
    }

    fn reserve_blocking<'a>(
        &'a self,
        tx: &'a async_mpsc::Sender<CommandEnvelope>,
    ) -> Result<CommandPermit<'a>, async_mpsc::SendError<()>> {
        loop {
            match tx.try_reserve() {
                Ok(permit) => return Ok(CommandPermit::new(permit, self)),
                Err(async_mpsc::SendError::Full(())) => {
                    let epoch = lock_unpoisoned(&self.epoch);
                    match tx.try_reserve() {
                        Ok(permit) => return Ok(CommandPermit::new(permit, self)),
                        Err(async_mpsc::SendError::Full(())) => {
                            let observed = *epoch;
                            #[cfg(test)]
                            if let Some(wait_observer) = &self.wait_observer {
                                let _ = wait_observer.send(());
                            }
                            let epoch = self
                                .changed
                                .wait_while(epoch, |current| *current == observed)
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

/// Reservation wrapper that also wakes synchronous capacity waiters when an
/// async admission attempt abandons its slot before enqueueing.
///
/// The underlying asupersync permit wakes only asupersync send waiters when it
/// releases capacity. Our synchronous bridge waits on a separate Condvar, so
/// every unsent release must advance that waiter's generation as one atomic
/// RAII obligation.
struct CommandPermit<'a> {
    permit: Option<async_mpsc::SendPermit<'a, CommandEnvelope>>,
    command_capacity: &'a CommandCapacitySignal,
}

impl<'a> CommandPermit<'a> {
    fn new(
        permit: async_mpsc::SendPermit<'a, CommandEnvelope>,
        command_capacity: &'a CommandCapacitySignal,
    ) -> Self {
        Self {
            permit: Some(permit),
            command_capacity,
        }
    }

    fn try_send(
        mut self,
        envelope: CommandEnvelope,
    ) -> Result<(), async_mpsc::SendError<CommandEnvelope>> {
        let permit = self
            .permit
            .take()
            .expect("command permit can be consumed only once");
        let result = permit.try_send(envelope);
        if result.is_err() {
            // A disconnected send consumed the reservation without a worker
            // receive, so it must wake the private synchronous wait lane.
            self.command_capacity.notify();
        }
        result
    }
}

impl Drop for CommandPermit<'_> {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            // Release the channel reservation before advancing the generation;
            // a woken waiter must observe the newly available slot.
            drop(permit);
            self.command_capacity.notify();
        }
    }
}

/// Pending asynchronous reservation whose cleanup also wakes the private
/// synchronous capacity lane.
///
/// Dropping asupersync's `Reserve` removes its FIFO position and can expose an
/// already-free slot, but it wakes only another asupersync waiter. This wrapper
/// owns that future so cleanup is ordered before the Condvar generation
/// advances. On success it transfers the notification obligation to
/// [`CommandPermit`].
struct CommandReserve<'a> {
    reserve: Option<async_mpsc::Reserve<'a, CommandEnvelope>>,
    command_capacity: &'a CommandCapacitySignal,
}

impl<'a> CommandReserve<'a> {
    fn new(
        reserve: async_mpsc::Reserve<'a, CommandEnvelope>,
        command_capacity: &'a CommandCapacitySignal,
    ) -> Self {
        Self {
            reserve: Some(reserve),
            command_capacity,
        }
    }
}

impl<'a> Future for CommandReserve<'a> {
    type Output = Result<CommandPermit<'a>, async_mpsc::SendError<()>>;

    fn poll(self: Pin<&mut Self>, task_cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let poll = Pin::new(
            this.reserve
                .as_mut()
                .expect("command reserve cannot be polled after completion"),
        )
        .poll(task_cx);
        match poll {
            Poll::Ready(Ok(permit)) => {
                drop(this.reserve.take());
                Poll::Ready(Ok(CommandPermit::new(permit, this.command_capacity)))
            }
            Poll::Ready(Err(error)) => {
                drop(this.reserve.take());
                this.command_capacity.notify();
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for CommandReserve<'_> {
    fn drop(&mut self) {
        if let Some(reserve) = self.reserve.take() {
            // Remove this waiter / expose any available slot before waking
            // the private synchronous wait lane.
            drop(reserve);
            self.command_capacity.notify();
        }
    }
}

struct CommandSender {
    tx: Option<async_mpsc::Sender<CommandEnvelope>>,
    command_capacity: Arc<CommandCapacitySignal>,
    lifecycle: Arc<WorkerLifecycle>,
}

impl CommandSender {
    fn tx(&self) -> Result<&async_mpsc::Sender<CommandEnvelope>, FrankenError> {
        self.tx.as_ref().ok_or_else(worker_dead_err)
    }

    async fn reconcile_send_error(&self, error: FrankenError) -> FrankenError {
        if self.tx.as_ref().is_some_and(|tx| tx.is_closed()) {
            WorkerExit::new(&self.lifecycle).await;
            self.lifecycle.terminal_error()
        } else {
            error
        }
    }

    fn reconcile_send_error_sync(&self, error: FrankenError) -> FrankenError {
        if self.tx.as_ref().is_some_and(|tx| tx.is_closed()) {
            self.lifecycle.wait_finished_sync();
            self.lifecycle.terminal_error()
        } else {
            error
        }
    }

    async fn reserve_async<'a, Caps>(
        &'a self,
        cx: &Cx<Caps>,
        cancellation: &CancellationRelay,
        polling_native_cx: &'a NativeCx,
    ) -> Result<CommandPermit<'a>, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let tx = self.tx()?;
        match tx.try_reserve() {
            Ok(permit) => Ok(CommandPermit::new(permit, &self.command_capacity)),
            Err(async_mpsc::SendError::Full(())) => {
                let mut cancellation_wait = std::pin::pin!(cancellation.cancelled());
                let mut reserve = std::pin::pin!(CommandReserve::new(
                    tx.reserve(polling_native_cx),
                    &self.command_capacity,
                ));
                let attached_native_cx = cx.attached_native_cx();
                let attached_native_sentinel = NativeOnceCell::<()>::new();
                let mut attached_native_cancel = std::pin::pin!(
                    attached_native_cx
                        .as_ref()
                        .map(|native_cx| attached_native_sentinel.wait(native_cx))
                );
                let result = std::future::poll_fn(|task_cx| {
                    let attached_native_cancelled = attached_native_cx
                        .as_ref()
                        .is_some_and(|native_cx| native_cx.checkpoint().is_err())
                        || attached_native_cancel
                            .as_mut()
                            .as_pin_mut()
                            .is_some_and(|wait| matches!(wait.poll(task_cx), Poll::Ready(Err(_))));
                    if checkpoint_or_interrupt(cx).is_err()
                        || matches!(cancellation_wait.as_mut().poll(task_cx), Poll::Ready(()))
                        || attached_native_cancelled
                    {
                        return Poll::Ready(Err(FrankenError::Interrupt));
                    }
                    match reserve.as_mut().poll(task_cx) {
                        Poll::Ready(Ok(permit)) => {
                            self.command_capacity.observe_async_reservation();
                            if checkpoint_or_interrupt(cx).is_err()
                                || polling_native_cx.checkpoint().is_err()
                                || cancellation.is_requested()
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
                .await;
                match result {
                    Ok(permit) => Ok(permit),
                    Err(error) => Err(self.reconcile_send_error(error).await),
                }
            }
            Err(error) => Err(self.reconcile_send_error(send_err(error)).await),
        }
    }

    async fn request_async<Caps, T, F>(
        &self,
        cx: &Cx<Caps>,
        build: F,
    ) -> Result<
        (
            ResponseReceiver<Result<T, FrankenError>>,
            CancellationRelay,
            NativeCx,
        ),
        FrankenError,
    >
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        T: Send + 'static,
        F: FnOnce(Responder<T>) -> Command,
    {
        checkpoint_or_interrupt(cx)?;
        let polling_native_cx = native_cx_for_polling_task()?;
        if polling_native_cx.checkpoint().is_err() {
            return Err(FrankenError::Interrupt);
        }
        let cancellation = cx.cancellation_relay();
        if cancellation.is_requested() {
            return Err(FrankenError::Interrupt);
        }
        let (response_tx, response_rx) = response_channel(Some(cancellation.clone()));
        let permit = self
            .reserve_async(cx, &cancellation, &polling_native_cx)
            .await?;
        checkpoint_or_interrupt(cx)?;
        if polling_native_cx.checkpoint().is_err() || cancellation.is_requested() {
            return Err(FrankenError::Interrupt);
        }

        response_rx.mark_admitted();
        if let Err(error) = permit.try_send(CommandEnvelope {
            cancellation: Some(cancellation.clone()),
            command: build(response_tx),
        }) {
            response_rx.mark_admission_failed();
            return Err(self.reconcile_send_error(send_err(error)).await);
        }
        Ok((response_rx, cancellation, polling_native_cx))
    }

    async fn request_close_async<Caps>(
        &self,
        cx: &Cx<Caps>,
    ) -> Result<ResponseReceiver<Result<(), FrankenError>>, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        checkpoint_or_interrupt(cx)?;
        let polling_native_cx = native_cx_for_polling_task()?;
        if polling_native_cx.checkpoint().is_err() {
            return Err(FrankenError::Interrupt);
        }
        let admission_cancellation = cx.cancellation_relay();
        let (response_tx, response_rx) = response_channel(None);
        let permit = self
            .reserve_async(cx, &admission_cancellation, &polling_native_cx)
            .await?;
        checkpoint_or_interrupt(cx)?;
        if polling_native_cx.checkpoint().is_err() || admission_cancellation.is_requested() {
            return Err(FrankenError::Interrupt);
        }

        response_rx.mark_admitted();
        if let Err(error) = permit.try_send(CommandEnvelope {
            cancellation: None,
            command: Command::Close { tx: response_tx },
        }) {
            response_rx.mark_admission_failed();
            return Err(self.reconcile_send_error(send_err(error)).await);
        }
        Ok(response_rx)
    }

    fn request_sync<T, F>(&self, build: F) -> Result<T, FrankenError>
    where
        T: Send + 'static,
        F: FnOnce(Responder<T>) -> Command,
    {
        if NativeCx::is_active() {
            return Err(sync_on_runtime_err());
        }
        let (response_tx, response_rx) = response_channel(None);
        let permit = self
            .command_capacity
            .reserve_blocking(self.tx()?)
            .map_err(send_err)
            .map_err(|error| self.reconcile_send_error_sync(error))?;
        response_rx.mark_admitted();
        if let Err(error) = permit.try_send(CommandEnvelope {
            cancellation: None,
            command: build(response_tx),
        }) {
            response_rx.mark_admission_failed();
            return Err(self.reconcile_send_error_sync(send_err(error)));
        }
        recv_worker_response(response_rx, &self.lifecycle)
    }

    fn send_stream_sync(&self, command: Command) -> Result<(), FrankenError> {
        if NativeCx::is_active() {
            return Err(sync_on_runtime_err());
        }
        let permit = self
            .command_capacity
            .reserve_blocking(self.tx()?)
            .map_err(send_err)
            .map_err(|error| self.reconcile_send_error_sync(error))?;
        permit
            .try_send(CommandEnvelope {
                cancellation: None,
                command,
            })
            .map_err(send_err)
            .map_err(|error| self.reconcile_send_error_sync(error))
    }
}

impl Drop for CommandSender {
    fn drop(&mut self) {
        // Disconnecting the last sender wakes the receiver. The worker drains
        // admitted commands and closes the connection itself.
        drop(self.tx.take());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerTransactionDisposition {
    NoActiveTransaction,
    Active,
    OutcomeUnknown,
}

impl WorkerTransactionDisposition {
    const fn label(self) -> &'static str {
        match self {
            Self::NoActiveTransaction => "no active transaction",
            Self::Active => "active transaction retained",
            Self::OutcomeUnknown => "transaction outcome unknown",
        }
    }

    const fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::NoActiveTransaction, Self::NoActiveTransaction) => Self::NoActiveTransaction,
            (Self::Active, Self::Active) => Self::Active,
            _ => Self::OutcomeUnknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerFailureStage {
    Open,
    ActorLoop,
    Close,
    Join,
    Lifecycle,
}

impl WorkerFailureStage {
    const fn label(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::ActorLoop => "actor loop",
            Self::Close => "connection close",
            Self::Join => "worker join",
            Self::Lifecycle => "lifecycle",
        }
    }
}

#[derive(Debug, Clone)]
enum WorkerTerminalState {
    Running,
    Closed,
    Broken {
        cause: Arc<str>,
        transaction_disposition: WorkerTransactionDisposition,
    },
}

#[cfg(test)]
struct TestActorOperationPause {
    entered: ResponseSender<()>,
    release: sync_mpsc::Receiver<()>,
}

#[cfg(test)]
struct TestClosePublicationPause {
    entered: ResponseSender<()>,
    release: sync_mpsc::Receiver<()>,
}

#[cfg(test)]
struct TestTransactionTerminalPublicationPause {
    entered: ResponseSender<()>,
    release: sync_mpsc::Receiver<()>,
}

struct WorkerLifecycle {
    finished: AtomicBool,
    terminal: Mutex<WorkerTerminalState>,
    next_waiter_id: AtomicU64,
    waiters: Mutex<Vec<(u64, Waker)>>,
    changed: Condvar,
    command_capacity: Arc<CommandCapacitySignal>,
    #[cfg(test)]
    close_connection_calls: AtomicUsize,
    #[cfg(test)]
    close_post_effect_failures: AtomicUsize,
    #[cfg(test)]
    drop_rollback_calls: AtomicUsize,
    #[cfg(test)]
    drop_rollback_retryable_failures: AtomicUsize,
    #[cfg(test)]
    drop_rollback_poison_failures: AtomicUsize,
    #[cfg(test)]
    terminal_post_effect_failures: AtomicUsize,
    #[cfg(test)]
    terminal_pre_effect_failures: AtomicUsize,
    #[cfg(test)]
    terminal_pre_publication_panics: AtomicUsize,
    #[cfg(test)]
    successful_nonterminal_ownership_endings: AtomicUsize,
    #[cfg(test)]
    terminal_delivered_response_failures: AtomicUsize,
    #[cfg(test)]
    ordinary_stream_panics: AtomicUsize,
    #[cfg(test)]
    transaction_stream_panics: AtomicUsize,
    #[cfg(test)]
    actor_operation_pause: Mutex<Option<TestActorOperationPause>>,
    #[cfg(test)]
    close_publication_pause: Mutex<Option<TestClosePublicationPause>>,
    #[cfg(test)]
    transaction_terminal_publication_pause: Mutex<Option<TestTransactionTerminalPublicationPause>>,
    #[cfg(test)]
    join_calls: AtomicUsize,
}

impl WorkerLifecycle {
    fn new(command_capacity: Arc<CommandCapacitySignal>) -> Self {
        Self {
            finished: AtomicBool::new(false),
            terminal: Mutex::new(WorkerTerminalState::Running),
            next_waiter_id: AtomicU64::new(1),
            waiters: Mutex::new(Vec::new()),
            changed: Condvar::new(),
            command_capacity,
            #[cfg(test)]
            close_connection_calls: AtomicUsize::new(0),
            #[cfg(test)]
            close_post_effect_failures: AtomicUsize::new(0),
            #[cfg(test)]
            drop_rollback_calls: AtomicUsize::new(0),
            #[cfg(test)]
            drop_rollback_retryable_failures: AtomicUsize::new(0),
            #[cfg(test)]
            drop_rollback_poison_failures: AtomicUsize::new(0),
            #[cfg(test)]
            terminal_post_effect_failures: AtomicUsize::new(0),
            #[cfg(test)]
            terminal_pre_effect_failures: AtomicUsize::new(0),
            #[cfg(test)]
            terminal_pre_publication_panics: AtomicUsize::new(0),
            #[cfg(test)]
            successful_nonterminal_ownership_endings: AtomicUsize::new(0),
            #[cfg(test)]
            terminal_delivered_response_failures: AtomicUsize::new(0),
            #[cfg(test)]
            ordinary_stream_panics: AtomicUsize::new(0),
            #[cfg(test)]
            transaction_stream_panics: AtomicUsize::new(0),
            #[cfg(test)]
            actor_operation_pause: Mutex::new(None),
            #[cfg(test)]
            close_publication_pause: Mutex::new(None),
            #[cfg(test)]
            transaction_terminal_publication_pause: Mutex::new(None),
            #[cfg(test)]
            join_calls: AtomicUsize::new(0),
        }
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    fn terminal_state(&self) -> WorkerTerminalState {
        lock_unpoisoned(&self.terminal).clone()
    }

    fn terminal_error(&self) -> FrankenError {
        match self.terminal_state() {
            WorkerTerminalState::Running => FrankenError::Internal(
                "async worker response disconnected before lifecycle terminalization".to_owned(),
            ),
            WorkerTerminalState::Closed => {
                FrankenError::Internal("async worker closed before request completion".to_owned())
            }
            WorkerTerminalState::Broken {
                cause,
                transaction_disposition,
            } => FrankenError::Internal(format!(
                "async worker failed: {cause}; {}",
                transaction_disposition.label()
            )),
        }
    }

    fn publish_closed(&self) {
        let mut terminal = lock_unpoisoned(&self.terminal);
        if matches!(*terminal, WorkerTerminalState::Running) {
            *terminal = WorkerTerminalState::Closed;
        }
    }

    fn publish_broken(
        &self,
        stage: WorkerFailureStage,
        detail: impl Into<String>,
        transaction_disposition: WorkerTransactionDisposition,
    ) {
        let new_cause = format!("{}: {}", stage.label(), detail.into());
        let mut terminal = lock_unpoisoned(&self.terminal);
        match &*terminal {
            WorkerTerminalState::Running | WorkerTerminalState::Closed => {
                *terminal = WorkerTerminalState::Broken {
                    cause: Arc::from(new_cause),
                    transaction_disposition,
                };
            }
            WorkerTerminalState::Broken {
                cause,
                transaction_disposition: existing_disposition,
            } => {
                let combined_cause = format!("{cause}; {new_cause}");
                let combined_disposition = existing_disposition.combine(transaction_disposition);
                *terminal = WorkerTerminalState::Broken {
                    cause: Arc::from(combined_cause),
                    transaction_disposition: combined_disposition,
                };
            }
        }
    }

    fn wait_finished_sync(&self) {
        let mut waiters = lock_unpoisoned(&self.waiters);
        while !self.finished.load(Ordering::Acquire) {
            waiters = self
                .changed
                .wait(waiters)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    #[cfg(test)]
    fn note_close_connection_call(&self) {
        self.close_connection_calls.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    fn take_close_post_effect_failure(&self) -> bool {
        Self::take_test_failure(&self.close_post_effect_failures)
    }

    #[cfg(test)]
    fn note_drop_rollback_call(&self) {
        self.drop_rollback_calls.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    fn take_test_failure(counter: &AtomicUsize) -> bool {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }

    #[cfg(test)]
    fn take_drop_rollback_retryable_failure(&self) -> bool {
        Self::take_test_failure(&self.drop_rollback_retryable_failures)
    }

    #[cfg(test)]
    fn take_drop_rollback_poison_failure(&self) -> bool {
        Self::take_test_failure(&self.drop_rollback_poison_failures)
    }

    #[cfg(test)]
    fn take_terminal_post_effect_failure(&self) -> bool {
        Self::take_test_failure(&self.terminal_post_effect_failures)
    }

    #[cfg(test)]
    fn take_terminal_pre_effect_failure(&self) -> bool {
        Self::take_test_failure(&self.terminal_pre_effect_failures)
    }

    #[cfg(test)]
    fn take_terminal_pre_publication_panic(&self) -> bool {
        Self::take_test_failure(&self.terminal_pre_publication_panics)
    }

    #[cfg(test)]
    fn take_successful_nonterminal_ownership_ending(&self) -> bool {
        Self::take_test_failure(&self.successful_nonterminal_ownership_endings)
    }

    #[cfg(test)]
    fn take_terminal_delivered_response_failure(&self) -> bool {
        Self::take_test_failure(&self.terminal_delivered_response_failures)
    }

    #[cfg(test)]
    fn take_ordinary_stream_panic(&self) -> bool {
        Self::take_test_failure(&self.ordinary_stream_panics)
    }

    #[cfg(test)]
    fn take_transaction_stream_panic(&self) -> bool {
        Self::take_test_failure(&self.transaction_stream_panics)
    }

    #[cfg(test)]
    fn install_actor_operation_pause(
        &self,
        entered: ResponseSender<()>,
        release: sync_mpsc::Receiver<()>,
    ) {
        let replaced = lock_unpoisoned(&self.actor_operation_pause)
            .replace(TestActorOperationPause { entered, release });
        assert!(
            replaced.is_none(),
            "only one actor-operation pause may be installed"
        );
    }

    #[cfg(test)]
    fn pause_actor_operation_once(&self, cancellation: &CancellationRelay) {
        let pause = lock_unpoisoned(&self.actor_operation_pause).take();
        if let Some(TestActorOperationPause { entered, release }) = pause {
            entered.send(());
            release
                .recv()
                .expect("test must release the paused actor operation");
            assert!(
                cancellation.is_requested(),
                "test must relay cancellation after actor-operation admission"
            );
        }
    }

    #[cfg(test)]
    fn install_close_publication_pause(
        &self,
        entered: ResponseSender<()>,
        release: sync_mpsc::Receiver<()>,
    ) {
        let replaced = lock_unpoisoned(&self.close_publication_pause)
            .replace(TestClosePublicationPause { entered, release });
        assert!(
            replaced.is_none(),
            "only one close-publication pause may be installed"
        );
    }

    #[cfg(test)]
    fn pause_close_after_publication_once(&self) {
        let pause = lock_unpoisoned(&self.close_publication_pause).take();
        if let Some(TestClosePublicationPause { entered, release }) = pause {
            entered.send(());
            release
                .recv()
                .expect("test must release the worker after Close publication");
        }
    }

    #[cfg(test)]
    fn install_transaction_terminal_publication_pause(
        &self,
        entered: ResponseSender<()>,
        release: sync_mpsc::Receiver<()>,
    ) {
        let replaced = lock_unpoisoned(&self.transaction_terminal_publication_pause)
            .replace(TestTransactionTerminalPublicationPause { entered, release });
        assert!(
            replaced.is_none(),
            "only one transaction terminal-publication pause may be installed"
        );
    }

    #[cfg(test)]
    fn pause_transaction_terminal_before_response_once(&self) {
        let pause = lock_unpoisoned(&self.transaction_terminal_publication_pause).take();
        if let Some(TestTransactionTerminalPublicationPause { entered, release }) = pause {
            entered.send(());
            release
                .recv()
                .expect("test must release the actor after terminal publication");
        }
    }

    fn remove_waiter(&self, waiter_id: u64) {
        let removed = {
            let mut waiters = lock_unpoisoned(&self.waiters);
            waiters
                .iter()
                .position(|(id, _)| *id == waiter_id)
                .map(|index| waiters.swap_remove(index))
        };
        // A RawWaker drop callback may reenter lifecycle registration.
        drop(removed);
    }

    fn allocate_waiter_id(&self, waiters: &[(u64, Waker)]) -> u64 {
        let mut candidate = self.next_waiter_id.load(Ordering::Relaxed);
        while waiters.iter().any(|(waiter_id, _)| *waiter_id == candidate) {
            candidate = candidate.wrapping_add(1);
        }
        self.next_waiter_id
            .store(candidate.wrapping_add(1), Ordering::Relaxed);
        candidate
    }

    fn finish(&self) {
        if matches!(self.terminal_state(), WorkerTerminalState::Running) {
            self.publish_broken(
                WorkerFailureStage::Lifecycle,
                "worker exited without publishing a terminal state",
                WorkerTransactionDisposition::OutcomeUnknown,
            );
        }
        self.finished.store(true, Ordering::Release);
        self.command_capacity.notify();
        let waiters = {
            let mut waiters = lock_unpoisoned(&self.waiters);
            std::mem::take(&mut *waiters)
        };
        self.changed.notify_all();
        for (_, waiter) in waiters {
            waiter.wake();
        }
    }
}

struct WorkerExit<'a> {
    lifecycle: &'a WorkerLifecycle,
    waiter_id: Option<u64>,
}

impl<'a> WorkerExit<'a> {
    const fn new(lifecycle: &'a WorkerLifecycle) -> Self {
        Self {
            lifecycle,
            waiter_id: None,
        }
    }
}

impl Future for WorkerExit<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, task_cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.lifecycle.finished.load(Ordering::Acquire) {
            if let Some(waiter_id) = self.waiter_id.take() {
                self.lifecycle.remove_waiter(waiter_id);
            }
            return Poll::Ready(());
        }

        // A RawWaker clone callback may be reentrant. Clone before taking the
        // lifecycle lock, and destroy displaced/unused Wakers after unlocking.
        let replacement_waker = task_cx.waker().clone();
        let current_waiter_id = self.waiter_id;
        let (poll, waiter_id, displaced_waker, unused_waker) = {
            let mut waiters = lock_unpoisoned(&self.lifecycle.waiters);
            if self.lifecycle.finished.load(Ordering::Acquire) {
                let displaced = current_waiter_id
                    .and_then(|waiter_id| waiters.iter().position(|(id, _)| *id == waiter_id))
                    .map(|index| waiters.swap_remove(index).1);
                (Poll::Ready(()), None, displaced, Some(replacement_waker))
            } else if let Some(waiter_id) = current_waiter_id {
                if let Some((_, registered)) = waiters.iter_mut().find(|(id, _)| *id == waiter_id) {
                    if registered.will_wake(&replacement_waker) {
                        (
                            Poll::Pending,
                            Some(waiter_id),
                            None,
                            Some(replacement_waker),
                        )
                    } else {
                        (
                            Poll::Pending,
                            Some(waiter_id),
                            Some(std::mem::replace(registered, replacement_waker)),
                            None,
                        )
                    }
                } else {
                    waiters.push((waiter_id, replacement_waker));
                    (Poll::Pending, Some(waiter_id), None, None)
                }
            } else {
                // Registration and collision checking share the waiter lock,
                // so wrapping the sequence can never reuse a live identity.
                let waiter_id = self.lifecycle.allocate_waiter_id(&waiters);
                waiters.push((waiter_id, replacement_waker));
                (Poll::Pending, Some(waiter_id), None, None)
            }
        };
        self.waiter_id = waiter_id;
        drop(displaced_waker);
        drop(unused_waker);
        poll
    }
}

impl Drop for WorkerExit<'_> {
    fn drop(&mut self) {
        if let Some(waiter_id) = self.waiter_id.take() {
            self.lifecycle.remove_waiter(waiter_id);
        }
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

struct ActorTransactionOwner {
    connection_id: u64,
    next_generation: u64,
    owner: Option<ActorOwnedTransaction>,
    last_terminal: Option<(TransactionToken, TransactionTerminalOutcome)>,
}

struct ActorOwnedTransaction {
    token: TransactionToken,
    terminal: Arc<TransactionTerminalReceipt>,
    in_flight: Option<&'static str>,
}

impl ActorTransactionOwner {
    fn new(connection_id: u64) -> Self {
        Self {
            connection_id,
            next_generation: 1,
            owner: None,
            last_terminal: None,
        }
    }

    fn allocate(&mut self) -> Result<TransactionToken, FrankenError> {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.checked_add(1).ok_or_else(|| {
            FrankenError::Internal("async transaction generation exhausted".to_owned())
        })?;
        Ok(TransactionToken {
            connection_id: self.connection_id,
            generation,
        })
    }

    fn owns(&self, token: TransactionToken) -> bool {
        self.owner
            .as_ref()
            .is_some_and(|owner| owner.token == token)
    }

    fn begin(&mut self, token: TransactionToken, terminal: Arc<TransactionTerminalReceipt>) {
        assert!(
            self.owner.is_none(),
            "a new actor transaction cannot replace a live owner"
        );
        self.owner = Some(ActorOwnedTransaction {
            token,
            terminal,
            in_flight: None,
        });
    }

    fn current_token(&self) -> Option<TransactionToken> {
        self.owner.as_ref().map(|owner| owner.token)
    }

    fn unpublished_in_flight_commit(&self) -> Option<TransactionToken> {
        self.owner
            .as_ref()
            .filter(|owner| owner.in_flight == Some("commit"))
            .map(|owner| owner.token)
    }

    fn begin_operation(&mut self, token: TransactionToken, operation: &'static str) {
        let owner = self
            .owner
            .as_mut()
            .expect("only the live transaction owner can begin an operation");
        assert_eq!(
            owner.token, token,
            "an actor operation must match the live transaction generation"
        );
        assert!(
            owner.in_flight.replace(operation).is_none(),
            "the single-owner actor cannot overlap transaction operations"
        );
    }

    fn terminal_outcome(&self, token: TransactionToken) -> Option<TransactionTerminalOutcome> {
        self.last_terminal
            .as_ref()
            .filter(|(terminal_token, _)| *terminal_token == token)
            .map(|(_, outcome)| outcome.clone())
    }

    fn publish_terminal(&mut self, token: TransactionToken, outcome: TransactionTerminalOutcome) {
        let owner = self
            .owner
            .take()
            .expect("only a live actor transaction can become terminal");
        assert_eq!(
            owner.token, token,
            "terminal publication must match the live transaction generation"
        );
        owner.terminal.publish(token, outcome.clone());
        self.last_terminal = Some((token, outcome));
    }

    fn publish_unknown_if_owned(&mut self, detail: impl Into<String>) {
        let Some((token, operation)) = self
            .owner
            .as_ref()
            .map(|owner| (owner.token, owner.in_flight.unwrap_or("actor transaction")))
        else {
            return;
        };
        self.publish_unknown(token, operation, detail);
    }

    fn publish_unknown(
        &mut self,
        token: TransactionToken,
        operation: &'static str,
        detail: impl Into<String>,
    ) {
        self.publish_terminal(
            token,
            TransactionTerminalOutcome::Unknown {
                operation,
                detail: Arc::from(detail.into()),
            },
        );
    }

    fn publish_if_ended(
        &mut self,
        conn: &Connection,
        token: TransactionToken,
        outcome: TransactionTerminalOutcome,
    ) -> bool {
        if self.owns(token) && !conn.in_transaction() {
            self.publish_terminal(token, outcome);
        } else if let Some(owner) = self.owner.as_mut() {
            assert_eq!(
                owner.token, token,
                "terminal reconciliation must match the live generation"
            );
            owner.in_flight = None;
        }
        !self.owns(token)
    }
}

impl Drop for ActorTransactionOwner {
    fn drop(&mut self) {
        if self.owner.is_some() {
            self.publish_unknown_if_owned(
                "worker ownership ended without a generation-specific terminal publication",
            );
        }
    }
}

fn quote_savepoint_name(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn sql_might_contain_transaction_control(sql: &str) -> bool {
    fn is_identifier_start(byte: u8) -> bool {
        byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
    }

    fn is_identifier_continue(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80
    }

    fn is_transaction_keyword(word: &[u8]) -> bool {
        [
            b"BEGIN".as_slice(),
            b"COMMIT".as_slice(),
            b"END".as_slice(),
            b"ROLLBACK".as_slice(),
            b"SAVEPOINT".as_slice(),
            b"RELEASE".as_slice(),
        ]
        .iter()
        .any(|keyword| word.eq_ignore_ascii_case(keyword))
    }

    let bytes = sql.as_bytes();
    let mut offset = 0;
    while offset < bytes.len() {
        match bytes[offset] {
            b'-' if bytes.get(offset + 1) == Some(&b'-') => {
                offset += 2;
                while offset < bytes.len() && bytes[offset] != b'\n' {
                    offset += 1;
                }
            }
            b'/' if bytes.get(offset + 1) == Some(&b'*') => {
                offset += 2;
                while offset + 1 < bytes.len()
                    && !(bytes[offset] == b'*' && bytes[offset + 1] == b'/')
                {
                    offset += 1;
                }
                offset = bytes.len().min(offset.saturating_add(2));
            }
            quote @ (b'\'' | b'"' | b'`') => {
                offset += 1;
                while offset < bytes.len() {
                    if bytes[offset] != quote {
                        offset += 1;
                    } else if bytes.get(offset + 1) == Some(&quote) {
                        offset += 2;
                    } else {
                        offset += 1;
                        break;
                    }
                }
            }
            b'[' => {
                offset += 1;
                while offset < bytes.len() {
                    if bytes[offset] != b']' {
                        offset += 1;
                    } else {
                        offset += 1;
                        break;
                    }
                }
            }
            byte if is_identifier_start(byte) => {
                let start = offset;
                offset += 1;
                while offset < bytes.len() && is_identifier_continue(bytes[offset]) {
                    offset += 1;
                }
                if is_transaction_keyword(&bytes[start..offset]) {
                    return true;
                }
            }
            _ => offset += 1,
        }
    }
    false
}

fn statement_contains_transaction_control(statement: &Statement) -> bool {
    match statement {
        Statement::Begin(_)
        | Statement::Commit
        | Statement::Rollback(_)
        | Statement::Savepoint(_)
        | Statement::Release(_) => true,
        Statement::Explain { stmt, .. } => statement_contains_transaction_control(stmt),
        Statement::Select(_)
        | Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::CreateTable(_)
        | Statement::CreateIndex(_)
        | Statement::CreateView(_)
        | Statement::CreateVirtualTable(_)
        | Statement::Drop(_)
        | Statement::AlterTable(_)
        | Statement::Attach(_)
        | Statement::Detach(_)
        | Statement::Pragma(_)
        | Statement::Vacuum(_)
        | Statement::Reindex(_)
        | Statement::Analyze(_) => false,
        Statement::CreateTrigger(trigger) => trigger
            .body
            .iter()
            .any(statement_contains_transaction_control),
    }
}

fn validate_no_raw_transaction_control(sql: &str) -> Result<(), FrankenError> {
    if !sql_might_contain_transaction_control(sql) {
        return Ok(());
    }

    let (statements, errors) = Parser::from_sql(sql).parse_all();
    if let Some(parse_error) = errors.first() {
        return Err(FrankenError::ParseError {
            offset: usize::try_from(parse_error.span.start).unwrap_or(usize::MAX),
            detail: parse_error.message.clone(),
        });
    }
    if statements
        .iter()
        .any(statement_contains_transaction_control)
    {
        return Err(FrankenError::Busy);
    }
    Ok(())
}

fn assert_actor_transaction_owner(conn: &Connection, owner: &ActorTransactionOwner) {
    assert_eq!(
        owner.owner.is_some(),
        conn.in_transaction(),
        "the async actor must own exactly every live explicit transaction"
    );
}

fn drop_cleanup_failure_is_retryable(error: &FrankenError) -> bool {
    matches!(
        error,
        FrankenError::Busy | FrankenError::BusyRecovery | FrankenError::Interrupt
    )
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
    // The actor publishes transaction state before the response. Scoped
    // transaction commands are therefore visible before their token/result,
    // while ordinary transaction-control SQL is rejected before execution.
    publish_transaction_state(conn, in_txn);
    tx.send(result);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionTerminalOperation {
    Commit,
    Rollback,
}

impl TransactionTerminalOperation {
    const fn label(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Rollback => "rollback",
        }
    }

    fn outcome(self, result: &Result<(), FrankenError>) -> TransactionTerminalOutcome {
        match self {
            Self::Commit if result.is_ok() => TransactionTerminalOutcome::Committed,
            Self::Commit => TransactionTerminalOutcome::Unknown {
                operation: self.label(),
                detail: Arc::from(
                    result
                        .as_ref()
                        .expect_err("an unsuccessful commit must retain its error")
                        .to_string(),
                ),
            },
            Self::Rollback => TransactionTerminalOutcome::RolledBack {
                reason: Arc::from(match result {
                    Ok(()) => "explicit rollback completed".to_owned(),
                    Err(error) => format!("explicit rollback ended the transaction: {error}"),
                }),
            },
        }
    }

    fn result_for(self, outcome: &TransactionTerminalOutcome) -> Result<(), FrankenError> {
        match self {
            Self::Commit => outcome.commit_result(),
            Self::Rollback => outcome.rollback_result(),
        }
    }
}

fn respond_transaction_terminal(
    conn: &Connection,
    in_txn: &AtomicBool,
    owner: &mut ActorTransactionOwner,
    token: TransactionToken,
    tx: Responder<TransactionTerminalResponse>,
    mut result: Result<(), FrankenError>,
    operation: TransactionTerminalOperation,
    _lifecycle: &WorkerLifecycle,
) {
    let terminal_outcome = operation.outcome(&result);
    let ownership_ended = owner.publish_if_ended(conn, token, terminal_outcome.clone());
    if ownership_ended {
        // The generation receipt is the authoritative terminal fact. Once
        // ownership ended, an ancillary post-effect failure must not replace a
        // proven commit/rollback result in this first response.
        result = operation.result_for(&terminal_outcome);
    } else if result.is_ok() {
        result = Err(FrankenError::Internal(
            "transaction terminal operation returned success without ending token ownership"
                .to_owned(),
        ));
    }
    #[cfg(test)]
    if ownership_ended && _lifecycle.take_terminal_delivered_response_failure() {
        result = Err(FrankenError::Internal(
            "injected delivered terminal response error".to_owned(),
        ));
    }
    publish_transaction_state(conn, in_txn);
    #[cfg(test)]
    if ownership_ended {
        _lifecycle.pause_transaction_terminal_before_response_once();
    }
    tx.send(Ok(TransactionTerminalResponse {
        result,
        ownership_ended,
    }));
}

fn respond_transaction_operation<T>(
    conn: &Connection,
    in_txn: &AtomicBool,
    owner: &mut ActorTransactionOwner,
    token: TransactionToken,
    tx: Responder<TransactionOperationResponse<T>>,
    mut result: Result<T, FrankenError>,
    _lifecycle: &WorkerLifecycle,
) {
    let terminal_outcome = match &result {
        Ok(_) => TransactionTerminalOutcome::Unknown {
            operation: "statement",
            detail: Arc::from(
                "a successful nonterminal statement unexpectedly ended transaction ownership",
            ),
        },
        Err(error) => TransactionTerminalOutcome::RolledBack {
            reason: Arc::from(error.to_string()),
        },
    };
    let ownership_ended = owner.publish_if_ended(conn, token, terminal_outcome.clone());
    if ownership_ended
        && matches!(
            &terminal_outcome,
            TransactionTerminalOutcome::Unknown { .. }
        )
    {
        // A nominally successful nonterminal operation cannot report success
        // after it unexpectedly ended the transaction generation.
        result = Err(terminal_outcome.nonterminal_error());
    }
    publish_transaction_state(conn, in_txn);
    #[cfg(test)]
    if ownership_ended {
        _lifecycle.pause_transaction_terminal_before_response_once();
    }
    tx.send(Ok(TransactionOperationResponse {
        result,
        ownership_ended,
    }));
}

fn process_transaction_drop_cleanup(
    conn: &mut Connection,
    in_txn: &AtomicBool,
    owner: &mut ActorTransactionOwner,
    cleanup: &TransactionDropCleanup,
    _lifecycle: &WorkerLifecycle,
) {
    let Some((token, attempt)) = cleanup.begin_attempt() else {
        return;
    };
    if !owner.owns(token) {
        cleanup.resolve(token);
        assert_actor_transaction_owner(conn, owner);
        return;
    }
    owner.begin_operation(token, "drop rollback");

    #[cfg(test)]
    _lifecycle.note_drop_rollback_call();
    conn.mark_transaction_cleanup_required();
    #[cfg(test)]
    let rollback_result = if _lifecycle.take_drop_rollback_retryable_failure() {
        Err(FrankenError::Busy)
    } else if _lifecycle.take_drop_rollback_poison_failure() {
        Err(FrankenError::Internal(
            "injected non-retryable transaction cleanup failure".to_owned(),
        ))
    } else {
        future::block_on(conn.rollback_transaction())
    };
    #[cfg(not(test))]
    let rollback_result = future::block_on(conn.rollback_transaction());
    let terminal_outcome = TransactionTerminalOutcome::RolledBack {
        reason: Arc::from(match &rollback_result {
            Ok(()) => "transaction handle dropped before explicit completion".to_owned(),
            Err(error) => format!("drop cleanup ended the transaction: {error}"),
        }),
    };
    owner.publish_if_ended(conn, token, terminal_outcome);
    publish_transaction_state(conn, in_txn);
    if !owner.owns(token) {
        cleanup.resolve(token);
    } else {
        match rollback_result {
            Err(ref error)
                if attempt == TransactionDropCleanupAttempt::Fresh
                    && drop_cleanup_failure_is_retryable(error) =>
            {
                cleanup.schedule_one_retry(token, attempt);
            }
            Ok(()) | Err(_) => cleanup.poison(token),
        }
    }
    assert_actor_transaction_owner(conn, owner);
}

impl Command {
    fn transaction_token(&self) -> Option<TransactionToken> {
        match self {
            Self::TransactionPrepare { token, .. }
            | Self::TransactionQuery { token, .. }
            | Self::TransactionQueryWithParamsStream { token, .. }
            | Self::TransactionQueryRow { token, .. }
            | Self::TransactionExecute { token, .. }
            | Self::TransactionExecuteMany { token, .. }
            | Self::TransactionExecuteBatch { token, .. }
            | Self::TransactionSavepoint { token, .. }
            | Self::TransactionCommit { token, .. }
            | Self::TransactionRollback { token, .. }
            | Self::TransactionLastInsertRowid { token, .. } => Some(*token),
            _ => None,
        }
    }

    fn transaction_operation_label(&self) -> Option<&'static str> {
        match self {
            Self::TransactionPrepare { .. } => Some("prepare"),
            Self::TransactionQuery { .. } => Some("query"),
            Self::TransactionQueryWithParamsStream { .. } => Some("stream query"),
            Self::TransactionQueryRow { .. } => Some("query row"),
            Self::TransactionExecute { .. } => Some("execute"),
            Self::TransactionExecuteMany { .. } => Some("execute many"),
            Self::TransactionExecuteBatch { .. } => Some("execute batch"),
            Self::TransactionSavepoint { .. } => Some("savepoint"),
            Self::TransactionCommit { .. } => Some("commit"),
            Self::TransactionRollback { .. } => Some("rollback"),
            Self::TransactionLastInsertRowid { .. } => Some("last insert rowid"),
            Self::Prepare { .. }
            | Self::Query { .. }
            | Self::QueryWithParams { .. }
            | Self::QueryWithParamsStream { .. }
            | Self::QueryRow { .. }
            | Self::QueryRowWithParams { .. }
            | Self::Execute { .. }
            | Self::ExecuteWithParams { .. }
            | Self::ExecuteBatch { .. }
            | Self::BeginOwnedTransaction { .. }
            | Self::LastInsertRowid { .. }
            | Self::Close { .. } => None,
            #[cfg(test)]
            Self::TestBlockActor { .. } | Self::TestPanicActor { .. } => None,
        }
    }

    fn validate_sql_transaction_control(&self) -> Result<(), FrankenError> {
        let sql = match self {
            Self::Prepare { sql, .. }
            | Self::Query { sql, .. }
            | Self::QueryWithParams { sql, .. }
            | Self::QueryWithParamsStream { sql, .. }
            | Self::QueryRow { sql, .. }
            | Self::QueryRowWithParams { sql, .. }
            | Self::Execute { sql, .. }
            | Self::ExecuteWithParams { sql, .. }
            | Self::ExecuteBatch { sql, .. }
            | Self::TransactionPrepare { sql, .. }
            | Self::TransactionQuery { sql, .. }
            | Self::TransactionQueryWithParamsStream { sql, .. }
            | Self::TransactionQueryRow { sql, .. }
            | Self::TransactionExecute { sql, .. }
            | Self::TransactionExecuteMany { sql, .. }
            | Self::TransactionExecuteBatch { sql, .. } => sql,
            Self::BeginOwnedTransaction { .. }
            | Self::TransactionSavepoint { .. }
            | Self::TransactionCommit { .. }
            | Self::TransactionRollback { .. }
            | Self::TransactionLastInsertRowid { .. }
            | Self::LastInsertRowid { .. }
            | Self::Close { .. } => return Ok(()),
            #[cfg(test)]
            Self::TestBlockActor { .. } | Self::TestPanicActor { .. } => return Ok(()),
        };
        validate_no_raw_transaction_control(sql)
    }

    fn bypasses_cleanup_poison(&self) -> bool {
        match self {
            Self::Close { .. } => true,
            #[cfg(test)]
            Self::TestBlockActor { .. } | Self::TestPanicActor { .. } => true,
            _ => false,
        }
    }

    fn bypasses_transaction_owner(&self) -> bool {
        match self {
            Self::Close { .. } => true,
            #[cfg(test)]
            Self::TestBlockActor { .. } | Self::TestPanicActor { .. } => true,
            _ => false,
        }
    }

    fn respond_error(self, conn: &Connection, in_txn: &AtomicBool, error: FrankenError) -> bool {
        publish_transaction_state(conn, in_txn);
        match self {
            Self::Prepare { tx, .. } | Self::ExecuteBatch { tx, .. } => tx.send(Err(error)),
            Self::TransactionPrepare { tx, .. }
            | Self::TransactionExecuteBatch { tx, .. }
            | Self::TransactionSavepoint { tx, .. } => tx.send(Err(error)),
            Self::TransactionCommit { tx, .. } | Self::TransactionRollback { tx, .. } => {
                tx.send(Err(error))
            }
            Self::BeginOwnedTransaction { tx, .. } => tx.send(Err(error)),
            Self::Query { tx, .. } | Self::QueryWithParams { tx, .. } => tx.send(Err(error)),
            Self::TransactionQuery { tx, .. } => tx.send(Err(error)),
            Self::QueryWithParamsStream { tx, .. } => {
                let _ = tx.send(Err(error));
            }
            Self::TransactionQueryWithParamsStream {
                row_tx,
                terminal_tx,
                ..
            } => {
                drop(row_tx);
                terminal_tx.send(Err(error));
            }
            Self::QueryRow { tx, .. } | Self::QueryRowWithParams { tx, .. } => tx.send(Err(error)),
            Self::TransactionQueryRow { tx, .. } => tx.send(Err(error)),
            Self::Execute { tx, .. } | Self::ExecuteWithParams { tx, .. } => tx.send(Err(error)),
            Self::TransactionExecute { tx, .. } | Self::TransactionExecuteMany { tx, .. } => {
                tx.send(Err(error))
            }
            Self::TransactionLastInsertRowid { tx, .. } => tx.send(Err(error)),
            Self::LastInsertRowid { tx } => tx.send(Err(error)),
            Self::Close { tx } => {
                tx.send(Err(error));
                return false;
            }
            #[cfg(test)]
            Self::TestBlockActor { tx, .. } => tx.send(Err(error)),
            #[cfg(test)]
            Self::TestPanicActor { tx } => tx.send(Err(error)),
        }
        true
    }

    fn respond_terminal_outcome(
        self,
        conn: &Connection,
        in_txn: &AtomicBool,
        outcome: &TransactionTerminalOutcome,
    ) -> bool {
        publish_transaction_state(conn, in_txn);
        macro_rules! send_operation_terminal {
            ($tx:expr) => {
                $tx.send(Ok(TransactionOperationResponse {
                    result: Err(outcome.nonterminal_error()),
                    ownership_ended: true,
                }))
            };
        }
        match self {
            Self::TransactionPrepare { tx, .. }
            | Self::TransactionExecuteBatch { tx, .. }
            | Self::TransactionSavepoint { tx, .. } => send_operation_terminal!(tx),
            Self::TransactionQuery { tx, .. } => send_operation_terminal!(tx),
            Self::TransactionQueryWithParamsStream {
                row_tx,
                terminal_tx,
                ..
            } => {
                drop(row_tx);
                send_operation_terminal!(terminal_tx);
            }
            Self::TransactionQueryRow { tx, .. } => send_operation_terminal!(tx),
            Self::TransactionExecute { tx, .. } | Self::TransactionExecuteMany { tx, .. } => {
                send_operation_terminal!(tx)
            }
            Self::TransactionLastInsertRowid { tx, .. } => send_operation_terminal!(tx),
            Self::TransactionCommit { tx, .. } => {
                tx.send(Ok(TransactionTerminalResponse {
                    result: outcome.commit_result(),
                    ownership_ended: true,
                }));
            }
            Self::TransactionRollback { tx, .. } => {
                tx.send(Ok(TransactionTerminalResponse {
                    result: outcome.rollback_result(),
                    ownership_ended: true,
                }));
            }
            Self::Prepare { .. }
            | Self::Query { .. }
            | Self::QueryWithParams { .. }
            | Self::QueryWithParamsStream { .. }
            | Self::QueryRow { .. }
            | Self::QueryRowWithParams { .. }
            | Self::Execute { .. }
            | Self::ExecuteWithParams { .. }
            | Self::ExecuteBatch { .. }
            | Self::BeginOwnedTransaction { .. }
            | Self::LastInsertRowid { .. }
            | Self::Close { .. } => {
                unreachable!("only token-scoped commands can consume terminal metadata")
            }
            #[cfg(test)]
            Self::TestBlockActor { .. } | Self::TestPanicActor { .. } => {
                unreachable!("test actor controls are never transaction-token scoped")
            }
        }
        true
    }

    fn respond_cancelled(self, conn: &Connection, in_txn: &AtomicBool) -> bool {
        if matches!(&self, Self::Close { .. }) {
            return self.respond_error(
                conn,
                in_txn,
                FrankenError::Internal(
                    "close commands must not carry operation cancellation".to_owned(),
                ),
            );
        }
        self.respond_error(conn, in_txn, FrankenError::Interrupt)
    }
}

fn process_command(
    conn: &mut Connection,
    in_txn: &AtomicBool,
    close_succeeded: &AtomicBool,
    transaction_owner: &mut ActorTransactionOwner,
    transaction_cleanup: &Arc<TransactionDropCleanup>,
    _lifecycle: &WorkerLifecycle,
    envelope: CommandEnvelope,
) -> bool {
    let CommandEnvelope {
        cancellation,
        command: cmd,
    } = envelope;
    let token = cmd.transaction_token();
    if let Some(outcome) = token.and_then(|token| transaction_owner.terminal_outcome(token)) {
        let keep_running = cmd.respond_terminal_outcome(conn, in_txn, &outcome);
        assert_actor_transaction_owner(conn, transaction_owner);
        return keep_running;
    }

    if cancellation
        .as_ref()
        .is_some_and(CancellationRelay::is_requested)
    {
        let keep_running = cmd.respond_cancelled(conn, in_txn);
        assert_actor_transaction_owner(conn, transaction_owner);
        return keep_running;
    }

    if let Some(poisoned) = transaction_cleanup.poisoned_token()
        && !cmd.bypasses_cleanup_poison()
    {
        let keep_running = cmd.respond_error(
            conn,
            in_txn,
            FrankenError::Internal(format!(
                "async transaction cleanup for generation {} is poisoned",
                poisoned.generation
            )),
        );
        assert_actor_transaction_owner(conn, transaction_owner);
        return keep_running;
    }

    if !cmd.bypasses_transaction_owner()
        && match transaction_owner.owner.as_ref() {
            Some(owner) => token != Some(owner.token),
            None => token.is_some(),
        }
    {
        let keep_running = cmd.respond_error(conn, in_txn, FrankenError::Busy);
        assert_actor_transaction_owner(conn, transaction_owner);
        return keep_running;
    }

    if let Err(error) = cmd.validate_sql_transaction_control() {
        let keep_running = cmd.respond_error(conn, in_txn, error);
        assert_actor_transaction_owner(conn, transaction_owner);
        return keep_running;
    }
    if let (Some(token), Some(operation)) = (token, cmd.transaction_operation_label()) {
        transaction_owner.begin_operation(token, operation);
    }

    macro_rules! actor_operation {
        ($operation:expr) => {{
            if let Some(cancellation) = cancellation.as_ref() {
                match conn.enter_actor_operation(cancellation.clone()) {
                    Ok(_guard) => {
                        #[cfg(test)]
                        _lifecycle.pause_actor_operation_once(cancellation);
                        future::block_on($operation)
                    }
                    Err(error) => Err(error),
                }
            } else {
                future::block_on($operation)
            }
        }};
    }

    match cmd {
        Command::Prepare { sql, tx } => {
            let result = actor_operation!(conn.prepare(&sql)).map(drop);
            respond(conn, in_txn, tx, result);
        }
        Command::Query { sql, tx } => {
            let result = actor_operation!(conn.query(&sql));
            respond(conn, in_txn, tx, result);
        }
        Command::QueryWithParams { sql, params, tx } => {
            let result = actor_operation!(conn.query_with_params(&sql, &params));
            respond(conn, in_txn, tx, result);
        }
        Command::QueryWithParamsStream { sql, params, tx } => {
            #[cfg(test)]
            if _lifecycle.take_ordinary_stream_panic() {
                panic!("injected ordinary synchronous stream panic");
            }
            let result = actor_operation!(conn.query_with_params_for_each(&sql, &params, |row| {
                tx.send(Ok(Some(row.clone())))
                    .map_err(|_| stream_consumer_dead_err())
            }));
            publish_transaction_state(conn, in_txn);
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
            let result = actor_operation!(conn.query_row(&sql));
            respond(conn, in_txn, tx, result);
        }
        Command::QueryRowWithParams { sql, params, tx } => {
            let result = actor_operation!(conn.query_row_with_params(&sql, &params));
            respond(conn, in_txn, tx, result);
        }
        Command::Execute { sql, tx } => {
            let result = actor_operation!(conn.execute(&sql));
            respond(conn, in_txn, tx, result);
        }
        Command::ExecuteWithParams { sql, params, tx } => {
            let result = actor_operation!(conn.execute_with_params(&sql, &params));
            respond(conn, in_txn, tx, result);
        }
        Command::ExecuteBatch { sql, tx } => {
            let result = actor_operation!(conn.execute_batch(&sql));
            respond(conn, in_txn, tx, result);
        }
        Command::BeginOwnedTransaction { connection_id, tx } => {
            let result =
                if connection_id != transaction_owner.connection_id || conn.in_transaction() {
                    Err(FrankenError::Busy)
                } else {
                    match transaction_owner.allocate() {
                        Ok(token) => match actor_operation!(conn.begin_transaction()) {
                            Ok(()) => {
                                let terminal = Arc::new(TransactionTerminalReceipt::new(token));
                                transaction_owner.begin(token, Arc::clone(&terminal));
                                Ok(OwnedTransactionReceipt::new(
                                    token,
                                    Arc::clone(transaction_cleanup),
                                    terminal,
                                ))
                            }
                            Err(error) => Err(error),
                        },
                        Err(error) => Err(error),
                    }
                };
            respond(conn, in_txn, tx, result);
        }
        Command::TransactionPrepare { token, sql, tx } => {
            let result = actor_operation!(conn.prepare(&sql)).map(drop);
            respond_transaction_operation(
                conn,
                in_txn,
                transaction_owner,
                token,
                tx,
                result,
                _lifecycle,
            );
        }
        Command::TransactionQuery {
            token,
            sql,
            params,
            tx,
        } => {
            let result = actor_operation!(conn.query_with_params(&sql, &params));
            respond_transaction_operation(
                conn,
                in_txn,
                transaction_owner,
                token,
                tx,
                result,
                _lifecycle,
            );
        }
        Command::TransactionQueryWithParamsStream {
            token,
            sql,
            params,
            row_tx,
            terminal_tx,
        } => {
            let result = actor_operation!(conn.query_with_params_for_each(&sql, &params, |row| {
                let send_result = row_tx
                    .send(Some(row.clone()))
                    .map_err(|_| stream_consumer_dead_err());
                #[cfg(test)]
                if send_result.is_ok() && _lifecycle.take_transaction_stream_panic() {
                    panic!("injected transaction synchronous stream panic after row publication");
                }
                send_result
            }));
            let _ = row_tx.send(None);
            respond_transaction_operation(
                conn,
                in_txn,
                transaction_owner,
                token,
                terminal_tx,
                result,
                _lifecycle,
            );
        }
        Command::TransactionQueryRow {
            token,
            sql,
            params,
            tx,
        } => {
            let result = actor_operation!(conn.query_row_with_params(&sql, &params));
            respond_transaction_operation(
                conn,
                in_txn,
                transaction_owner,
                token,
                tx,
                result,
                _lifecycle,
            );
        }
        Command::TransactionExecute {
            token,
            sql,
            params,
            tx,
        } => {
            let result = actor_operation!(conn.execute_with_params(&sql, &params));
            #[cfg(test)]
            if result.is_ok() && _lifecycle.take_successful_nonterminal_ownership_ending() {
                future::block_on(conn.rollback_transaction())
                    .expect("test injection must end the live transaction after statement success");
            }
            respond_transaction_operation(
                conn,
                in_txn,
                transaction_owner,
                token,
                tx,
                result,
                _lifecycle,
            );
        }
        Command::TransactionExecuteMany {
            token,
            sql,
            parameter_sets,
            tx,
        } => {
            let result = actor_operation!(
                conn.execute_many_with_params_skip_statement_savepoint_in_explicit_txn(
                    &sql,
                    &parameter_sets,
                )
            );
            respond_transaction_operation(
                conn,
                in_txn,
                transaction_owner,
                token,
                tx,
                result,
                _lifecycle,
            );
        }
        Command::TransactionExecuteBatch { token, sql, tx } => {
            let result = actor_operation!(conn.execute_batch(&sql));
            respond_transaction_operation(
                conn,
                in_txn,
                transaction_owner,
                token,
                tx,
                result,
                _lifecycle,
            );
        }
        Command::TransactionSavepoint {
            token,
            action,
            name,
            tx,
        } => {
            let name = quote_savepoint_name(&name);
            let sql = match action {
                TransactionSavepointAction::Create => format!("SAVEPOINT {name}"),
                TransactionSavepointAction::Release => format!("RELEASE SAVEPOINT {name}"),
                TransactionSavepointAction::RollbackTo => {
                    format!("ROLLBACK TO SAVEPOINT {name}")
                }
            };
            let result = actor_operation!(conn.execute(&sql)).map(drop);
            respond_transaction_operation(
                conn,
                in_txn,
                transaction_owner,
                token,
                tx,
                result,
                _lifecycle,
            );
        }
        Command::TransactionCommit { token, tx } => {
            #[cfg(test)]
            let result = if _lifecycle.take_terminal_pre_effect_failure() {
                Err(FrankenError::Internal(
                    "injected commit error while transaction remains active".to_owned(),
                ))
            } else {
                actor_operation!(conn.commit_transaction())
            };
            #[cfg(not(test))]
            let result = actor_operation!(conn.commit_transaction());
            #[cfg(test)]
            let result = if result.is_ok() && _lifecycle.take_terminal_post_effect_failure() {
                Err(FrankenError::Internal(
                    "injected terminal error after transaction end".to_owned(),
                ))
            } else {
                result
            };
            #[cfg(test)]
            if result.is_ok() && _lifecycle.take_terminal_pre_publication_panic() {
                panic!("injected commit panic before terminal receipt publication");
            }
            respond_transaction_terminal(
                conn,
                in_txn,
                transaction_owner,
                token,
                tx,
                result,
                TransactionTerminalOperation::Commit,
                _lifecycle,
            );
        }
        Command::TransactionRollback { token, tx } => {
            let result = actor_operation!(conn.rollback_transaction());
            #[cfg(test)]
            let result = if result.is_ok() && _lifecycle.take_terminal_post_effect_failure() {
                Err(FrankenError::Internal(
                    "injected terminal error after transaction end".to_owned(),
                ))
            } else {
                result
            };
            #[cfg(test)]
            if result.is_ok() && _lifecycle.take_terminal_pre_publication_panic() {
                panic!("injected rollback panic before terminal receipt publication");
            }
            respond_transaction_terminal(
                conn,
                in_txn,
                transaction_owner,
                token,
                tx,
                result,
                TransactionTerminalOperation::Rollback,
                _lifecycle,
            );
        }
        Command::TransactionLastInsertRowid { token, tx } => {
            respond_transaction_operation(
                conn,
                in_txn,
                transaction_owner,
                token,
                tx,
                Ok(conn.last_insert_rowid()),
                _lifecycle,
            );
        }
        Command::LastInsertRowid { tx } => {
            respond(conn, in_txn, tx, Ok(conn.last_insert_rowid()));
        }
        Command::Close { tx } => {
            #[cfg(test)]
            _lifecycle.note_close_connection_call();
            let pre_close_live_token = transaction_owner
                .current_token()
                .filter(|_| conn.in_transaction());
            let result = future::block_on(conn.close_in_place());
            #[cfg(test)]
            let result = if result.is_ok() && _lifecycle.take_close_post_effect_failure() {
                Err(FrankenError::Internal(
                    "injected connection-close error after transaction end".to_owned(),
                ))
            } else {
                result
            };
            let closed = result.is_ok();
            if let Some(token) = transaction_owner.current_token() {
                if closed && pre_close_live_token == Some(token) && !conn.in_transaction() {
                    transaction_owner.publish_terminal(
                        token,
                        TransactionTerminalOutcome::RolledBack {
                            reason: Arc::from(
                                "connection close completed rollback of the live transaction",
                            ),
                        },
                    );
                } else if !conn.in_transaction() {
                    let detail = match &result {
                        Ok(()) => {
                            "connection close found the transaction already ended without a terminal receipt"
                                .to_owned()
                        }
                        Err(error) => format!(
                            "connection close returned an error after the transaction disappeared: {error}"
                        ),
                    };
                    transaction_owner.publish_unknown(token, "connection close", detail);
                }
            }
            if closed {
                close_succeeded.store(true, Ordering::Release);
                in_txn.store(false, Ordering::Release);
            } else {
                publish_transaction_state(conn, in_txn);
            }
            tx.send(result);
            #[cfg(test)]
            _lifecycle.pause_close_after_publication_once();
            assert_actor_transaction_owner(conn, transaction_owner);
            return !closed;
        }
        #[cfg(test)]
        Command::TestBlockActor {
            entered,
            release,
            tx,
        } => {
            entered.send(());
            let result = release.recv().map_err(|_| {
                FrankenError::Internal("test actor release channel disconnected".to_owned())
            });
            respond(conn, in_txn, tx, result);
        }
        #[cfg(test)]
        Command::TestPanicActor { tx: _ } => {
            panic!("injected async actor panic");
        }
    }
    assert_actor_transaction_owner(conn, transaction_owner);
    true
}

fn worker_loop(
    conn: &mut Connection,
    in_txn: &AtomicBool,
    mut rx: async_mpsc::Receiver<CommandEnvelope>,
    mut cleanup_rx: async_mpsc::Receiver<()>,
    transaction_owner: &mut ActorTransactionOwner,
    transaction_cleanup: &Arc<TransactionDropCleanup>,
    connection_id: u64,
    close_succeeded: &AtomicBool,
    lifecycle: &WorkerLifecycle,
) {
    enum WorkerEvent {
        Command(Result<CommandEnvelope, async_mpsc::RecvError>),
        Cleanup(Result<(), async_mpsc::RecvError>),
    }

    let mailbox_cx = NativeCx::<native_cap::None>::detached_cancel_context();
    assert_eq!(
        transaction_owner.connection_id, connection_id,
        "worker transaction ownership must be bound to its connection"
    );
    loop {
        process_transaction_drop_cleanup(
            conn,
            in_txn,
            transaction_owner,
            transaction_cleanup,
            lifecycle,
        );
        let event = future::block_on(future::race(
            async { WorkerEvent::Command(rx.recv(&mailbox_cx).await) },
            async { WorkerEvent::Cleanup(cleanup_rx.recv(&mailbox_cx).await) },
        ));
        let envelope = match event {
            WorkerEvent::Command(Ok(envelope)) => {
                lifecycle.command_capacity.notify();
                envelope
            }
            WorkerEvent::Command(Err(async_mpsc::RecvError::Disconnected))
            | WorkerEvent::Cleanup(Err(async_mpsc::RecvError::Disconnected)) => return,
            WorkerEvent::Command(Err(
                async_mpsc::RecvError::Cancelled | async_mpsc::RecvError::Empty,
            ))
            | WorkerEvent::Cleanup(Err(
                async_mpsc::RecvError::Cancelled | async_mpsc::RecvError::Empty,
            ))
            | WorkerEvent::Cleanup(Ok(())) => continue,
        };
        // Drop cleanup has priority over an ordinary command that happened to
        // become ready in the same turn. This is what makes the dedicated wake
        // lossless even when the ordinary mailbox is full.
        process_transaction_drop_cleanup(
            conn,
            in_txn,
            transaction_owner,
            transaction_cleanup,
            lifecycle,
        );
        if !process_command(
            conn,
            in_txn,
            close_succeeded,
            transaction_owner,
            transaction_cleanup,
            lifecycle,
            envelope,
        ) {
            return;
        }
    }
}

struct WorkerHandle {
    join: Option<JoinHandle<()>>,
    lifecycle: Arc<WorkerLifecycle>,
}

fn panic_payload_detail(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

impl WorkerHandle {
    fn wait_sync(mut self) {
        if let Some(join) = self.join.take() {
            #[cfg(test)]
            self.lifecycle.join_calls.fetch_add(1, Ordering::AcqRel);
            if let Err(payload) = join.join() {
                self.lifecycle.publish_broken(
                    WorkerFailureStage::Join,
                    panic_payload_detail(payload.as_ref()),
                    WorkerTransactionDisposition::OutcomeUnknown,
                );
            }
        }
    }

    async fn wait_async(&mut self) {
        WorkerExit::new(&self.lifecycle).await;
        while self.join.as_ref().is_some_and(|join| !join.is_finished()) {
            future::yield_now().await;
        }
        if let Some(join) = self.join.take() {
            #[cfg(test)]
            self.lifecycle.join_calls.fetch_add(1, Ordering::AcqRel);
            if let Err(payload) = join.join() {
                self.lifecycle.publish_broken(
                    WorkerFailureStage::Join,
                    panic_payload_detail(payload.as_ref()),
                    WorkerTransactionDisposition::OutcomeUnknown,
                );
            }
        }
    }
}

fn spawn_worker_thread(
    path: String,
    env: ConnectionEnv,
    cmd_rx: async_mpsc::Receiver<CommandEnvelope>,
    cleanup_rx: async_mpsc::Receiver<()>,
    transaction_cleanup: Arc<TransactionDropCleanup>,
    connection_id: u64,
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
            match catch_unwind(AssertUnwindSafe(|| {
                future::block_on(Connection::open_with_env(path, env))
            })) {
                Ok(Ok(mut conn)) => {
                    let publication =
                        open_tx.send_prefer_cancellation(Ok(()), Err(FrankenError::Interrupt));
                    if publication != ResponsePublication::Primary {
                        #[cfg(test)]
                        lifecycle.note_close_connection_call();
                        match catch_unwind(AssertUnwindSafe(|| {
                            future::block_on(conn.close_in_place())
                        })) {
                            Ok(Ok(())) => {
                                in_txn.store(false, Ordering::Release);
                                lifecycle.publish_closed();
                            }
                            Ok(Err(error)) => lifecycle.publish_broken(
                                WorkerFailureStage::Close,
                                error.to_string(),
                                WorkerTransactionDisposition::NoActiveTransaction,
                            ),
                            Err(payload) => lifecycle.publish_broken(
                                WorkerFailureStage::Close,
                                panic_payload_detail(payload.as_ref()),
                                WorkerTransactionDisposition::OutcomeUnknown,
                            ),
                        }
                        return;
                    }

                    // bd-bjm5d: this thread is a dedicated engine OS thread.
                    // A bounded inline pread/pwrite can stall only this
                    // connection's next command.
                    conn.root_cx().mark_blocking_io_inline_safe();
                    let close_succeeded = AtomicBool::new(false);
                    let mut transaction_owner = ActorTransactionOwner::new(connection_id);
                    let worker_failure = catch_unwind(AssertUnwindSafe(|| {
                        worker_loop(
                            &mut conn,
                            &in_txn,
                            cmd_rx,
                            cleanup_rx,
                            &mut transaction_owner,
                            &transaction_cleanup,
                            connection_id,
                            &close_succeeded,
                            &lifecycle,
                        );
                    }))
                    .err()
                    .map(|payload| panic_payload_detail(payload.as_ref()));
                    let unpublished_in_flight_commit =
                        transaction_owner.unpublished_in_flight_commit();
                    if close_succeeded.load(Ordering::Acquire) {
                        transaction_owner.publish_unknown_if_owned(
                            "connection close returned success without publishing a generation-specific terminal receipt",
                        );
                        in_txn.store(false, Ordering::Release);
                        if let Some(detail) = worker_failure {
                            let disposition = if unpublished_in_flight_commit.is_some() {
                                WorkerTransactionDisposition::OutcomeUnknown
                            } else {
                                WorkerTransactionDisposition::NoActiveTransaction
                            };
                            lifecycle.publish_broken(
                                WorkerFailureStage::ActorLoop,
                                detail,
                                disposition,
                            );
                        } else if unpublished_in_flight_commit.is_some() {
                            lifecycle.publish_broken(
                                WorkerFailureStage::Lifecycle,
                                "connection close completed while an unpublished commit operation still owned the transaction generation",
                                WorkerTransactionDisposition::OutcomeUnknown,
                            );
                        } else {
                            lifecycle.publish_closed();
                        }
                        return;
                    }

                    let pre_close_live_token = transaction_owner
                        .current_token()
                        .filter(|_| conn.in_transaction());
                    let transaction_was_active =
                        conn.in_transaction() || in_txn.load(Ordering::Acquire);
                    #[cfg(test)]
                    lifecycle.note_close_connection_call();
                    let close_result =
                        catch_unwind(AssertUnwindSafe(|| future::block_on(conn.close_in_place())));
                    match close_result {
                        Ok(Ok(())) => {
                            if let Some(token) = transaction_owner.current_token() {
                                if unpublished_in_flight_commit == Some(token) {
                                    transaction_owner.publish_unknown(
                                        token,
                                        "commit",
                                        "the worker failed after commit began but before a generation-specific commit result was published",
                                    );
                                } else if pre_close_live_token == Some(token) {
                                    transaction_owner.publish_terminal(
                                        token,
                                        TransactionTerminalOutcome::RolledBack {
                                            reason: Arc::from(
                                                "worker terminal close completed rollback of the live transaction",
                                            ),
                                        },
                                    );
                                } else {
                                    transaction_owner.publish_unknown_if_owned(
                                        "the transaction ended before worker-terminal close began and no generation-specific terminal receipt was published",
                                    );
                                }
                            }
                            in_txn.store(false, Ordering::Release);
                            if let Some(detail) = worker_failure {
                                let disposition = if unpublished_in_flight_commit.is_some() {
                                    WorkerTransactionDisposition::OutcomeUnknown
                                } else {
                                    WorkerTransactionDisposition::NoActiveTransaction
                                };
                                lifecycle.publish_broken(
                                    WorkerFailureStage::ActorLoop,
                                    detail,
                                    disposition,
                                );
                            } else if unpublished_in_flight_commit.is_some() {
                                lifecycle.publish_broken(
                                    WorkerFailureStage::Lifecycle,
                                    "worker-terminal close completed while commit outcome remained unpublished",
                                    WorkerTransactionDisposition::OutcomeUnknown,
                                );
                            } else {
                                lifecycle.publish_closed();
                            }
                        }
                        Ok(Err(error)) => {
                            transaction_owner.publish_unknown_if_owned(format!(
                                "worker-terminal close failed without proving the transaction disposition: {error}"
                            ));
                            let disposition = if unpublished_in_flight_commit.is_some() {
                                WorkerTransactionDisposition::OutcomeUnknown
                            } else if conn.in_transaction() {
                                WorkerTransactionDisposition::Active
                            } else if transaction_was_active {
                                WorkerTransactionDisposition::OutcomeUnknown
                            } else {
                                WorkerTransactionDisposition::NoActiveTransaction
                            };
                            if !matches!(
                                disposition,
                                WorkerTransactionDisposition::NoActiveTransaction
                            ) {
                                in_txn.store(true, Ordering::Release);
                            }
                            if let Some(detail) = worker_failure {
                                lifecycle.publish_broken(
                                    WorkerFailureStage::ActorLoop,
                                    detail,
                                    disposition,
                                );
                            }
                            lifecycle.publish_broken(
                                WorkerFailureStage::Close,
                                error.to_string(),
                                disposition,
                            );
                        }
                        Err(payload) => {
                            let panic_detail = panic_payload_detail(payload.as_ref());
                            transaction_owner.publish_unknown_if_owned(format!(
                                "worker-terminal close panicked without proving the transaction disposition: {panic_detail}"
                            ));
                            let disposition = if unpublished_in_flight_commit.is_some()
                                || transaction_was_active
                            {
                                WorkerTransactionDisposition::OutcomeUnknown
                            } else {
                                WorkerTransactionDisposition::NoActiveTransaction
                            };
                            if !matches!(
                                disposition,
                                WorkerTransactionDisposition::NoActiveTransaction
                            ) {
                                in_txn.store(true, Ordering::Release);
                            }
                            if let Some(detail) = worker_failure {
                                lifecycle.publish_broken(
                                    WorkerFailureStage::ActorLoop,
                                    detail,
                                    disposition,
                                );
                            }
                            lifecycle.publish_broken(
                                WorkerFailureStage::Close,
                                panic_detail,
                                disposition,
                            );
                        }
                    }
                }
                Ok(Err(error)) => {
                    let detail = error.to_string();
                    lifecycle.publish_broken(
                        WorkerFailureStage::Open,
                        detail,
                        WorkerTransactionDisposition::NoActiveTransaction,
                    );
                    let _ =
                        open_tx.send_prefer_cancellation(Err(error), Err(FrankenError::Interrupt));
                }
                Err(payload) => {
                    let detail = panic_payload_detail(payload.as_ref());
                    lifecycle.publish_broken(
                        WorkerFailureStage::Open,
                        detail.clone(),
                        WorkerTransactionDisposition::NoActiveTransaction,
                    );
                    let _ = open_tx.send_prefer_cancellation(
                        Err(FrankenError::Internal(format!(
                            "async worker open panicked: {detail}"
                        ))),
                        Err(FrankenError::Interrupt),
                    );
                }
            }
        })
        .map_err(worker_thread_spawn_err)
}

fn start_worker(
    path: String,
    env: ConnectionEnv,
    connection_id: u64,
    open_cancellation: Option<CancellationRelay>,
) -> Result<
    (
        CommandSender,
        WorkerHandle,
        Arc<AtomicBool>,
        Arc<TransactionDropCleanup>,
        ResponseReceiver<Result<(), FrankenError>>,
    ),
    FrankenError,
> {
    let (cmd_tx, cmd_rx) = async_mpsc::channel(COMMAND_CAPACITY);
    let (cleanup_wake_tx, cleanup_rx) = async_mpsc::channel(1);
    let transaction_cleanup = Arc::new(TransactionDropCleanup::new(connection_id, cleanup_wake_tx));
    let (open_tx, open_rx) = response_channel(open_cancellation);
    let in_txn = Arc::new(AtomicBool::new(false));
    let command_capacity = Arc::new(CommandCapacitySignal::new());
    let lifecycle = Arc::new(WorkerLifecycle::new(Arc::clone(&command_capacity)));
    let join = spawn_worker_thread(
        path,
        env,
        cmd_rx,
        cleanup_rx,
        Arc::clone(&transaction_cleanup),
        connection_id,
        open_tx,
        Arc::clone(&in_txn),
        Arc::clone(&lifecycle),
    )?;
    open_rx.mark_admitted();
    Ok((
        CommandSender {
            tx: Some(cmd_tx),
            command_capacity,
            lifecycle: Arc::clone(&lifecycle),
        },
        WorkerHandle {
            join: Some(join),
            lifecycle,
        },
        in_txn,
        transaction_cleanup,
        open_rx,
    ))
}

fn wait_for_worker_open(
    open_rx: ResponseReceiver<Result<(), FrankenError>>,
    lifecycle: &WorkerLifecycle,
) -> Result<(), FrankenError> {
    match open_rx.recv_blocking() {
        Ok(result) => result,
        Err(ResponseDisconnected) => {
            lifecycle.wait_finished_sync();
            Err(lifecycle.terminal_error())
        }
    }
}

async fn wait_for_worker_open_async<Caps>(
    cx: &Cx<Caps>,
    open_rx: ResponseReceiver<Result<(), FrankenError>>,
    cancellation: CancellationRelay,
    polling_native_cx: NativeCx,
    lifecycle: &WorkerLifecycle,
) -> Result<(), FrankenError>
where
    Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
{
    recv_authoritative_worker_response(cx, open_rx, cancellation, polling_native_cx, lifecycle)
        .await
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
/// All async methods accept a `&Cx`. Cancellation before bounded-mailbox
/// admission returns without touching the underlying connection. Cancellation
/// after admission is relayed to the worker-owned operation and does not
/// fabricate a competing terminal result. Admission-side cancellation is
/// reported as [`FrankenError::Interrupt`]; an already-running engine operation
/// may instead publish [`FrankenError::Abort`] when its execution checkpoint
/// observes that relay. In either case the worker response remains
/// authoritative and the connection root context is not cancelled.
///
/// The connection itself lives on a dedicated large-stack worker thread (because
/// [`Connection`] is `!Send`). Commands are dispatched via an internal channel
/// and results flow back through response waiters owned by the caller runtime.
/// The facade handle itself is `Send + Sync` and may be moved or shared across
/// executor threads; the actor remains the only owner of the raw connection.
///
/// # Shutdown
///
/// Dropping `AsyncConnection` disconnects its mailbox without blocking. The
/// worker drains admitted commands, explicitly closes the underlying
/// [`Connection`], and then exits. Use explicit close to observe cleanup
/// errors.
///
/// For explicit, error-checked shutdown use [`close`](Self::close) on the
/// async path or [`close_sync`](Self::close_sync) on the synchronous path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsyncConnectionState {
    Open,
    Closing,
    Closed,
    Broken,
}

static NEXT_ASYNC_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

fn allocate_async_connection_id() -> Result<u64, FrankenError> {
    NEXT_ASYNC_CONNECTION_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .map_err(|_| FrankenError::Internal("async connection identity exhausted".to_owned()))
}

thread_local! {
    static ACTIVE_STREAM_CALLBACKS: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
}

struct StreamCallbackGuard {
    connection_id: u64,
}

impl StreamCallbackGuard {
    fn enter(connection_id: u64) -> Self {
        ACTIVE_STREAM_CALLBACKS.with(|active| {
            let mut active = active.borrow_mut();
            debug_assert!(
                !active.contains(&connection_id),
                "same-connection stream callback reentered without admission"
            );
            active.push(connection_id);
        });
        Self { connection_id }
    }
}

impl Drop for StreamCallbackGuard {
    fn drop(&mut self) {
        ACTIVE_STREAM_CALLBACKS.with(|active| {
            let removed = active.borrow_mut().pop();
            debug_assert_eq!(
                removed,
                Some(self.connection_id),
                "stream callback guards must unwind in stack order"
            );
        });
    }
}

pub struct AsyncConnection {
    cmd_tx: Option<CommandSender>,
    worker: Option<WorkerHandle>,
    lifecycle: Arc<WorkerLifecycle>,
    /// Retained after Close admission, so dropping a close future cannot lose
    /// the worker's authoritative cleanup result.
    close_response: Option<ResponseReceiver<Result<(), FrankenError>>>,
    state: AsyncConnectionState,
    /// Published by the worker before every command response, including
    /// scoped transaction operations and rejected ownerless control attempts.
    in_txn: Arc<AtomicBool>,
    /// Actor-owned rollback obligation slot used by scoped transaction Drop.
    transaction_cleanup: Arc<TransactionDropCleanup>,
    /// Stable, non-reused process-local identity fencing transaction tokens
    /// and same-thread stream callback reentry.
    connection_id: u64,
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
        env.ensure_dedicated_actor_safe()?;
        if NativeCx::is_active() {
            return Err(sync_on_runtime_err());
        }
        let connection_id = allocate_async_connection_id()?;
        let (cmd_tx, worker, in_txn, transaction_cleanup, open_rx) =
            start_worker(path.into(), env, connection_id, None)?;
        match wait_for_worker_open(open_rx, &worker.lifecycle) {
            Ok(()) => {
                let lifecycle = Arc::clone(&worker.lifecycle);
                Ok(Self {
                    cmd_tx: Some(cmd_tx),
                    worker: Some(worker),
                    lifecycle,
                    close_response: None,
                    state: AsyncConnectionState::Open,
                    in_txn,
                    transaction_cleanup,
                    connection_id,
                })
            }
            Err(error) => {
                drop(cmd_tx);
                worker.wait_sync();
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
        env.ensure_dedicated_actor_safe()?;
        checkpoint_or_interrupt(cx)?;
        let polling_native_cx = native_cx_for_polling_task()?;
        if polling_native_cx.checkpoint().is_err() {
            return Err(FrankenError::Interrupt);
        }
        let cancellation = cx.cancellation_relay();
        if cancellation.is_requested() {
            return Err(FrankenError::Interrupt);
        }
        let connection_id = allocate_async_connection_id()?;
        let (cmd_tx, mut worker, in_txn, transaction_cleanup, open_rx) =
            start_worker(path.into(), env, connection_id, Some(cancellation.clone()))?;

        match wait_for_worker_open_async(
            cx,
            open_rx,
            cancellation,
            polling_native_cx,
            &worker.lifecycle,
        )
        .await
        {
            Ok(()) => {
                let lifecycle = Arc::clone(&worker.lifecycle);
                Ok(Self {
                    cmd_tx: Some(cmd_tx),
                    worker: Some(worker),
                    lifecycle,
                    close_response: None,
                    state: AsyncConnectionState::Open,
                    in_txn,
                    transaction_cleanup,
                    connection_id,
                })
            }
            Err(error) => {
                drop(cmd_tx);
                worker.wait_async().await;
                if matches!(&error, FrankenError::Interrupt)
                    && matches!(
                        worker.lifecycle.terminal_state(),
                        WorkerTerminalState::Broken { .. }
                    )
                {
                    Err(worker.lifecycle.terminal_error())
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Return a reference to the command sender, or an error if the worker is gone.
    fn sender(&self) -> Result<&CommandSender, FrankenError> {
        match self.state {
            AsyncConnectionState::Open => {}
            AsyncConnectionState::Broken => return Err(self.lifecycle.terminal_error()),
            AsyncConnectionState::Closing | AsyncConnectionState::Closed => {
                return Err(FrankenError::Internal(
                    "AsyncConnection is closing or closed".to_owned(),
                ));
            }
        }
        if ACTIVE_STREAM_CALLBACKS.with(|active| active.borrow().contains(&self.connection_id)) {
            return Err(FrankenError::Busy);
        }
        self.cmd_tx.as_ref().ok_or_else(worker_dead_err)
    }

    async fn request_async<Caps, T, F>(&self, cx: &Cx<Caps>, build: F) -> Result<T, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
        T: Send + 'static,
        F: FnOnce(Responder<T>) -> Command,
    {
        let sender = self.sender()?;
        let lifecycle = Arc::clone(&sender.lifecycle);
        let (response, cancellation, polling_native_cx) = sender.request_async(cx, build).await?;
        recv_authoritative_worker_response(
            cx,
            response,
            cancellation,
            polling_native_cx,
            &lifecycle,
        )
        .await
    }

    /// Validate and prepare one SQL statement on the dedicated worker.
    ///
    /// This is the synchronous-consumer counterpart to the async methods
    /// below. It intentionally performs no cancellation check and blocks the
    /// caller until the worker responds.
    pub fn prepare_sync(&self, sql: &str) -> Result<(), FrankenError> {
        self.sender()?.request_sync(|tx| Command::Prepare {
            sql: sql.to_owned(),
            tx,
        })
    }

    /// Execute a query through the dedicated worker and block for all rows.
    pub fn query_sync(&self, sql: &str) -> Result<Vec<Row>, FrankenError> {
        self.sender()?.request_sync(|tx| Command::Query {
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
        self.sender()?.request_sync(|tx| Command::QueryWithParams {
            sql: sql.to_owned(),
            params: params.to_vec(),
            tx,
        })
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
        let sender = self.sender()?;
        let lifecycle = Arc::clone(&sender.lifecycle);
        let (tx, rx) = sync_mpsc::sync_channel(1);
        sender.send_stream_sync(Command::QueryWithParamsStream {
            sql: sql.to_owned(),
            params: params.to_vec(),
            tx,
        })?;

        loop {
            match rx.recv() {
                Ok(Ok(Some(row))) => {
                    let _guard = StreamCallbackGuard::enter(self.connection_id);
                    f(&row)?;
                }
                Ok(Ok(None)) => return Ok(()),
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    lifecycle.wait_finished_sync();
                    return Err(lifecycle.terminal_error());
                }
            }
        }
    }

    /// Execute a query through the dedicated worker and return exactly one row.
    pub fn query_row_sync(&self, sql: &str) -> Result<Row, FrankenError> {
        self.sender()?.request_sync(|tx| Command::QueryRow {
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
        self.sender()?
            .request_sync(|tx| Command::QueryRowWithParams {
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
    }

    /// Execute SQL through the dedicated worker.
    pub fn execute_sync(&self, sql: &str) -> Result<usize, FrankenError> {
        self.sender()?.request_sync(|tx| Command::Execute {
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
        self.sender()?
            .request_sync(|tx| Command::ExecuteWithParams {
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
    }

    /// Execute zero or more SQL statements through the dedicated worker.
    pub fn execute_batch_sync(&self, sql: &str) -> Result<(), FrankenError> {
        self.sender()?.request_sync(|tx| Command::ExecuteBatch {
            sql: sql.to_owned(),
            tx,
        })
    }

    /// Begin an actor-owned scoped transaction for synchronous callers.
    ///
    /// The returned handle is the transaction's ownership capability and must
    /// be retained until [`Transaction::commit_sync`] or
    /// [`Transaction::rollback_sync`]. Discarding it immediately schedules an
    /// actor rollback; subsequent ordinary connection calls are not part of
    /// that transaction.
    ///
    /// # Migration
    ///
    /// Code written for the former connection-owned transaction API must bind
    /// this return value and move all transactional SQL onto its token-scoped
    /// methods. A standalone `connection.begin_transaction_sync()?;` no longer
    /// keeps a transaction open.
    pub fn begin_transaction_sync(&self) -> Result<Transaction<'_>, FrankenError> {
        let receipt = self
            .sender()?
            .request_sync(|tx| Command::BeginOwnedTransaction {
                connection_id: self.connection_id,
                tx,
            })?;
        self.transaction_from_receipt(receipt)
    }

    fn transaction_from_receipt(
        &self,
        receipt: OwnedTransactionReceipt,
    ) -> Result<Transaction<'_>, FrankenError> {
        let (token, terminal) = receipt.into_parts();
        if let Some(outcome) = terminal.outcome() {
            return Err(outcome.nonterminal_error());
        }
        Ok(Transaction {
            connection: self,
            token,
            terminal,
            finalized: AtomicBool::new(false),
        })
    }

    /// Return the worker-owned connection's last inserted row identifier.
    pub fn last_insert_rowid_sync(&self) -> Result<i64, FrankenError> {
        self.sender()?
            .request_sync(|tx| Command::LastInsertRowid { tx })
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

    /// Begin an actor-owned scoped transaction.
    ///
    /// While the returned [`Transaction`] is live, ordinary connection
    /// commands return [`FrankenError::Busy`]. SQL inside the transaction must
    /// use the token-scoped methods on that handle. Dropping the handle without
    /// an awaited commit or rollback records a lossless actor rollback
    /// obligation.
    ///
    /// # Migration
    ///
    /// The returned handle is now the transaction's ownership capability.
    /// Bind it and issue transactional SQL through [`Transaction`]:
    ///
    /// ```no_run
    /// # use fsqlite::{AsyncConnection, FrankenError};
    /// # use fsqlite_types::cx::Cx;
    /// # async fn example(connection: &AsyncConnection, cx: &Cx) -> Result<(), FrankenError> {
    /// let mut transaction = connection.begin_transaction(cx).await?;
    /// transaction.execute(cx, "INSERT INTO t VALUES (1)").await?;
    /// transaction.commit(cx).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A standalone `connection.begin_transaction(cx).await?;` discards the
    /// handle, schedules rollback, and does not place later connection calls
    /// inside that transaction.
    pub async fn begin_transaction<Caps>(
        &self,
        cx: &Cx<Caps>,
    ) -> Result<Transaction<'_>, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let receipt = self
            .request_async(cx, |tx| Command::BeginOwnedTransaction {
                connection_id: self.connection_id,
                tx,
            })
            .await?;
        self.transaction_from_receipt(receipt)
    }

    /// Returns `true` if an explicit transaction is currently active.
    ///
    /// This is a cheap local read — no round-trip to the worker thread. A
    /// broken worker never publishes `false` unless terminal cleanup proved
    /// that no transaction obligation remains.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.in_txn.load(Ordering::Acquire)
    }

    fn apply_worker_terminal_state(&mut self) -> Result<(), FrankenError> {
        match self.lifecycle.terminal_state() {
            WorkerTerminalState::Closed => {
                self.state = AsyncConnectionState::Closed;
                Ok(())
            }
            WorkerTerminalState::Broken { .. } => {
                self.state = AsyncConnectionState::Broken;
                Err(self.lifecycle.terminal_error())
            }
            WorkerTerminalState::Running => {
                self.state = AsyncConnectionState::Broken;
                Err(FrankenError::Internal(
                    "async worker join completed without a terminal lifecycle state".to_owned(),
                ))
            }
        }
    }

    async fn terminalize_worker_async(&mut self) -> Result<(), FrankenError> {
        drop(self.cmd_tx.take());
        drop(self.close_response.take());
        if let Some(worker) = self.worker.as_mut() {
            worker.wait_async().await;
        } else if !self.lifecycle.is_finished() {
            WorkerExit::new(&self.lifecycle).await;
        }
        drop(self.worker.take());
        self.apply_worker_terminal_state()
    }

    fn terminalize_worker_sync(&mut self) -> Result<(), FrankenError> {
        drop(self.cmd_tx.take());
        drop(self.close_response.take());
        if let Some(worker) = self.worker.take() {
            worker.wait_sync();
        } else if !self.lifecycle.is_finished() {
            self.lifecycle.wait_finished_sync();
        }
        self.apply_worker_terminal_state()
    }

    /// Explicitly close the connection, returning any error from the close operation.
    ///
    /// Cancellation before Close admission leaves the connection open. After
    /// admission, the exact close response is retained across dropped futures.
    /// A terminal success or worker failure joins the worker before returning;
    /// an operational close error leaves only this method available for retry.
    pub async fn close<Caps>(&mut self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        if self.state == AsyncConnectionState::Closed {
            return Ok(());
        }
        if self.state == AsyncConnectionState::Broken {
            return self.terminalize_worker_async().await;
        }

        if self.close_response.is_none() {
            let Some(sender) = self.cmd_tx.as_ref() else {
                return self.terminalize_worker_async().await;
            };
            let response = match sender.request_close_async(cx).await {
                Ok(response) => response,
                Err(error) if is_worker_dead_error(&error) || self.lifecycle.is_finished() => {
                    return self.terminalize_worker_async().await;
                }
                Err(error) => return Err(error),
            };
            self.state = AsyncConnectionState::Closing;
            self.close_response = Some(response);
        }

        // Close cancellation is observed only before admission. Once admitted,
        // retain and drain this exact response even if the caller drops the
        // current close future.
        let response = self
            .close_response
            .as_mut()
            .expect("closing state retains its response")
            .await;
        self.close_response = None;
        match response {
            Ok(Ok(())) => self.terminalize_worker_async().await,
            Ok(Err(error)) => Err(error),
            Err(ResponseDisconnected) => self.terminalize_worker_async().await,
        }
    }

    /// Explicitly close a synchronously used connection and join its worker.
    pub fn close_sync(&mut self) -> Result<(), FrankenError> {
        if self.state == AsyncConnectionState::Closed {
            return Ok(());
        }
        if self.state == AsyncConnectionState::Broken {
            return self.terminalize_worker_sync();
        }
        if NativeCx::is_active() {
            return Err(sync_on_runtime_err());
        }

        if let Some(response) = self.close_response.take() {
            match response.recv_blocking() {
                Ok(Ok(())) => return self.terminalize_worker_sync(),
                Ok(Err(error)) => return Err(error),
                Err(ResponseDisconnected) => return self.terminalize_worker_sync(),
            }
        }

        let Some(sender) = self.cmd_tx.as_ref() else {
            return self.terminalize_worker_sync();
        };
        match sender.request_sync(|tx| Command::Close { tx }) {
            Ok(()) => {
                self.state = AsyncConnectionState::Closing;
                self.terminalize_worker_sync()
            }
            Err(error) => {
                if is_worker_dead_error(&error) || self.lifecycle.is_finished() {
                    self.terminalize_worker_sync()
                } else {
                    // The Close command was admitted and returned its own
                    // operational error. Keep the actor available only for a
                    // retry of this exact terminal operation.
                    self.state = AsyncConnectionState::Closing;
                    Err(error)
                }
            }
        }
    }
}

/// Scoped transaction whose authoritative ownership token lives in the
/// connection actor.
///
/// This value is intentionally not cloneable. Every command carries its token
/// back to the actor, which rejects stale generations and tokens belonging to a
/// different connection. Dropping an unfinished value schedules rollback
/// through the actor's dedicated cleanup slot.
///
/// The handle is `Send + Sync`: nonterminal operations may be awaited from
/// different executor threads, while the connection actor remains the sole
/// serialization and ownership authority.
#[must_use = "dropping an unfinished transaction schedules rollback"]
pub struct Transaction<'connection> {
    connection: &'connection AsyncConnection,
    token: TransactionToken,
    terminal: Arc<TransactionTerminalReceipt>,
    /// Monotonic cross-task publication that actor ownership has ended.
    finalized: AtomicBool,
}

impl Transaction<'_> {
    fn terminal_outcome(&self) -> Option<TransactionTerminalOutcome> {
        let outcome = self.terminal.outcome();
        if outcome.is_some() {
            self.finalized.store(true, Ordering::Release);
        }
        outcome
    }

    fn live_token(&self) -> Result<TransactionToken, FrankenError> {
        if let Some(outcome) = self.terminal_outcome() {
            return Err(outcome.nonterminal_error());
        }
        if self.finalized.load(Ordering::Acquire) {
            return Err(FrankenError::TransactionOutcomeUnknown {
                operation: "actor publication",
                detail: "transaction ownership ended without a terminal receipt".to_owned(),
            });
        }
        Ok(self.token)
    }

    fn resolved_commit(&self) -> Option<Result<(), FrankenError>> {
        self.terminal_outcome()
            .map(|outcome| outcome.commit_result())
    }

    fn resolved_rollback(&self) -> Option<Result<(), FrankenError>> {
        self.terminal_outcome()
            .map(|outcome| outcome.rollback_result())
    }

    fn finish_operation_response<T>(
        &self,
        response: TransactionOperationResponse<T>,
    ) -> Result<T, FrankenError> {
        if response.ownership_ended {
            assert!(
                self.terminal.outcome().is_some(),
                "the actor must publish a terminal receipt before its response"
            );
            self.finalized.store(true, Ordering::Release);
        }
        if let Some(outcome) = self.terminal_outcome() {
            Err(outcome.nonterminal_error())
        } else {
            response.result
        }
    }

    fn reconcile_operation_error(&self, error: FrankenError) -> FrankenError {
        self.terminal_outcome()
            .map_or(error, |outcome| outcome.nonterminal_error())
    }

    fn finish_operation_request<T>(
        &self,
        response: Result<TransactionOperationResponse<T>, FrankenError>,
    ) -> Result<T, FrankenError> {
        match response {
            Ok(response) => self.finish_operation_response(response),
            Err(error) => Err(self.reconcile_operation_error(error)),
        }
    }

    fn observe_terminal_response(
        &self,
        response: TransactionTerminalResponse,
    ) -> Result<(), FrankenError> {
        if response.ownership_ended {
            assert!(
                self.terminal.outcome().is_some(),
                "the actor must publish a terminal receipt before its response"
            );
            self.finalized.store(true, Ordering::Release);
        }
        response.result
    }

    fn finish_commit_response(
        &self,
        response: TransactionTerminalResponse,
    ) -> Result<(), FrankenError> {
        let delivered_result = self.observe_terminal_response(response);
        self.resolved_commit().unwrap_or(delivered_result)
    }

    fn finish_rollback_response(
        &self,
        response: TransactionTerminalResponse,
    ) -> Result<(), FrankenError> {
        let delivered_result = self.observe_terminal_response(response);
        self.resolved_rollback().unwrap_or(delivered_result)
    }

    /// Validate one statement without letting its prepared state outlive this
    /// transaction's ownership token.
    pub fn prepare_sync(&self, sql: &str) -> Result<(), FrankenError> {
        let token = self.live_token()?;
        let response = self.connection.sender().and_then(|sender| {
            sender.request_sync(|tx| Command::TransactionPrepare {
                token,
                sql: sql.to_owned(),
                tx,
            })
        });
        self.finish_operation_request(response)
    }

    /// Query within this transaction from a synchronous caller.
    pub fn query_sync(&self, sql: &str) -> Result<Vec<Row>, FrankenError> {
        self.query_with_params_sync(sql, &[])
    }

    /// Execute a parameterized query from a synchronous caller.
    pub fn query_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, FrankenError> {
        let token = self.live_token()?;
        let response = self.connection.sender().and_then(|sender| {
            sender.request_sync(|tx| Command::TransactionQuery {
                token,
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
        });
        self.finish_operation_request(response)
    }

    /// Stream a parameterized query while retaining this transaction's
    /// ownership token.
    ///
    /// Rows are bounded to one buffered item. If the callback stops the stream,
    /// the actor still publishes a separate terminal ownership response before
    /// this method returns.
    pub fn query_with_params_for_each_sync<F>(
        &self,
        sql: &str,
        params: &[SqliteValue],
        mut f: F,
    ) -> Result<(), FrankenError>
    where
        F: FnMut(&Row) -> Result<(), FrankenError>,
    {
        let token = self.live_token()?;
        let sender = self
            .connection
            .sender()
            .map_err(|error| self.reconcile_operation_error(error))?;
        let lifecycle = Arc::clone(&sender.lifecycle);
        let (row_tx, row_rx) = sync_mpsc::sync_channel(1);
        let (terminal_tx, terminal_rx) = response_channel(None);
        terminal_rx.mark_admitted();
        if let Err(error) = sender.send_stream_sync(Command::TransactionQueryWithParamsStream {
            token,
            sql: sql.to_owned(),
            params: params.to_vec(),
            row_tx,
            terminal_tx,
        }) {
            terminal_rx.mark_admission_failed();
            return Err(self.reconcile_operation_error(error));
        }

        let mut callback_error = None;
        loop {
            match row_rx.recv() {
                Ok(Some(row)) => {
                    let _guard = StreamCallbackGuard::enter(self.connection.connection_id);
                    if let Err(error) = f(&row) {
                        callback_error = Some(error);
                        break;
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        drop(row_rx);
        let response = recv_worker_response(terminal_rx, &lifecycle);
        let operation_result = self.finish_operation_request(response);
        if let Some(outcome) = self.terminal_outcome() {
            Err(outcome.nonterminal_error())
        } else if let Some(error) = callback_error {
            let _ = operation_result;
            Err(error)
        } else {
            operation_result
        }
    }

    /// Query exactly one row from a synchronous caller.
    pub fn query_row_sync(&self, sql: &str) -> Result<Row, FrankenError> {
        self.query_row_with_params_sync(sql, &[])
    }

    /// Execute a parameterized query returning exactly one row synchronously.
    pub fn query_row_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Row, FrankenError> {
        let token = self.live_token()?;
        let response = self.connection.sender().and_then(|sender| {
            sender.request_sync(|tx| Command::TransactionQueryRow {
                token,
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
        });
        self.finish_operation_request(response)
    }

    /// Execute SQL within this transaction from a synchronous caller.
    pub fn execute_sync(&self, sql: &str) -> Result<usize, FrankenError> {
        self.execute_with_params_sync(sql, &[])
    }

    /// Execute parameterized SQL synchronously within this transaction.
    pub fn execute_with_params_sync(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError> {
        let token = self.live_token()?;
        let response = self.connection.sender().and_then(|sender| {
            sender.request_sync(|tx| Command::TransactionExecute {
                token,
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
        });
        self.finish_operation_request(response)
    }

    /// Execute prevalidated parameter sets under this transaction's rollback
    /// boundary.
    pub fn execute_many_with_params_sync(
        &self,
        sql: &str,
        parameter_sets: &[Vec<SqliteValue>],
    ) -> Result<usize, FrankenError> {
        let token = self.live_token()?;
        let response = self.connection.sender().and_then(|sender| {
            sender.request_sync(|tx| Command::TransactionExecuteMany {
                token,
                sql: sql.to_owned(),
                parameter_sets: parameter_sets.to_vec(),
                tx,
            })
        });
        self.finish_operation_request(response)
    }

    /// Execute a semicolon-separated batch synchronously.
    pub fn execute_batch_sync(&self, sql: &str) -> Result<(), FrankenError> {
        let token = self.live_token()?;
        let response = self.connection.sender().and_then(|sender| {
            sender.request_sync(|tx| Command::TransactionExecuteBatch {
                token,
                sql: sql.to_owned(),
                tx,
            })
        });
        self.finish_operation_request(response)
    }

    /// Return the connection's last inserted row identifier while retaining
    /// this transaction's ownership fence.
    pub fn last_insert_rowid_sync(&self) -> Result<i64, FrankenError> {
        let token = self.live_token()?;
        let response = self.connection.sender().and_then(|sender| {
            sender.request_sync(|tx| Command::TransactionLastInsertRowid { token, tx })
        });
        self.finish_operation_request(response)
    }

    fn savepoint_command_sync(
        &self,
        action: TransactionSavepointAction,
        name: &str,
    ) -> Result<(), FrankenError> {
        let token = self.live_token()?;
        let response = self.connection.sender().and_then(|sender| {
            sender.request_sync(|tx| Command::TransactionSavepoint {
                token,
                action,
                name: name.to_owned(),
                tx,
            })
        });
        self.finish_operation_request(response)
    }

    /// Create a named savepoint synchronously.
    pub fn savepoint_sync(&self, name: &str) -> Result<(), FrankenError> {
        self.savepoint_command_sync(TransactionSavepointAction::Create, name)
    }

    /// Release a named savepoint synchronously.
    pub fn release_savepoint_sync(&self, name: &str) -> Result<(), FrankenError> {
        self.savepoint_command_sync(TransactionSavepointAction::Release, name)
    }

    /// Roll back to a named savepoint synchronously.
    pub fn rollback_to_savepoint_sync(&self, name: &str) -> Result<(), FrankenError> {
        self.savepoint_command_sync(TransactionSavepointAction::RollbackTo, name)
    }

    /// Commit this transaction from a synchronous caller.
    pub fn commit_sync(&mut self) -> Result<(), FrankenError> {
        if let Some(result) = self.resolved_commit() {
            return result;
        }
        let token = self.live_token()?;
        let sender = match self.connection.sender() {
            Ok(sender) => sender,
            Err(error) => return self.resolved_commit().unwrap_or(Err(error)),
        };
        let response = match sender.request_sync(|tx| Command::TransactionCommit { token, tx }) {
            Ok(response) => response,
            Err(error) => return self.resolved_commit().unwrap_or(Err(error)),
        };
        self.finish_commit_response(response)
    }

    /// Roll back this transaction from a synchronous caller.
    pub fn rollback_sync(&mut self) -> Result<(), FrankenError> {
        if let Some(result) = self.resolved_rollback() {
            return result;
        }
        let token = self.live_token()?;
        let sender = match self.connection.sender() {
            Ok(sender) => sender,
            Err(error) => return self.resolved_rollback().unwrap_or(Err(error)),
        };
        let response = match sender.request_sync(|tx| Command::TransactionRollback { token, tx }) {
            Ok(response) => response,
            Err(error) => return self.resolved_rollback().unwrap_or(Err(error)),
        };
        self.finish_rollback_response(response)
    }

    /// Validate one statement while retaining this transaction's ownership
    /// token.
    pub async fn prepare<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let token = self.live_token()?;
        let response = self
            .connection
            .request_async(cx, |tx| Command::TransactionPrepare {
                token,
                sql: sql.to_owned(),
                tx,
            })
            .await;
        self.finish_operation_request(response)
    }

    /// Query within this transaction.
    pub async fn query<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<Vec<Row>, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.query_with_params(cx, sql, &[]).await
    }

    /// Execute a parameterized query within this transaction.
    pub async fn query_with_params<Caps>(
        &self,
        cx: &Cx<Caps>,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let token = self.live_token()?;
        let response = self
            .connection
            .request_async(cx, |tx| Command::TransactionQuery {
                token,
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
            .await;
        self.finish_operation_request(response)
    }

    /// Execute prevalidated parameter sets under this transaction's rollback
    /// boundary.
    pub async fn execute_many_with_params<Caps>(
        &self,
        cx: &Cx<Caps>,
        sql: &str,
        parameter_sets: &[Vec<SqliteValue>],
    ) -> Result<usize, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let token = self.live_token()?;
        let response = self
            .connection
            .request_async(cx, |tx| Command::TransactionExecuteMany {
                token,
                sql: sql.to_owned(),
                parameter_sets: parameter_sets.to_vec(),
                tx,
            })
            .await;
        self.finish_operation_request(response)
    }

    /// Query exactly one row within this transaction.
    pub async fn query_row<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<Row, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.query_row_with_params(cx, sql, &[]).await
    }

    /// Execute a parameterized query returning exactly one row.
    pub async fn query_row_with_params<Caps>(
        &self,
        cx: &Cx<Caps>,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Row, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let token = self.live_token()?;
        let response = self
            .connection
            .request_async(cx, |tx| Command::TransactionQueryRow {
                token,
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
            .await;
        self.finish_operation_request(response)
    }

    /// Execute SQL within this transaction.
    pub async fn execute<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<usize, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.execute_with_params(cx, sql, &[]).await
    }

    /// Execute parameterized SQL within this transaction.
    pub async fn execute_with_params<Caps>(
        &self,
        cx: &Cx<Caps>,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let token = self.live_token()?;
        let response = self
            .connection
            .request_async(cx, |tx| Command::TransactionExecute {
                token,
                sql: sql.to_owned(),
                params: params.to_vec(),
                tx,
            })
            .await;
        self.finish_operation_request(response)
    }

    /// Execute a semicolon-separated batch within this transaction.
    pub async fn execute_batch<Caps>(&self, cx: &Cx<Caps>, sql: &str) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let token = self.live_token()?;
        let response = self
            .connection
            .request_async(cx, |tx| Command::TransactionExecuteBatch {
                token,
                sql: sql.to_owned(),
                tx,
            })
            .await;
        self.finish_operation_request(response)
    }

    /// Return the connection's last inserted row identifier while retaining
    /// this transaction's ownership fence.
    pub async fn last_insert_rowid<Caps>(&self, cx: &Cx<Caps>) -> Result<i64, FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let token = self.live_token()?;
        let response = self
            .connection
            .request_async(cx, |tx| Command::TransactionLastInsertRowid { token, tx })
            .await;
        self.finish_operation_request(response)
    }

    async fn savepoint_command<Caps>(
        &self,
        cx: &Cx<Caps>,
        action: TransactionSavepointAction,
        name: &str,
    ) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        let token = self.live_token()?;
        let response = self
            .connection
            .request_async(cx, |tx| Command::TransactionSavepoint {
                token,
                action,
                name: name.to_owned(),
                tx,
            })
            .await;
        self.finish_operation_request(response)
    }

    /// Create a named savepoint within this transaction.
    pub async fn savepoint<Caps>(&self, cx: &Cx<Caps>, name: &str) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.savepoint_command(cx, TransactionSavepointAction::Create, name)
            .await
    }

    /// Release a named savepoint.
    pub async fn release_savepoint<Caps>(
        &self,
        cx: &Cx<Caps>,
        name: &str,
    ) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.savepoint_command(cx, TransactionSavepointAction::Release, name)
            .await
    }

    /// Roll back to a named savepoint without ending the outer transaction.
    pub async fn rollback_to_savepoint<Caps>(
        &self,
        cx: &Cx<Caps>,
        name: &str,
    ) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        self.savepoint_command(cx, TransactionSavepointAction::RollbackTo, name)
            .await
    }

    /// Commit this transaction.
    pub async fn commit<Caps>(&mut self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        if let Some(result) = self.resolved_commit() {
            return result;
        }
        let token = self.live_token()?;
        let response = match self
            .connection
            .request_async(cx, |tx| Command::TransactionCommit { token, tx })
            .await
        {
            Ok(response) => response,
            Err(error) => return self.resolved_commit().unwrap_or(Err(error)),
        };
        self.finish_commit_response(response)
    }

    /// Roll back this transaction.
    pub async fn rollback<Caps>(&mut self, cx: &Cx<Caps>) -> Result<(), FrankenError>
    where
        Caps: fsqlite_types::cx::cap::SubsetOf<fsqlite_types::cx::cap::All>,
    {
        if let Some(result) = self.resolved_rollback() {
            return result;
        }
        let token = self.live_token()?;
        let response = match self
            .connection
            .request_async(cx, |tx| Command::TransactionRollback { token, tx })
            .await
        {
            Ok(response) => response,
            Err(error) => return self.resolved_rollback().unwrap_or(Err(error)),
        };
        self.finish_rollback_response(response)
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if self.terminal.outcome().is_none() && !self.finalized.load(Ordering::Acquire) {
            self.connection.transaction_cleanup.request(self.token);
        }
    }
}

impl Drop for AsyncConnection {
    fn drop(&mut self) {
        // Drop never blocks and never discards an admitted effect. Disconnect
        // the mailbox; the worker drains queued commands and explicitly closes
        // its Connection before its detached thread exits.
        drop(self.cmd_tx.take());
        drop(self.close_response.take());
        drop(self.worker.take());
        self.state = AsyncConnectionState::Closed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RuntimeConfig, RuntimeContext};
    use asupersync::runtime::{Runtime, RuntimeBuilder};
    use fsqlite_types::cx::Cx;
    use std::task::Wake;

    #[derive(Default)]
    struct WakeCounter {
        wake_count: AtomicUsize,
    }

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::AcqRel);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn test_runtime() -> Runtime {
        RuntimeBuilder::current_thread()
            .blocking_threads(2, 2)
            .build()
            .expect("test runtime should build")
    }

    #[test]
    fn dropped_worker_exit_removes_its_registered_waker() {
        let lifecycle = WorkerLifecycle::new(Arc::new(CommandCapacitySignal::new()));
        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&wake_counter));
        let mut task_cx = Context::from_waker(&waker);

        {
            let mut exit = Box::pin(WorkerExit::new(&lifecycle));
            assert!(exit.as_mut().poll(&mut task_cx).is_pending());
            assert_eq!(
                lock_unpoisoned(&lifecycle.waiters).len(),
                1,
                "a pending lifecycle wait must register exactly one waker"
            );
        }

        assert!(
            lock_unpoisoned(&lifecycle.waiters).is_empty(),
            "dropping the lifecycle wait must remove its stale task waker"
        );
    }

    #[test]
    fn worker_exit_repoll_replaces_its_waker_without_growing_waiters() {
        let lifecycle = WorkerLifecycle::new(Arc::new(CommandCapacitySignal::new()));
        let first_counter = Arc::new(WakeCounter::default());
        let first_waker = Waker::from(Arc::clone(&first_counter));
        let mut first_task_cx = Context::from_waker(&first_waker);
        let mut exit = Box::pin(WorkerExit::new(&lifecycle));

        assert!(exit.as_mut().poll(&mut first_task_cx).is_pending());
        let waiter_id = exit
            .as_ref()
            .get_ref()
            .waiter_id
            .expect("the first poll must allocate a stable waiter identity");

        let second_counter = Arc::new(WakeCounter::default());
        let second_waker = Waker::from(Arc::clone(&second_counter));
        let mut second_task_cx = Context::from_waker(&second_waker);
        assert!(exit.as_mut().poll(&mut second_task_cx).is_pending());

        {
            let waiters = lock_unpoisoned(&lifecycle.waiters);
            assert_eq!(
                waiters.len(),
                1,
                "repolling one future must replace rather than append its waker"
            );
            let (registered_id, registered_waker) = &waiters[0];
            assert_eq!(*registered_id, waiter_id);
            assert!(registered_waker.will_wake(&second_waker));
            assert!(!registered_waker.will_wake(&first_waker));
        }

        drop(exit);
        assert!(
            lock_unpoisoned(&lifecycle.waiters).is_empty(),
            "replacement registration must retain the same Drop cleanup obligation"
        );
    }

    #[test]
    fn worker_exit_waiter_ids_skip_live_collision_across_wrap() {
        let lifecycle = WorkerLifecycle::new(Arc::new(CommandCapacitySignal::new()));
        lifecycle.next_waiter_id.store(u64::MAX, Ordering::Relaxed);

        let first_counter = Arc::new(WakeCounter::default());
        let first_waker = Waker::from(Arc::clone(&first_counter));
        let mut first_task_cx = Context::from_waker(&first_waker);
        let mut first_exit = Box::pin(WorkerExit::new(&lifecycle));
        assert!(first_exit.as_mut().poll(&mut first_task_cx).is_pending());
        let first_id = first_exit
            .as_ref()
            .get_ref()
            .waiter_id
            .expect("first near-wrap waiter must register");
        assert_eq!(first_id, u64::MAX);

        // Force the allocator's next candidate back onto the live near-wrap
        // identity. Collision-safe selection must wrap and skip it.
        lifecycle.next_waiter_id.store(u64::MAX, Ordering::Relaxed);
        let second_counter = Arc::new(WakeCounter::default());
        let second_waker = Waker::from(Arc::clone(&second_counter));
        let mut second_task_cx = Context::from_waker(&second_waker);
        let mut second_exit = Box::pin(WorkerExit::new(&lifecycle));
        assert!(second_exit.as_mut().poll(&mut second_task_cx).is_pending());
        let second_id = second_exit
            .as_ref()
            .get_ref()
            .waiter_id
            .expect("second near-wrap waiter must register");
        assert_eq!(second_id, 0);
        assert_ne!(first_id, second_id);
        assert_eq!(
            lock_unpoisoned(&lifecycle.waiters).len(),
            2,
            "both collision-tested waits must remain independently registered"
        );

        drop(first_exit);
        {
            let waiters = lock_unpoisoned(&lifecycle.waiters);
            assert_eq!(
                waiters.len(),
                1,
                "dropping one colliding wait must remove only its own registration"
            );
            assert_eq!(waiters[0].0, second_id);
        }
        assert_eq!(first_counter.wake_count.load(Ordering::Acquire), 0);
        assert_eq!(second_counter.wake_count.load(Ordering::Acquire), 0);

        lifecycle.publish_closed();
        lifecycle.finish();
        assert_eq!(
            first_counter.wake_count.load(Ordering::Acquire),
            0,
            "the dropped wait must never be spuriously woken"
        );
        assert_eq!(
            second_counter.wake_count.load(Ordering::Acquire),
            1,
            "the surviving wait must be woken exactly once"
        );
        assert!(lock_unpoisoned(&lifecycle.waiters).is_empty());
        assert!(second_exit.as_mut().poll(&mut second_task_cx).is_ready());
        drop(second_exit);
        assert!(lock_unpoisoned(&lifecycle.waiters).is_empty());
    }

    #[test]
    fn transaction_control_classifier_is_lexical_then_ast_exact() {
        for harmless in [
            "SELECT 'BEGIN', 1 AS \"COMMIT\", 2 AS `ROLLBACK`, 3 AS [SAVEPOINT]",
            "-- RELEASE\nSELECT 1",
            "/* BEGIN; COMMIT; */ SELECT 1",
        ] {
            assert!(
                !sql_might_contain_transaction_control(harmless),
                "quoted/comment-only keywords should stay on the prefilter fast path: {harmless}"
            );
            validate_no_raw_transaction_control(harmless)
                .expect("quoted and commented keywords are not transaction control");
        }

        let trigger = "CREATE TRIGGER audit_insert AFTER INSERT ON source \
                       BEGIN INSERT INTO audit VALUES (new.id); END";
        assert!(
            sql_might_contain_transaction_control(trigger),
            "trigger bodies should take the exact AST path"
        );
        validate_no_raw_transaction_control(trigger)
            .expect("BEGIN/END inside a trigger body is not top-level transaction control");

        for control in [
            "CREATE TRIGGER bad AFTER INSERT ON source BEGIN COMMIT; END",
            "CREATE TRIGGER bad AFTER INSERT ON source BEGIN ROLLBACK; END",
            "CREATE TRIGGER bad AFTER INSERT ON source BEGIN EXPLAIN COMMIT; END",
        ] {
            assert!(
                matches!(
                    validate_no_raw_transaction_control(control),
                    Err(FrankenError::Busy)
                ),
                "transaction control nested in a trigger body must be rejected: {control}"
            );
        }

        for control in [
            "BEGIN",
            "COMMIT",
            "END TRANSACTION",
            "ROLLBACK",
            "SAVEPOINT hidden",
            "RELEASE hidden",
            "EXPLAIN BEGIN",
            "EXPLAIN QUERY PLAN COMMIT",
            "SELECT 1; SAVEPOINT hidden; SELECT 2",
        ] {
            assert!(
                matches!(
                    validate_no_raw_transaction_control(control),
                    Err(FrankenError::Busy)
                ),
                "raw transaction control must be rejected: {control}"
            );
        }

        assert!(matches!(
            validate_no_raw_transaction_control(
                "INSERT INTO t VALUES (1); BEGIN IMMEDIATE trailing_garbage"
            ),
            Err(FrankenError::ParseError { .. })
        ));
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
            let mut transaction = conn.begin_transaction(&cx).await.expect("begin");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert");
            transaction.rollback(&cx).await.expect("rollback");

            let rows = conn.query(&cx, "SELECT * FROM t").await.expect("query");
            assert!(rows.is_empty(), "rollback should have removed the row");

            // Begin, insert, commit — row should persist.
            let mut transaction = conn.begin_transaction(&cx).await.expect("begin");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (2)")
                .await
                .expect("insert");
            transaction.commit(&cx).await.expect("commit");

            let rows = conn.query(&cx, "SELECT * FROM t").await.expect("query");
            assert_eq!(rows.len(), 1);
        });
    }

    #[test]
    fn actor_owned_transaction_isolates_commands_and_rejects_ordinary_mail() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");

            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("begin owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("token-scoped insert should succeed");
            assert!(matches!(
                conn.query(&cx, "SELECT * FROM t").await,
                Err(FrankenError::Busy)
            ));
            assert!(matches!(
                conn.execute(&cx, "INSERT INTO t VALUES (2)").await,
                Err(FrankenError::Busy)
            ));
            assert_eq!(
                transaction
                    .query(&cx, "SELECT * FROM t")
                    .await
                    .expect("transaction query should see its write")
                    .len(),
                1
            );

            transaction
                .rollback(&cx)
                .await
                .expect("owned rollback should succeed");
            assert!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("ordinary query should resume after rollback")
                    .is_empty()
            );
        });
    }

    #[test]
    fn actor_owned_transaction_savepoints_are_token_scoped() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");

            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("begin owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("prefix insert should succeed");
            transaction
                .savepoint(&cx, "quoted \" savepoint")
                .await
                .expect("savepoint should succeed");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (2)")
                .await
                .expect("savepoint insert should succeed");
            transaction
                .rollback_to_savepoint(&cx, "quoted \" savepoint")
                .await
                .expect("rollback to savepoint should succeed");
            transaction
                .release_savepoint(&cx, "quoted \" savepoint")
                .await
                .expect("release should succeed");
            transaction
                .commit(&cx)
                .await
                .expect("owned commit should succeed");

            let rows = conn
                .query(&cx, "SELECT id FROM t ORDER BY id")
                .await
                .expect("committed query should succeed");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
        });
    }

    #[test]
    fn ordinary_and_token_scoped_sql_reject_all_raw_transaction_control() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_batch_sync(
            "CREATE TABLE t (id INTEGER PRIMARY KEY);
             CREATE TABLE source (id INTEGER PRIMARY KEY);
             CREATE TABLE audit (id INTEGER);
             CREATE TRIGGER audit_insert AFTER INSERT ON source
             BEGIN
                 INSERT INTO audit VALUES (new.id);
             END;",
        )
        .expect("trigger-body BEGIN/END must remain legal");

        assert!(matches!(conn.query_sync("BEGIN"), Err(FrankenError::Busy)));
        assert!(matches!(
            conn.query_row_sync("EXPLAIN COMMIT"),
            Err(FrankenError::Busy)
        ));
        assert!(matches!(
            conn.execute_sync("ROLLBACK"),
            Err(FrankenError::Busy)
        ));
        assert!(matches!(
            conn.execute_batch_sync("INSERT INTO t VALUES (1); SAVEPOINT hidden; SELECT 1"),
            Err(FrankenError::Busy)
        ));
        assert!(
            conn.query_sync("SELECT * FROM t")
                .expect("rejected mixed batch must leave the table queryable")
                .is_empty(),
            "the classifier must reject the whole mixed batch before its first effect"
        );
        assert!(matches!(
            conn.execute_batch_sync("INSERT INTO t VALUES (1); BEGIN IMMEDIATE trailing_garbage"),
            Err(FrankenError::ParseError { .. })
        ));
        assert!(
            conn.query_sync("SELECT * FROM t")
                .expect("parse-rejected mixed batch must leave the table queryable")
                .is_empty(),
            "candidate parse errors must be returned before the first batch effect"
        );
        assert!(matches!(
            conn.execute_batch_sync(
                "INSERT INTO source VALUES (99);
                 CREATE TRIGGER illegal_control AFTER INSERT ON source
                 BEGIN
                     EXPLAIN COMMIT;
                 END;"
            ),
            Err(FrankenError::Busy)
        ));
        assert!(
            conn.query_sync("SELECT id FROM source WHERE id = 99")
                .expect("trigger-control rejection must leave source queryable")
                .is_empty(),
            "a trigger-body control must reject the whole batch before its first effect"
        );
        assert!(
            conn.query_sync(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name = 'illegal_control'"
            )
            .expect("schema should remain queryable")
            .is_empty(),
            "the rejected trigger must not be persisted"
        );

        let mut transaction = conn
            .begin_transaction_sync()
            .expect("owned transaction should begin");
        assert!(matches!(
            transaction.query_sync("BEGIN"),
            Err(FrankenError::Busy)
        ));
        assert!(matches!(
            transaction.query_row_sync("EXPLAIN ROLLBACK"),
            Err(FrankenError::Busy)
        ));
        assert!(matches!(
            transaction.execute_sync("COMMIT"),
            Err(FrankenError::Busy)
        ));
        assert!(matches!(
            transaction.execute_many_with_params_sync("SAVEPOINT hidden", &[Vec::new()]),
            Err(FrankenError::Busy)
        ));
        assert!(matches!(
            transaction.execute_batch_sync("INSERT INTO t VALUES (2); RELEASE hidden; SELECT 1"),
            Err(FrankenError::Busy)
        ));
        assert!(
            transaction
                .query_sync("SELECT * FROM t")
                .expect("owned transaction should remain usable")
                .is_empty(),
            "token-scoped mixed control must be rejected before its first effect"
        );
        transaction
            .execute_sync("INSERT INTO source VALUES (7)")
            .expect("a trigger body containing BEGIN/END must execute normally");
        assert_eq!(
            transaction
                .query_sync("SELECT id FROM audit")
                .expect("trigger effect should be visible inside the transaction")
                .len(),
            1
        );
        transaction
            .rollback_sync()
            .expect("owned transaction should roll back");
        drop(transaction);
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn live_transaction_owner_fence_precedes_raw_control_classification() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_sync("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .expect("schema should succeed");
        let mut transaction = conn
            .begin_transaction_sync()
            .expect("owned transaction should begin");
        let malformed_control = "INSERT INTO t VALUES (1); BEGIN IMMEDIATE trailing_garbage";

        assert!(
            matches!(
                conn.execute_batch_sync(malformed_control),
                Err(FrankenError::Busy)
            ),
            "the live owner fence must win before parsing ordinary SQL"
        );
        assert!(
            transaction
                .query_sync("SELECT * FROM t")
                .expect("the rejected ordinary batch must have no effect")
                .is_empty()
        );

        transaction
            .rollback_sync()
            .expect("owned transaction should roll back");
        assert!(
            matches!(
                conn.execute_batch_sync(malformed_control),
                Err(FrankenError::ParseError { .. })
            ),
            "without a live owner, exact SQL classification should remain authoritative"
        );
        drop(transaction);
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn actor_rejects_stale_and_foreign_transaction_tokens() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("first connection should open");
            let other = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("second connection should open");

            let mut first = conn.begin_transaction(&cx).await.expect("first generation");
            let stale = first.token;
            first.rollback(&cx).await.expect("first rollback");
            let mut current = conn
                .begin_transaction(&cx)
                .await
                .expect("second generation");
            let mut foreign = other
                .begin_transaction(&cx)
                .await
                .expect("foreign generation");

            assert!(matches!(
                conn.request_async(&cx, |tx| Command::TransactionQuery {
                    token: stale,
                    sql: "SELECT 1".to_owned(),
                    params: Vec::new(),
                    tx,
                })
                .await,
                Err(FrankenError::Busy)
            ));
            assert!(matches!(
                conn.request_async(&cx, |tx| Command::TransactionQuery {
                    token: foreign.token,
                    sql: "SELECT 1".to_owned(),
                    params: Vec::new(),
                    tx,
                })
                .await,
                Err(FrankenError::Busy)
            ));

            current.rollback(&cx).await.expect("current rollback");
            foreign.rollback(&cx).await.expect("foreign rollback");
        });
    }

    #[test]
    fn transaction_drop_cleanup_is_coalesced_and_idempotent() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(
                &conn
                    .worker
                    .as_ref()
                    .expect("worker should be live")
                    .lifecycle,
            );
            let transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");
            let token = transaction.token;

            let (entered_tx, entered_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            let (blocked_response, blocked_cancellation, blocked_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::TestBlockActor {
                    entered: entered_tx,
                    release: release_rx,
                    tx,
                })
                .await
                .expect("blocking command should be admitted");
            entered_rx.await.expect("actor should enter blocker");

            conn.transaction_cleanup.request(token);
            conn.transaction_cleanup.request(token);
            drop(transaction);
            release_tx.send(()).expect("actor should retain blocker");
            recv_authoritative_worker_response(
                &cx,
                blocked_response,
                blocked_cancellation,
                blocked_native_cx,
                &conn.lifecycle,
            )
            .await
            .expect("blocking command should finish");

            assert!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("post-drop query should succeed")
                    .is_empty()
            );
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                1,
                "coalesced duplicate cleanup requests must roll back once"
            );
        });
    }

    #[test]
    fn transaction_drop_cleanup_self_wakes_exactly_one_retry() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(
                &conn
                    .worker
                    .as_ref()
                    .expect("worker should be live")
                    .lifecycle,
            );
            lifecycle
                .drop_rollback_retryable_failures
                .store(1, Ordering::Release);

            let transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");
            drop(transaction);

            for _ in 0..10_000 {
                if lifecycle.drop_rollback_calls.load(Ordering::Acquire) == 2
                    && !conn.in_transaction()
                {
                    break;
                }
                future::yield_now().await;
            }
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                2,
                "a retryable first failure must publish one new cleanup wake"
            );
            assert!(
                !conn.in_transaction(),
                "the self-woken retry must finish rollback without ordinary mailbox traffic"
            );
            assert!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("connection should remain usable after retry")
                    .is_empty()
            );
        });
    }

    #[test]
    fn transaction_drop_cleanup_poison_is_persistent_and_does_not_spin() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = Arc::clone(
                &conn
                    .worker
                    .as_ref()
                    .expect("worker should be live")
                    .lifecycle,
            );
            lifecycle
                .drop_rollback_poison_failures
                .store(1, Ordering::Release);

            let transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            drop(transaction);

            for _ in 0..10_000 {
                if conn.transaction_cleanup.poisoned_token().is_some() {
                    break;
                }
                future::yield_now().await;
            }
            assert!(
                conn.transaction_cleanup.poisoned_token().is_some(),
                "a non-retryable cleanup failure must remain explicitly poisoned"
            );
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                1,
                "poison must stop automatic retry"
            );
            for _ in 0..100 {
                future::yield_now().await;
            }
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                1,
                "a poisoned cleanup obligation must not spin the worker"
            );
            assert!(matches!(
                conn.query(&cx, "SELECT 1").await,
                Err(FrankenError::Internal(detail)) if detail.contains("cleanup")
            ));

            conn.close(&cx)
                .await
                .expect("explicit close should use the connection's retained exact cleanup owner");
        });
    }

    #[test]
    fn terminal_error_after_owner_release_finalizes_the_handle() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(
                &conn
                    .worker
                    .as_ref()
                    .expect("worker should be live")
                    .lifecycle,
            );
            lifecycle
                .terminal_post_effect_failures
                .store(1, Ordering::Release);

            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");
            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "commit",
                    detail,
                }) if detail.contains("after transaction end")
            ));
            assert!(
                transaction.finalized.load(Ordering::Acquire),
                "the terminal response must report that token ownership ended despite the error"
            );
            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "commit",
                    ..
                })
            ));
            assert!(matches!(
                transaction.rollback(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "commit",
                    ..
                })
            ));
            drop(transaction);

            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                0,
                "dropping a finalized error-returning handle must not schedule rollback"
            );
            assert_eq!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("committed row should remain queryable")
                    .len(),
                1
            );
        });
    }

    #[test]
    fn active_commit_error_preserves_original_error_and_generation() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            lifecycle
                .terminal_pre_effect_failures
                .store(1, Ordering::Release);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");

            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::Internal(detail))
                    if detail.contains("transaction remains active")
            ));
            assert!(conn.in_transaction());
            assert!(transaction.terminal.outcome().is_none());
            assert!(!transaction.finalized.load(Ordering::Acquire));

            transaction
                .commit(&cx)
                .await
                .expect("the same active generation must remain safely retryable");
            assert_eq!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("committed row should remain queryable")
                    .len(),
                1
            );
        });
    }

    #[test]
    fn rollback_receipt_normalizes_first_post_effect_failure_and_retry() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            lifecycle
                .terminal_post_effect_failures
                .store(1, Ordering::Release);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");

            transaction
                .rollback(&cx)
                .await
                .expect("the first response must preserve the proven rollback");
            assert!(matches!(
                transaction.terminal.outcome(),
                Some(TransactionTerminalOutcome::RolledBack { reason })
                    if reason.contains("injected terminal error after transaction end")
            ));
            transaction
                .rollback(&cx)
                .await
                .expect("rollback retry must replay the same proof");
            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionRolledBack { .. })
            ));
            assert!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("connection should remain usable")
                    .is_empty()
            );
        });
    }

    #[test]
    fn delivered_commit_and_rollback_errors_are_overridden_by_exact_receipts() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);

            let mut committed = conn
                .begin_transaction(&cx)
                .await
                .expect("commit transaction should begin");
            committed
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("committed insert should succeed");
            lifecycle
                .terminal_delivered_response_failures
                .store(1, Ordering::Release);
            committed
                .commit(&cx)
                .await
                .expect("the committed receipt must override its delivered response error");

            let mut rolled_back = conn
                .begin_transaction(&cx)
                .await
                .expect("rollback transaction should begin");
            rolled_back
                .execute(&cx, "INSERT INTO t VALUES (2)")
                .await
                .expect("rolled-back insert should succeed");
            lifecycle
                .terminal_delivered_response_failures
                .store(1, Ordering::Release);
            rolled_back
                .rollback(&cx)
                .await
                .expect("the rollback receipt must override its delivered response error");

            let rows = conn
                .query(&cx, "SELECT id FROM t ORDER BY id")
                .await
                .expect("connection should remain usable");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
        });
    }

    #[test]
    fn dropped_admitted_commit_recovers_proven_commit_before_response_publication() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");
            let (terminal_tx, terminal_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            lifecycle.install_transaction_terminal_publication_pause(terminal_tx, release_rx);

            let mut commit = Box::pin(transaction.commit(&cx));
            assert!(
                future::poll_once(commit.as_mut()).await.is_none(),
                "commit must remain pending after mailbox admission"
            );
            terminal_rx
                .await
                .expect("actor must publish the generation receipt before pausing");
            drop(commit);
            assert!(matches!(
                transaction.terminal.outcome(),
                Some(TransactionTerminalOutcome::Committed)
            ));

            transaction
                .commit(&cx)
                .await
                .expect("retry must return the retained commit proof without re-execution");
            assert!(matches!(
                transaction.rollback(&cx).await,
                Err(FrankenError::NoActiveTransaction)
            ));

            release_tx
                .send(())
                .expect("actor must retain the abandoned response publication");
            assert_eq!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("connection should remain usable")
                    .len(),
                1
            );
            drop(transaction);
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                0,
                "a proven committed generation must never schedule stale rollback"
            );
        });
    }

    #[test]
    fn proven_commit_publication_wins_over_later_cancellation() {
        test_runtime().block_on(async {
            let setup_cx = Cx::new();
            let conn = AsyncConnection::open(&setup_cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&setup_cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let mut transaction = conn
                .begin_transaction(&setup_cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&setup_cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");
            let (terminal_tx, terminal_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            conn.lifecycle
                .install_transaction_terminal_publication_pause(terminal_tx, release_rx);

            let commit_cx = Cx::new();
            let mut commit = Box::pin(transaction.commit(&commit_cx));
            assert!(
                future::poll_once(commit.as_mut()).await.is_none(),
                "commit must remain pending after admission"
            );
            terminal_rx
                .await
                .expect("the commit proof must exist before response publication");
            commit_cx.cancel();
            assert!(
                future::poll_once(commit.as_mut()).await.is_none(),
                "late cancellation may relay but must not fabricate a terminal result"
            );

            release_tx
                .send(())
                .expect("actor must retain the terminal response");
            commit
                .await
                .expect("the proven commit must win over later cancellation");
            drop(transaction);
            assert_eq!(
                conn.query(&setup_cx, "SELECT * FROM t")
                    .await
                    .expect("committed row should remain queryable")
                    .len(),
                1
            );
        });
    }

    #[test]
    fn published_commit_receipt_survives_worker_panic_before_response() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");
            let (terminal_tx, terminal_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            lifecycle.install_transaction_terminal_publication_pause(terminal_tx, release_rx);

            let mut commit = Box::pin(transaction.commit(&cx));
            assert!(
                future::poll_once(commit.as_mut()).await.is_none(),
                "commit must remain pending after admission"
            );
            terminal_rx
                .await
                .expect("the published commit receipt must precede response publication");
            drop(release_tx);
            commit
                .await
                .expect("a proven commit receipt must survive worker failure before response");
            assert!(matches!(
                transaction.rollback(&cx).await,
                Err(FrankenError::NoActiveTransaction)
            ));
            WorkerExit::new(&lifecycle).await;
            drop(transaction);
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                0,
                "a proven commit receipt must suppress stale Drop cleanup"
            );
        });
    }

    #[test]
    fn dropped_admitted_rollback_recovers_proven_rollback_and_rejects_commit() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");
            let (terminal_tx, terminal_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            lifecycle.install_transaction_terminal_publication_pause(terminal_tx, release_rx);

            let mut rollback = Box::pin(transaction.rollback(&cx));
            assert!(
                future::poll_once(rollback.as_mut()).await.is_none(),
                "rollback must remain pending after mailbox admission"
            );
            terminal_rx
                .await
                .expect("actor must publish rollback before pausing");
            drop(rollback);

            transaction
                .rollback(&cx)
                .await
                .expect("rollback retry must consume the retained proof");
            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionRolledBack { .. })
            ));

            release_tx
                .send(())
                .expect("actor must retain the abandoned response publication");
            assert!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("connection should remain usable")
                    .is_empty()
            );
            drop(transaction);
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                0,
                "a proven rolled-back generation must never schedule stale cleanup"
            );
        });
    }

    #[test]
    fn normal_async_or_rollback_returns_exact_terminal_receipt() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("first insert should succeed");

            assert!(matches!(
                transaction
                    .execute(&cx, "INSERT OR ROLLBACK INTO t VALUES (1)")
                    .await,
                Err(FrankenError::TransactionRolledBack { .. })
            ));
            transaction
                .rollback(&cx)
                .await
                .expect("rollback retry must acknowledge the exact receipt");
            assert!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("connection should remain usable")
                    .is_empty()
            );
        });
    }

    #[test]
    fn normal_sync_or_rollback_returns_exact_terminal_receipt() {
        let conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_sync("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .expect("schema should succeed");
        let mut transaction = conn.begin_transaction_sync().expect("owned transaction");
        transaction
            .execute_sync("INSERT INTO t VALUES (1)")
            .expect("first insert should succeed");

        assert!(matches!(
            transaction.execute_sync("INSERT OR ROLLBACK INTO t VALUES (1)"),
            Err(FrankenError::TransactionRolledBack { .. })
        ));
        transaction
            .rollback_sync()
            .expect("rollback retry must acknowledge the exact receipt");
        assert!(
            conn.query_sync("SELECT * FROM t")
                .expect("connection should remain usable")
                .is_empty()
        );
    }

    #[test]
    fn ready_async_begin_response_is_overridden_by_later_terminal_receipt() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            let (ready_response, cancellation, polling_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::BeginOwnedTransaction {
                    connection_id: conn.connection_id,
                    tx,
                })
                .await
                .expect("owned begin should be admitted");

            let mut response_is_ready = false;
            for _ in 0..10_000 {
                response_is_ready = matches!(
                    &*lock_unpoisoned(&ready_response.control.status),
                    ResponseStatus::Ready(_)
                );
                if response_is_ready {
                    break;
                }
                future::yield_now().await;
            }
            assert!(
                response_is_ready,
                "the begin response must be ready before the worker failure"
            );

            conn.request_async(&cx, |tx| Command::TestPanicActor { tx })
                .await
                .expect_err("the later actor panic must terminate the worker");
            let receipt = recv_authoritative_worker_response(
                &cx,
                ready_response,
                cancellation,
                polling_native_cx,
                &lifecycle,
            )
            .await
            .expect("the already-ready begin response must remain consumable");
            assert!(matches!(
                receipt.terminal.outcome(),
                Some(TransactionTerminalOutcome::RolledBack { .. })
            ));
            assert!(matches!(
                conn.transaction_from_receipt(receipt),
                Err(FrankenError::TransactionRolledBack { .. })
            ));
            assert!(!conn.in_transaction());
            assert_eq!(
                *lock_unpoisoned(&conn.transaction_cleanup.state),
                TransactionDropCleanupState::Idle
            );
        });
    }

    #[test]
    fn ready_sync_begin_response_is_overridden_by_later_terminal_receipt() {
        let conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        let lifecycle = Arc::clone(&conn.lifecycle);
        let sender = conn.sender().expect("actor should be open");
        let (response_tx, ready_response) = response_channel(None);
        let permit = sender
            .command_capacity
            .reserve_blocking(sender.tx().expect("worker command sender should exist"))
            .expect("owned begin should reserve mailbox capacity");
        ready_response.mark_admitted();
        assert!(
            permit
                .try_send(CommandEnvelope {
                    cancellation: None,
                    command: Command::BeginOwnedTransaction {
                        connection_id: conn.connection_id,
                        tx: response_tx,
                    },
                })
                .is_ok(),
            "owned begin should be admitted"
        );

        {
            let status = lock_unpoisoned(&ready_response.control.status);
            let status = ready_response
                .control
                .ready
                .wait_while(status, |status| {
                    matches!(status, ResponseStatus::Pending(_))
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(
                matches!(&*status, ResponseStatus::Ready(_)),
                "the begin response must be ready before the worker failure"
            );
        }

        sender
            .request_sync(|tx| Command::TestPanicActor { tx })
            .expect_err("the later actor panic must terminate the worker");
        let receipt = recv_worker_response(ready_response, &lifecycle)
            .expect("the already-ready begin response must remain consumable");
        assert!(matches!(
            receipt.terminal.outcome(),
            Some(TransactionTerminalOutcome::RolledBack { .. })
        ));
        assert!(matches!(
            conn.transaction_from_receipt(receipt),
            Err(FrankenError::TransactionRolledBack { .. })
        ));
        assert!(!conn.in_transaction());
        assert_eq!(
            *lock_unpoisoned(&conn.transaction_cleanup.state),
            TransactionDropCleanupState::Idle
        );
    }

    #[test]
    fn ready_operation_response_is_overridden_by_later_terminal_receipt() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            let (ready_response, cancellation, polling_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::TransactionQuery {
                    token: transaction.token,
                    sql: "SELECT 1".to_owned(),
                    params: Vec::new(),
                    tx,
                })
                .await
                .expect("query should be admitted");

            let mut response_is_ready = false;
            for _ in 0..10_000 {
                response_is_ready = matches!(
                    &*lock_unpoisoned(&ready_response.control.status),
                    ResponseStatus::Ready(_)
                );
                if response_is_ready {
                    break;
                }
                future::yield_now().await;
            }
            assert!(
                response_is_ready,
                "the operation response must be ready before the worker failure"
            );

            conn.request_async(&cx, |tx| Command::TestPanicActor { tx })
                .await
                .expect_err("the later actor panic must terminate the worker");
            let delivered = recv_authoritative_worker_response(
                &cx,
                ready_response,
                cancellation,
                polling_native_cx,
                &lifecycle,
            )
            .await
            .expect("the already-ready response must remain consumable");
            assert!(matches!(
                transaction.finish_operation_response(delivered),
                Err(FrankenError::TransactionRolledBack { .. })
            ));
            transaction
                .rollback(&cx)
                .await
                .expect("rollback retry must acknowledge the later exact receipt");
        });
    }

    #[test]
    fn dropped_or_rollback_response_retains_implicit_rollback_proof() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("first insert should succeed");
            let (terminal_tx, terminal_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            lifecycle.install_transaction_terminal_publication_pause(terminal_tx, release_rx);

            let mut conflict =
                Box::pin(transaction.execute(&cx, "INSERT OR ROLLBACK INTO t VALUES (1)"));
            assert!(
                future::poll_once(conflict.as_mut()).await.is_none(),
                "OR ROLLBACK must remain pending until its authoritative response"
            );
            terminal_rx
                .await
                .expect("implicit rollback must publish its generation receipt");
            drop(conflict);

            assert!(matches!(
                transaction.execute(&cx, "SELECT 1").await,
                Err(FrankenError::TransactionRolledBack { .. })
            ));
            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionRolledBack { .. })
            ));
            transaction
                .rollback(&cx)
                .await
                .expect("rollback retry must acknowledge the retained rollback proof");

            release_tx
                .send(())
                .expect("actor must retain the abandoned response publication");
            assert!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("connection should remain usable")
                    .is_empty(),
                "OR ROLLBACK must remove every row in the transaction"
            );
            drop(transaction);
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                0,
                "implicit rollback proof must suppress stale Drop cleanup"
            );
        });
    }

    #[test]
    fn implicit_rollback_receipt_overrides_worker_disconnect_on_first_execute() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("first insert should succeed");
            let (terminal_tx, terminal_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            lifecycle.install_transaction_terminal_publication_pause(terminal_tx, release_rx);

            let mut conflict =
                Box::pin(transaction.execute(&cx, "INSERT OR ROLLBACK INTO t VALUES (1)"));
            assert!(
                future::poll_once(conflict.as_mut()).await.is_none(),
                "OR ROLLBACK must remain pending after receipt publication"
            );
            terminal_rx
                .await
                .expect("implicit rollback must publish its generation receipt");
            drop(release_tx);

            assert!(matches!(
                conflict.await,
                Err(FrankenError::TransactionRolledBack { .. })
            ));
            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionRolledBack { .. })
            ));
            transaction
                .rollback(&cx)
                .await
                .expect("rollback retry must acknowledge the retained rollback proof");
            WorkerExit::new(&lifecycle).await;
        });
    }

    #[test]
    fn successful_nonterminal_ownership_loss_is_unknown_in_first_response() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            conn.lifecycle
                .successful_nonterminal_ownership_endings
                .store(1, Ordering::Release);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");

            assert!(matches!(
                transaction.execute(&cx, "INSERT INTO t VALUES (1)").await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "statement",
                    ..
                })
            ));
            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "statement",
                    ..
                })
            ));
            assert!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("the worker should remain usable after the injected ownership loss")
                    .is_empty()
            );
        });
    }

    #[test]
    fn unknown_receipt_overrides_worker_disconnect_on_first_nonterminal_response() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            lifecycle
                .successful_nonterminal_ownership_endings
                .store(1, Ordering::Release);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            let (terminal_tx, terminal_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            lifecycle.install_transaction_terminal_publication_pause(terminal_tx, release_rx);

            let mut execute = Box::pin(transaction.execute(&cx, "INSERT INTO t VALUES (1)"));
            assert!(
                future::poll_once(execute.as_mut()).await.is_none(),
                "the injected ownership loss must pause after receipt publication"
            );
            terminal_rx
                .await
                .expect("unexpected ownership loss must publish an unknown receipt");
            drop(release_tx);

            assert!(matches!(
                execute.await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "statement",
                    ..
                })
            ));
            assert!(matches!(
                transaction.execute(&cx, "SELECT 1").await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "statement",
                    ..
                })
            ));
            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "statement",
                    ..
                })
            ));
            assert!(matches!(
                transaction.rollback(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "statement",
                    ..
                })
            ));
            WorkerExit::new(&lifecycle).await;
        });
    }

    #[test]
    fn actor_tombstone_lookup_precedes_busy_and_eviction_fails_closed() {
        let conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_sync("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .expect("schema should succeed");

        let mut committed = conn
            .begin_transaction_sync()
            .expect("first transaction should begin");
        committed
            .execute_sync("INSERT INTO t VALUES (1)")
            .expect("first insert should succeed");
        let committed_token = committed.token;
        committed
            .commit_sync()
            .expect("first transaction should commit");

        let immediate_retry = conn
            .sender()
            .expect("actor should remain open")
            .request_sync(|tx| Command::TransactionCommit {
                token: committed_token,
                tx,
            })
            .expect("the actor tombstone must answer before the Busy fence");
        assert!(immediate_retry.ownership_ended);
        immediate_retry
            .result
            .expect("the exact committed proof must be replayed without execution");

        let mut rolled_back = conn
            .begin_transaction_sync()
            .expect("second transaction should begin");
        rolled_back
            .execute_sync("INSERT INTO t VALUES (2)")
            .expect("second insert should succeed");
        let rolled_back_token = rolled_back.token;
        let cross_operation_retry = conn
            .sender()
            .expect("actor should remain open")
            .request_sync(|tx| Command::TransactionRollback {
                token: committed_token,
                tx,
            })
            .expect("the committed tombstone must answer before the newer-owner Busy fence");
        assert!(cross_operation_retry.ownership_ended);
        assert!(matches!(
            cross_operation_retry.result,
            Err(FrankenError::NoActiveTransaction)
        ));
        assert!(
            conn.in_transaction(),
            "a committed generation's rollback retry must not roll back its successor"
        );
        rolled_back
            .rollback_sync()
            .expect("second transaction should roll back");

        let rolled_back_commit_retry = conn
            .sender()
            .expect("actor should remain open")
            .request_sync(|tx| Command::TransactionCommit {
                token: rolled_back_token,
                tx,
            })
            .expect("the rollback tombstone must answer before the stale-token fence");
        assert!(rolled_back_commit_retry.ownership_ended);
        assert!(matches!(
            rolled_back_commit_retry.result,
            Err(FrankenError::TransactionRolledBack { .. })
        ));

        let mut newer = conn
            .begin_transaction_sync()
            .expect("newer transaction should begin");
        newer
            .execute_sync("INSERT INTO t VALUES (3)")
            .expect("newer insert should succeed");
        assert!(matches!(
            conn.sender()
                .expect("actor should remain open")
                .request_sync(|tx| Command::TransactionCommit {
                    token: committed_token,
                    tx,
                }),
            Err(FrankenError::Busy)
        ));
        committed
            .commit_sync()
            .expect("the retained per-generation receipt must outlive actor eviction");
        assert!(matches!(
            committed.rollback_sync(),
            Err(FrankenError::NoActiveTransaction)
        ));
        rolled_back
            .rollback_sync()
            .expect("rolled-back receipt must remain independently recoverable");
        assert!(matches!(
            rolled_back.commit_sync(),
            Err(FrankenError::TransactionRolledBack { .. })
        ));
        assert!(
            conn.in_transaction(),
            "old-generation retries must not touch the newer live transaction"
        );
        newer
            .commit_sync()
            .expect("newer transaction should still commit normally");

        let rows = conn
            .query_sync("SELECT id FROM t ORDER BY id")
            .expect("committed rows should remain queryable");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
        assert_eq!(rows[1].get(0), Some(&SqliteValue::Integer(3)));
    }

    #[test]
    fn connection_close_publishes_rollback_to_the_live_generation() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");

            let (response, cancellation, polling_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::Close { tx })
                .await
                .expect("close should be admitted");
            recv_authoritative_worker_response(
                &cx,
                response,
                cancellation,
                polling_native_cx,
                &lifecycle,
            )
            .await
            .expect("close should succeed");

            transaction
                .rollback(&cx)
                .await
                .expect("close must retain a proven rollback result");
            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionRolledBack { .. })
            ));
            drop(transaction);
            WorkerExit::new(&lifecycle).await;
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                0,
                "close-published rollback must suppress stale transaction Drop cleanup"
            );
        });
    }

    #[test]
    fn connection_close_error_after_effect_publishes_typed_unknown() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            lifecycle
                .close_post_effect_failures
                .store(1, Ordering::Release);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");

            let (response, cancellation, polling_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::Close { tx })
                .await
                .expect("close should be admitted");
            assert!(matches!(
                recv_authoritative_worker_response(
                    &cx,
                    response,
                    cancellation,
                    polling_native_cx,
                    &lifecycle,
                )
                .await,
                Err(FrankenError::Internal(detail))
                    if detail.contains("connection-close error after transaction end")
            ));

            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "connection close",
                    ..
                })
            ));
            assert!(matches!(
                transaction.rollback(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "connection close",
                    ..
                })
            ));
            drop(transaction);
            drop(conn);
            WorkerExit::new(&lifecycle).await;
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                0,
                "unknown close disposition must suppress stale transaction cleanup"
            );
        });
    }

    #[test]
    fn worker_failure_before_terminal_effect_is_closed_as_proven_rollback() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");

            let (response, cancellation, polling_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::TestPanicActor { tx })
                .await
                .expect("panic command should be admitted");
            assert!(
                recv_authoritative_worker_response(
                    &cx,
                    response,
                    cancellation,
                    polling_native_cx,
                    &lifecycle,
                )
                .await
                .is_err(),
                "worker panic must disconnect the admitted response"
            );

            transaction
                .rollback(&cx)
                .await
                .expect("worker-terminal close must retain its proven rollback");
            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionRolledBack { .. })
            ));
            drop(transaction);
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                0,
                "worker-terminal rollback proof must suppress cleanup onto a dead actor"
            );
        });
    }

    #[test]
    fn worker_failure_after_commit_effect_before_publication_is_typed_unknown() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            lifecycle
                .terminal_pre_publication_panics
                .store(1, Ordering::Release);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");

            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "commit",
                    ..
                })
            ));
            assert!(matches!(
                transaction.rollback(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "commit",
                    ..
                })
            ));
            WorkerExit::new(&lifecycle).await;
            drop(transaction);
            assert_eq!(
                lifecycle.drop_rollback_calls.load(Ordering::Acquire),
                0,
                "unknown committed-or-rolled-back disposition must suppress stale cleanup"
            );
        });
    }

    #[cfg(all(feature = "native", any(unix, windows)))]
    #[test]
    fn worker_failure_after_durable_commit_before_logical_teardown_is_typed_unknown() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let dir = tempfile::tempdir().expect("temporary directory should be created");
            let db_path = dir.path().join("post-durable-commit-panic.db");
            let db_path = db_path
                .to_str()
                .expect("temporary database path should be UTF-8")
                .to_owned();
            let conn = AsyncConnection::open(&cx, db_path.clone())
                .await
                .expect("file-backed connection should open");
            conn.execute(
                &cx,
                "CREATE TABLE marker (id INTEGER PRIMARY KEY, note TEXT NOT NULL)",
            )
            .await
            .expect("marker schema should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            let mut transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(
                    &cx,
                    "INSERT INTO marker VALUES (1, 'durable-before-teardown')",
                )
                .await
                .expect("marker insert should succeed");

            if !fsqlite_core::connection::arm_post_durable_commit_panic_for_test(db_path.clone()) {
                transaction
                    .rollback(&cx)
                    .await
                    .expect("optimized builds compile the commit-path hook out");
                return;
            }

            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "commit",
                    ..
                })
            ));
            assert!(matches!(
                transaction.commit(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "commit",
                    ..
                })
            ));
            assert!(matches!(
                transaction.rollback(&cx).await,
                Err(FrankenError::TransactionOutcomeUnknown {
                    operation: "commit",
                    ..
                })
            ));
            WorkerExit::new(&lifecycle).await;
            assert!(matches!(
                lifecycle.terminal_state(),
                WorkerTerminalState::Broken {
                    transaction_disposition: WorkerTransactionDisposition::OutcomeUnknown,
                    ..
                }
            ));
            drop(transaction);
            drop(conn);

            let successor = AsyncConnection::open(&cx, db_path)
                .await
                .expect("successor connection should reopen the committed database");
            let marker_rows = successor
                .query(&cx, "SELECT id FROM marker WHERE id = 1")
                .await
                .expect("durable marker should be readable");
            assert_eq!(
                marker_rows.len(),
                1,
                "the ambiguous commit effect must be present exactly once"
            );

            let mut successor_transaction = successor
                .begin_transaction(&cx)
                .await
                .expect("successor transaction should begin");
            successor_transaction
                .execute(&cx, "INSERT INTO marker VALUES (2, 'successor')")
                .await
                .expect("successor insert should succeed");
            successor_transaction
                .commit(&cx)
                .await
                .expect("successor commit should succeed");
            let rows = successor
                .query(&cx, "SELECT id FROM marker ORDER BY id")
                .await
                .expect("both committed effects should be readable");
            assert_eq!(
                rows.iter()
                    .map(|row| row.values()[0].clone())
                    .collect::<Vec<_>>(),
                vec![SqliteValue::Integer(1), SqliteValue::Integer(2)],
                "retrying the unknown generation and using its successor must not duplicate or lose effects"
            );
        });
    }

    #[test]
    fn abandoned_ready_begin_receipt_rolls_back_without_a_transaction_handle() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let (response, _cancellation, _polling_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::BeginOwnedTransaction {
                    connection_id: conn.connection_id,
                    tx,
                })
                .await
                .expect("owned begin should be admitted");

            while !conn.in_transaction() {
                future::yield_now().await;
            }
            drop(response);

            conn.query(&cx, "SELECT 1")
                .await
                .expect("receipt Drop must schedule rollback before ordinary work");
            assert!(
                !conn.in_transaction(),
                "abandoned ready receipt must not leave an ownerless transaction"
            );
        });
    }

    #[test]
    fn transaction_drop_rollback_bypasses_a_full_ordinary_mailbox() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");

            let (entered_tx, entered_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            let (blocked_response, blocked_cancellation, blocked_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::TestBlockActor {
                    entered: entered_tx,
                    release: release_rx,
                    tx,
                })
                .await
                .expect("blocking command should be admitted");
            entered_rx.await.expect("actor should enter blocker");

            let mut queued = Vec::with_capacity(COMMAND_CAPACITY);
            for _ in 0..COMMAND_CAPACITY {
                queued.push(
                    conn.sender()
                        .expect("actor should remain open")
                        .request_async(&cx, |tx| Command::Query {
                            sql: "SELECT 1".to_owned(),
                            tx,
                        })
                        .await
                        .expect("ordinary mailbox slot should be admitted"),
                );
            }

            drop(transaction);
            release_tx.send(()).expect("actor should retain blocker");
            recv_authoritative_worker_response(
                &cx,
                blocked_response,
                blocked_cancellation,
                blocked_native_cx,
                &conn.lifecycle,
            )
            .await
            .expect("blocking command should finish");
            for (response, cancellation, polling_native_cx) in queued {
                recv_authoritative_worker_response(
                    &cx,
                    response,
                    cancellation,
                    polling_native_cx,
                    &conn.lifecycle,
                )
                .await
                .expect("queued command should run after priority rollback");
            }

            assert!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("post-drop query should succeed")
                    .is_empty(),
                "drop cleanup must roll back even when ordinary admission was full"
            );
        });
    }

    #[test]
    fn stale_drop_cleanup_cannot_rollback_a_new_generation() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");

            let mut first = conn.begin_transaction(&cx).await.expect("first generation");
            let stale = first.token;
            first.rollback(&cx).await.expect("first rollback");

            let mut second = conn
                .begin_transaction(&cx)
                .await
                .expect("second generation");
            second
                .execute(&cx, "INSERT INTO t VALUES (2)")
                .await
                .expect("second-generation insert should succeed");
            conn.transaction_cleanup.request(stale);
            assert_eq!(
                second
                    .query(&cx, "SELECT * FROM t")
                    .await
                    .expect("stale cleanup must leave current owner live")
                    .len(),
                1
            );
            second
                .commit(&cx)
                .await
                .expect("current generation should still commit");

            assert_eq!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("committed row should remain")
                    .len(),
                1
            );
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

    #[test]
    fn sync_entry_from_runtime_task_fails_without_blocking() {
        test_runtime().block_on(async {
            assert!(
                matches!(
                    AsyncConnection::open_sync(":memory:"),
                    Err(FrankenError::Internal(_))
                ),
                "sync open must not block a runtime task"
            );
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");

            let error = match conn.query_sync("SELECT 1") {
                Ok(_) => panic!("sync entry on a runtime task must fail"),
                Err(error) => error,
            };
            assert!(
                matches!(error, FrankenError::Internal(_)),
                "runtime reentry should report an API contract error: {error}"
            );
        });
    }

    #[test]
    fn request_drop_before_admission_has_no_actor_effect() {
        let cancellation = CancellationRelay::new();
        let (sender, receiver) =
            response_channel::<Result<(), FrankenError>>(Some(cancellation.clone()));

        drop(receiver);

        assert!(
            !cancellation.is_requested(),
            "a request that never reached the mailbox must not cancel actor work"
        );
        drop(sender);
    }

    #[test]
    fn dropped_admitted_receiver_requests_actor_cancellation() {
        let cancellation = CancellationRelay::new();
        let (sender, receiver) =
            response_channel::<Result<(), FrankenError>>(Some(cancellation.clone()));
        receiver.mark_admitted();

        drop(receiver);

        assert!(
            cancellation.is_requested(),
            "abandoning an admitted request must relay cancellation to the worker"
        );
        drop(sender);
    }

    #[test]
    fn abandoned_async_permit_wakes_blocked_sync_capacity_waiter() {
        let (wait_tx, wait_rx) = sync_mpsc::channel();
        let command_capacity = Arc::new(CommandCapacitySignal::with_wait_observer(wait_tx));
        let (command_tx, _command_rx) = async_mpsc::channel(1);
        let held_permit = CommandPermit::new(
            command_tx
                .try_reserve()
                .expect("capacity-one channel should admit the first reservation"),
            &command_capacity,
        );
        let sync_command_tx = command_tx.clone();
        let sync_capacity = Arc::clone(&command_capacity);
        let (completed_tx, completed_rx) = sync_mpsc::channel();

        let waiter = thread::spawn(move || {
            let permit = sync_capacity
                .reserve_blocking(&sync_command_tx)
                .expect("sync waiter should reserve released capacity");
            drop(permit);
            completed_tx
                .send(())
                .expect("test owner should retain completion receiver");
        });

        wait_rx
            .recv()
            .expect("sync waiter should reach the full-capacity wait boundary");
        drop(held_permit);
        completed_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("abandoned async permit must wake the private sync wait lane");
        waiter
            .join()
            .expect("sync capacity waiter should not panic");
    }

    #[test]
    fn cancellation_after_async_reservation_wakes_blocked_sync_capacity_waiter() {
        test_runtime().block_on(async {
            let cancellation = CancellationRelay::new();
            let (wait_tx, wait_rx) = sync_mpsc::channel();
            let command_capacity =
                Arc::new(CommandCapacitySignal::with_post_reservation_cancellation(
                    wait_tx,
                    cancellation.clone(),
                ));
            let (command_tx, _command_rx) = async_mpsc::channel(1);
            // Deliberately raw setup permit: releasing it wakes the registered
            // async waiter but not our private synchronous Condvar lane.
            let initial_permit = command_tx
                .try_reserve()
                .expect("capacity-one channel should admit the setup reservation");
            let sync_command_tx = command_tx.clone();
            let sync_capacity = Arc::clone(&command_capacity);
            let sender = CommandSender {
                tx: Some(command_tx),
                command_capacity: Arc::clone(&command_capacity),
                lifecycle: Arc::new(WorkerLifecycle::new(Arc::clone(&command_capacity))),
            };
            let cx = Cx::new();
            let polling_native_cx =
                native_cx_for_polling_task().expect("test must run inside a native runtime task");
            let mut async_reservation =
                Box::pin(sender.reserve_async(&cx, &cancellation, &polling_native_cx));
            assert!(
                future::poll_once(async_reservation.as_mut())
                    .await
                    .is_none(),
                "async reservation must initially wait behind the setup permit"
            );

            let (completed_tx, completed_rx) = sync_mpsc::channel();
            let sync_waiter = thread::spawn(move || {
                let permit = sync_capacity
                    .reserve_blocking(&sync_command_tx)
                    .expect("sync waiter should reserve released capacity");
                drop(permit);
                completed_tx
                    .send(())
                    .expect("test owner should retain completion receiver");
            });
            wait_rx
                .recv()
                .expect("sync waiter should be asleep before async reservation wins");

            drop(initial_permit);
            assert!(matches!(
                future::poll_once(async_reservation.as_mut())
                    .await
                    .expect("freed capacity must resolve the async reservation"),
                Err(FrankenError::Interrupt)
            ));
            completed_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("post-reservation cancellation must wake the sleeping sync lane");
            sync_waiter
                .join()
                .expect("sync capacity waiter should not panic");
        });
    }

    #[test]
    fn queued_async_reserve_cancellation_wakes_blocked_sync_capacity_waiter() {
        test_runtime().block_on(async {
            let cancellation = CancellationRelay::new();
            let (wait_tx, wait_rx) = sync_mpsc::channel();
            let command_capacity = Arc::new(CommandCapacitySignal::with_wait_observer(wait_tx));
            let (command_tx, _command_rx) = async_mpsc::channel(1);
            // The raw setup reservation lets the async Reserve join the FIFO
            // before any capacity becomes available.
            let initial_permit = command_tx
                .try_reserve()
                .expect("capacity-one channel should admit the setup reservation");
            let sync_command_tx = command_tx.clone();
            let sync_capacity = Arc::clone(&command_capacity);
            let sender = CommandSender {
                tx: Some(command_tx),
                command_capacity: Arc::clone(&command_capacity),
                lifecycle: Arc::new(WorkerLifecycle::new(Arc::clone(&command_capacity))),
            };
            let cx = Cx::new();
            let polling_native_cx =
                native_cx_for_polling_task().expect("test must run inside a native runtime task");
            let mut async_reservation =
                Box::pin(sender.reserve_async(&cx, &cancellation, &polling_native_cx));
            assert!(
                future::poll_once(async_reservation.as_mut())
                    .await
                    .is_none(),
                "async Reserve must be queued behind the setup reservation"
            );

            let (completed_tx, completed_rx) = sync_mpsc::channel();
            let sync_waiter = thread::spawn(move || {
                let permit = sync_capacity
                    .reserve_blocking(&sync_command_tx)
                    .expect("sync waiter should reserve exposed capacity");
                drop(permit);
                completed_tx
                    .send(())
                    .expect("test owner should retain completion receiver");
            });
            wait_rx
                .recv()
                .expect("sync waiter should be asleep before queued cancellation");

            // Capacity is now physically free but logically belongs to the
            // queued async Reserve. Cancelling that Reserve exposes the slot;
            // its RAII notifier must wake the unrelated sync lane.
            drop(initial_permit);
            cancellation.request();
            assert!(matches!(
                future::poll_once(async_reservation.as_mut())
                    .await
                    .expect("queued cancellation must resolve the async reservation"),
                Err(FrankenError::Interrupt)
            ));
            drop(async_reservation);
            completed_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("queued Reserve cleanup must wake the sleeping sync lane");
            sync_waiter
                .join()
                .expect("sync capacity waiter should not panic");
        });
    }

    #[test]
    fn full_actor_mailbox_cancellation_consumes_no_sql_effect() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let (entered_tx, entered_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            let (blocked_response, blocked_cancellation, blocked_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::TestBlockActor {
                    entered: entered_tx,
                    release: release_rx,
                    tx,
                })
                .await
                .expect("blocking command should be admitted");
            entered_rx.await.expect("actor should enter blocker");

            let mut queued = Vec::with_capacity(COMMAND_CAPACITY);
            for _ in 0..COMMAND_CAPACITY {
                queued.push(
                    conn.sender()
                        .expect("actor should remain open")
                        .request_async(&cx, |tx| Command::Query {
                            sql: "SELECT 1".to_owned(),
                            tx,
                        })
                        .await
                        .expect("mailbox slot should be admitted"),
                );
            }

            let cancelled_cx = Cx::new();
            let mut cancelled_insert =
                Box::pin(conn.execute(&cancelled_cx, "INSERT INTO t VALUES (99)"));
            assert!(
                future::poll_once(cancelled_insert.as_mut()).await.is_none(),
                "the state-changing request must wait behind the full mailbox"
            );
            cancelled_cx.cancel();
            assert!(matches!(
                cancelled_insert.await,
                Err(FrankenError::Interrupt)
            ));

            release_tx.send(()).expect("actor should retain blocker");
            recv_authoritative_worker_response(
                &cx,
                blocked_response,
                blocked_cancellation,
                blocked_native_cx,
                &conn.lifecycle,
            )
            .await
            .expect("blocking command should finish");
            for (response, cancellation, polling_native_cx) in queued {
                recv_authoritative_worker_response(
                    &cx,
                    response,
                    cancellation,
                    polling_native_cx,
                    &conn.lifecycle,
                )
                .await
                .expect("queued query should finish");
            }
            assert!(
                conn.query(&cx, "SELECT * FROM t")
                    .await
                    .expect("table should remain queryable")
                    .is_empty(),
                "cancellation before mailbox admission must consume no SQL effect"
            );
        });
    }

    #[test]
    fn published_response_wins_over_later_cancellation() {
        let cancellation = CancellationRelay::new();
        let (sender, receiver) =
            response_channel::<Result<i32, FrankenError>>(Some(cancellation.clone()));
        receiver.mark_admitted();
        assert_eq!(
            sender.send_prefer_cancellation(Ok(47), Err(FrankenError::Interrupt)),
            ResponsePublication::Primary
        );

        cancellation.request();

        assert_eq!(
            receiver
                .recv_blocking()
                .expect("published response remains connected")
                .expect("published result remains authoritative"),
            47
        );
    }

    #[test]
    fn cancellation_before_open_publication_selects_interrupt() {
        let cancellation = CancellationRelay::new();
        let (sender, receiver) =
            response_channel::<Result<i32, FrankenError>>(Some(cancellation.clone()));
        receiver.mark_admitted();
        cancellation.request();

        assert_eq!(
            sender.send_prefer_cancellation(Ok(47), Err(FrankenError::Interrupt)),
            ResponsePublication::Cancellation
        );
        assert!(matches!(
            receiver
                .recv_blocking()
                .expect("cancellation result must be published"),
            Err(FrankenError::Interrupt)
        ));
    }

    #[test]
    fn dropping_live_admitted_actor_response_requests_only_its_relay() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let (entered_tx, entered_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);

            let (response, cancellation, _polling_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::TestBlockActor {
                    entered: entered_tx,
                    release: release_rx,
                    tx,
                })
                .await
                .expect("test command should be admitted");
            entered_rx
                .await
                .expect("worker should publish actor entry before blocking");
            assert!(!cancellation.is_requested());

            drop(response);

            assert!(
                cancellation.is_requested(),
                "dropping the admitted response must request its operation relay"
            );
            assert!(
                cx.checkpoint().is_ok(),
                "the one-way relay must not cancel the caller context"
            );
            release_tx
                .send(())
                .expect("worker should still own the admitted command");

            let fresh_cx = Cx::new();
            let rows = conn
                .query(&fresh_cx, "SELECT 1")
                .await
                .expect("worker should drain the abandoned command and remain usable");
            assert_eq!(rows.len(), 1);
        });
    }

    #[test]
    fn dropping_public_admitted_sql_future_relays_cancellation_and_keeps_root_usable() {
        test_runtime().block_on(async {
            let setup_cx = Cx::new();
            let conn = AsyncConnection::open(&setup_cx, ":memory:")
                .await
                .expect("open should succeed");
            let (entered_tx, entered_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            conn.lifecycle
                .install_actor_operation_pause(entered_tx, release_rx);

            let caller_cx = Cx::new();
            let mut query = Box::pin(conn.query(&caller_cx, "SELECT 42"));
            assert!(
                future::poll_once(query.as_mut()).await.is_none(),
                "the public SQL future must remain pending after admission"
            );
            entered_rx
                .await
                .expect("the worker must enter the admitted engine operation");

            drop(query);
            assert!(
                caller_cx.checkpoint().is_ok(),
                "dropping one admitted SQL future must not cancel its caller context"
            );
            release_tx
                .send(())
                .expect("the worker must retain the admitted SQL operation");

            let fresh_cx = Cx::new();
            let row = conn
                .query_row(&fresh_cx, "SELECT 7")
                .await
                .expect("the worker must drain the abandoned operation and stay usable");
            assert_eq!(row.get(0), Some(&SqliteValue::Integer(7)));
            assert!(
                matches!(
                    conn.lifecycle.terminal_state(),
                    WorkerTerminalState::Running
                ),
                "abandoning one admitted SQL future must not terminate the actor"
            );
        });
    }

    #[test]
    fn admitted_real_sql_observes_cancellation_without_poisoning_connection_root() {
        test_runtime().block_on(async {
            let setup_cx = Cx::new();
            let conn = AsyncConnection::open(&setup_cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute_batch(
                &setup_cx,
                "CREATE TABLE numbers (n INTEGER PRIMARY KEY);
                 INSERT INTO numbers VALUES (0), (1), (2), (3), (4), (5), (6), (7);",
            )
            .await
            .expect("cancellation fixture should succeed");

            let (entered_tx, entered_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            conn.lifecycle
                .install_actor_operation_pause(entered_tx, release_rx);

            let cancelled_cx = Cx::new();
            let mut query = Box::pin(conn.query(
                &cancelled_cx,
                "SELECT a.n, b.n, c.n, d.n, e.n
                 FROM numbers AS a, numbers AS b, numbers AS c,
                      numbers AS d, numbers AS e",
            ));
            assert!(
                future::poll_once(query.as_mut()).await.is_none(),
                "the real SQL request must remain pending after mailbox admission"
            );
            entered_rx
                .await
                .expect("the worker must enter the real engine operation");

            cancelled_cx.cancel();
            assert!(
                future::poll_once(query.as_mut()).await.is_none(),
                "cancellation must relay to the admitted actor without fabricating a response"
            );
            release_tx
                .send(())
                .expect("the actor must retain the admitted SQL operation");

            let error = query
                .await
                .expect_err("the engine execution checkpoint must observe cancellation");
            assert!(
                matches!(error, FrankenError::Interrupt | FrankenError::Abort),
                "admitted SQL must publish a documented cancellation outcome: {error}"
            );
            assert!(
                cancelled_cx.checkpoint().is_err(),
                "the operation's caller context remains cancelled"
            );
            assert!(
                matches!(
                    conn.lifecycle.terminal_state(),
                    WorkerTerminalState::Running
                ),
                "operation cancellation must not terminate the actor"
            );

            let fresh_cx = Cx::new();
            let row = conn
                .query_row(&fresh_cx, "SELECT count(*) FROM numbers")
                .await
                .expect("a fresh operation must prove the connection root was not poisoned");
            assert_eq!(row.get(0), Some(&SqliteValue::Integer(8)));
        });
    }

    #[test]
    fn distinct_attached_native_cancellation_wakes_and_relays_after_admission() {
        test_runtime().block_on(async {
            // This synthetic native Cx is deliberately distinct from the
            // runtime's ambient current-task Cx.
            let attached_native = NativeCx::for_testing();
            let cx = Cx::new();
            cx.set_native_cx(attached_native.clone());
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let (entered_tx, entered_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            let (response, cancellation, polling_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::TestBlockActor {
                    entered: entered_tx,
                    release: release_rx,
                    tx,
                })
                .await
                .expect("test command should be admitted");
            entered_rx
                .await
                .expect("worker should publish actor entry before blocking");

            let mut waiter = Box::pin(recv_authoritative_worker_response(
                &cx,
                response,
                cancellation.clone(),
                polling_native_cx,
                &conn.lifecycle,
            ));
            let wake_counter = Arc::new(WakeCounter::default());
            let waker = Waker::from(Arc::clone(&wake_counter));
            let mut task_cx = Context::from_waker(&waker);
            assert!(
                waiter.as_mut().poll(&mut task_cx).is_pending(),
                "admitted blocked command must leave its response pending"
            );

            attached_native.set_cancel_requested(true);
            assert!(
                wake_counter.wake_count.load(Ordering::Acquire) > 0,
                "distinct attached native cancellation must wake the caller waiter"
            );
            assert!(
                waiter.as_mut().poll(&mut task_cx).is_pending(),
                "cancellation relays to the actor but does not fabricate a result"
            );
            assert!(
                cancellation.is_requested(),
                "supplied-Cx cancellation must reach the admitted actor operation"
            );

            release_tx
                .send(())
                .expect("worker should retain the admitted test command");
            waiter
                .await
                .expect("worker publication remains the authoritative result");
        });
    }

    #[test]
    fn dropped_close_future_resumes_the_same_admitted_close() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let (entered_tx, entered_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            let (blocked_response, blocked_cancellation, blocked_native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::TestBlockActor {
                    entered: entered_tx,
                    release: release_rx,
                    tx,
                })
                .await
                .expect("blocking command should be admitted");
            entered_rx
                .await
                .expect("worker should enter blocking command");

            let close_cx = Cx::new();
            let mut close_future = Box::pin(conn.close(&close_cx));
            assert!(
                future::poll_once(close_future.as_mut()).await.is_none(),
                "close must remain pending behind the admitted blocker"
            );
            drop(close_future);

            release_tx
                .send(())
                .expect("blocked worker should still own its release receiver");
            recv_authoritative_worker_response(
                &cx,
                blocked_response,
                blocked_cancellation,
                blocked_native_cx,
                &conn.lifecycle,
            )
            .await
            .expect("blocking command should finish");

            let resume_cx = Cx::new();
            conn.close(&resume_cx)
                .await
                .expect("second close call should drain the retained response");
            assert_eq!(conn.state, AsyncConnectionState::Closed);
        });
    }

    #[test]
    fn dropped_close_after_publication_preserves_worker_for_exactly_one_retry_join() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);
            let (published_tx, published_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            lifecycle.install_close_publication_pause(published_tx, release_rx);

            let mut close_future = Box::pin(conn.close(&cx));
            assert!(
                future::poll_once(close_future.as_mut()).await.is_none(),
                "Close must remain pending before worker lifecycle terminalization"
            );
            published_rx
                .await
                .expect("worker must pause after publishing the exact Close response");
            assert!(
                future::poll_once(close_future.as_mut()).await.is_none(),
                "Close must consume its response and wait for worker termination"
            );

            drop(close_future);
            assert!(
                conn.worker.is_some(),
                "cancelling terminalization must preserve ownership of the JoinHandle"
            );
            assert_eq!(conn.state, AsyncConnectionState::Closing);
            assert_eq!(
                lifecycle.join_calls.load(Ordering::Acquire),
                0,
                "the paused worker cannot have been joined yet"
            );

            release_tx
                .send(())
                .expect("the paused worker must retain its release receiver");
            conn.close(&cx)
                .await
                .expect("retry must await and join the same terminal worker");
            assert_eq!(conn.state, AsyncConnectionState::Closed);
            assert!(
                conn.worker.is_none(),
                "the worker handle may be consumed only after its join completes"
            );
            assert_eq!(
                lifecycle.join_calls.load(Ordering::Acquire),
                1,
                "retry must join the retained worker exactly once"
            );

            conn.close(&cx)
                .await
                .expect("an already closed connection remains idempotent");
            assert_eq!(
                lifecycle.join_calls.load(Ordering::Acquire),
                1,
                "idempotent close must not repeat the completed join"
            );
        });
    }

    #[test]
    fn async_close_joins_broken_worker_once_and_preserves_panic() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = Arc::clone(&conn.lifecycle);

            let error = conn
                .request_async(&cx, |tx| Command::TestPanicActor { tx })
                .await
                .expect_err("injected actor panic must disconnect its response");
            assert!(
                matches!(
                    error,
                    FrankenError::Internal(ref detail)
                        if detail.contains("actor loop")
                            && detail.contains("injected async actor panic")
                ),
                "worker response must retain the panic stage and payload: {error}"
            );
            assert_eq!(lifecycle.join_calls.load(Ordering::Acquire), 0);

            let close_error = conn
                .close(&cx)
                .await
                .expect_err("broken worker close must report its terminal cause");
            assert!(
                close_error
                    .to_string()
                    .contains("injected async actor panic")
            );
            assert_eq!(conn.state, AsyncConnectionState::Broken);
            assert_eq!(lifecycle.join_calls.load(Ordering::Acquire), 1);

            let repeated = conn
                .close(&cx)
                .await
                .expect_err("broken close remains observably broken");
            assert!(repeated.to_string().contains("injected async actor panic"));
            assert_eq!(
                lifecycle.join_calls.load(Ordering::Acquire),
                1,
                "repeated close must not join a consumed handle twice"
            );
        });
    }

    #[test]
    fn ordinary_sync_stream_disconnect_preserves_actor_panic_cause() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        let lifecycle = Arc::clone(&conn.lifecycle);
        lifecycle.ordinary_stream_panics.store(1, Ordering::Release);

        let error = conn
            .query_with_params_for_each_sync("SELECT 1", &[], |_| Ok(()))
            .expect_err("injected stream panic must disconnect the row channel");
        assert!(
            matches!(
                error,
                FrankenError::Internal(ref detail)
                    if detail.contains("actor loop")
                        && detail.contains("injected ordinary synchronous stream panic")
            ),
            "row-channel disconnect must preserve the lifecycle stage and panic payload: {error}"
        );
        assert!(
            lifecycle.is_finished(),
            "the sync stream must wait for authoritative worker terminalization"
        );

        let close_error = conn
            .close_sync()
            .expect_err("terminalizing the broken worker must retain the same cause");
        assert!(
            close_error
                .to_string()
                .contains("injected ordinary synchronous stream panic")
        );
    }

    #[test]
    fn transaction_sync_stream_receipt_overrides_disconnect_and_close_preserves_cause() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_batch_sync(
            "CREATE TABLE t (id INTEGER PRIMARY KEY);
             INSERT INTO t VALUES (1), (2);",
        )
        .expect("fixture should succeed");
        let lifecycle = Arc::clone(&conn.lifecycle);
        let transaction = conn
            .begin_transaction_sync()
            .expect("owned transaction should begin");
        lifecycle
            .transaction_stream_panics
            .store(1, Ordering::Release);

        let error = transaction
            .query_with_params_for_each_sync("SELECT id FROM t ORDER BY id", &[], |_| Ok(()))
            .expect_err("injected transaction stream panic must disconnect both response lanes");
        assert!(
            matches!(error, FrankenError::TransactionRolledBack { .. }),
            "the exact rollback receipt must override the stream transport failure: {error}"
        );
        assert!(
            lifecycle.is_finished(),
            "the transaction stream must wait for authoritative worker terminalization"
        );
        assert!(
            !conn.in_transaction(),
            "worker fallback close must publish exact transaction cleanup"
        );

        drop(transaction);
        let close_error = conn
            .close_sync()
            .expect_err("terminalizing the broken worker must retain the same cause");
        assert!(
            close_error
                .to_string()
                .contains("injected transaction synchronous stream panic")
        );
        assert_eq!(
            lifecycle.join_calls.load(Ordering::Acquire),
            1,
            "explicit close must join the broken worker exactly once"
        );
    }

    #[test]
    fn transaction_sync_stream_terminal_receipt_outranks_callback_error() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_batch_sync(
            "CREATE TABLE t (id INTEGER PRIMARY KEY);
             INSERT INTO t VALUES (1), (2);",
        )
        .expect("fixture should succeed");
        let lifecycle = Arc::clone(&conn.lifecycle);
        let transaction = conn
            .begin_transaction_sync()
            .expect("owned transaction should begin");
        lifecycle
            .transaction_stream_panics
            .store(1, Ordering::Release);
        let mut callback_called = false;

        let error = transaction
            .query_with_params_for_each_sync("SELECT id FROM t ORDER BY id", &[], |_| {
                callback_called = true;
                Err(FrankenError::Internal(
                    "injected stream callback error".to_owned(),
                ))
            })
            .expect_err("the exact terminal receipt must remain observable");
        assert!(
            callback_called,
            "the callback error must exist before receipt precedence is tested"
        );
        assert!(
            matches!(error, FrankenError::TransactionRolledBack { .. }),
            "the rollback receipt must outrank the earlier callback error: {error}"
        );

        drop(transaction);
        let close_error = conn
            .close_sync()
            .expect_err("the broken worker must retain its lifecycle diagnostic");
        assert!(
            close_error
                .to_string()
                .contains("injected transaction synchronous stream panic")
        );
    }

    #[test]
    fn sync_close_after_pre_admission_worker_death_never_sticks_closing() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        let lifecycle = Arc::clone(&conn.lifecycle);
        let (panic_tx, panic_rx) = response_channel(None);
        panic_rx.mark_admitted();
        conn.sender()
            .expect("actor should be open")
            .send_stream_sync(Command::TestPanicActor { tx: panic_tx })
            .expect("panic command should be admitted");
        lifecycle.wait_finished_sync();
        drop(panic_rx);

        let error = conn
            .close_sync()
            .expect_err("pre-admission worker death must be terminalized");
        assert!(error.to_string().contains("injected async actor panic"));
        assert_eq!(conn.state, AsyncConnectionState::Broken);
        assert_eq!(lifecycle.join_calls.load(Ordering::Acquire), 1);
        assert!(
            conn.close_response.is_none(),
            "failed admission must not fabricate a retained Close response"
        );

        let repeated = conn
            .close_sync()
            .expect_err("broken close remains observably broken");
        assert!(repeated.to_string().contains("injected async actor panic"));
        assert_eq!(lifecycle.join_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn worker_panic_with_active_transaction_publishes_exact_cleanup_disposition() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let mut conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            conn.execute(&cx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .expect("schema should succeed");
            let transaction = conn
                .begin_transaction(&cx)
                .await
                .expect("owned transaction should begin");
            transaction
                .execute(&cx, "INSERT INTO t VALUES (1)")
                .await
                .expect("insert should succeed");

            let error = conn
                .request_async(&cx, |tx| Command::TestPanicActor { tx })
                .await
                .expect_err("actor panic must terminate the worker");
            assert!(error.to_string().contains("injected async actor panic"));
            assert!(
                matches!(
                    conn.lifecycle.terminal_state(),
                    WorkerTerminalState::Broken {
                        transaction_disposition: WorkerTransactionDisposition::NoActiveTransaction,
                        ..
                    }
                ),
                "fallback close must publish exact cleanup of the active transaction"
            );
            assert!(!conn.in_transaction());

            drop(transaction);
            conn.close(&cx)
                .await
                .expect_err("the actor failure remains observable after exact cleanup");
        });
    }

    #[test]
    fn drop_disconnects_without_waiting_and_worker_closes_explicitly() {
        test_runtime().block_on(async {
            let cx = Cx::new();
            let conn = AsyncConnection::open(&cx, ":memory:")
                .await
                .expect("open should succeed");
            let lifecycle = Arc::clone(
                &conn
                    .worker
                    .as_ref()
                    .expect("open actor should retain its worker")
                    .lifecycle,
            );
            let (entered_tx, entered_rx) = response_channel(None);
            let (release_tx, release_rx) = sync_mpsc::sync_channel(1);
            let (response, _cancellation, _native_cx) = conn
                .sender()
                .expect("actor should be open")
                .request_async(&cx, |tx| Command::TestBlockActor {
                    entered: entered_tx,
                    release: release_rx,
                    tx,
                })
                .await
                .expect("blocking command should be admitted");
            entered_rx
                .await
                .expect("worker should enter blocking command");

            drop(response);
            drop(conn);

            release_tx
                .send(())
                .expect("connection Drop must not wait for the blocked worker");
            WorkerExit::new(&lifecycle).await;
            assert_eq!(
                lifecycle.close_connection_calls.load(Ordering::Acquire),
                1,
                "mailbox disconnect must make the worker close exactly once"
            );
        });
    }

    #[test]
    fn ordinary_sql_rejects_ownerless_transaction_control() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        assert!(!conn.in_transaction());

        assert!(
            matches!(conn.execute_sync("BEGIN"), Err(FrankenError::Busy)),
            "raw BEGIN must not create an ownerless transaction"
        );
        assert!(!conn.in_transaction());
        assert!(
            matches!(
                conn.execute_batch_sync("SAVEPOINT hidden; SELECT 1; RELEASE hidden"),
                Err(FrankenError::Busy)
            ),
            "ordinary batches must not hide a complete ownerless transaction"
        );
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn synchronous_transactions_are_token_scoped() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_sync("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .expect("schema should succeed");
        let mut transaction = conn
            .begin_transaction_sync()
            .expect("owned transaction should begin");
        transaction
            .execute_sync("INSERT INTO t VALUES (1)")
            .expect("token-scoped insert should succeed");
        assert!(matches!(
            conn.query_sync("SELECT * FROM t"),
            Err(FrankenError::Busy)
        ));
        transaction
            .commit_sync()
            .expect("token-scoped commit should succeed");
        assert_eq!(
            conn.query_sync("SELECT * FROM t")
                .expect("ordinary query should resume")
                .len(),
            1
        );
        drop(transaction);
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn transaction_prepare_and_stream_remain_token_scoped() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_batch_sync(
            "CREATE TABLE t (id INTEGER PRIMARY KEY);
             INSERT INTO t VALUES (1), (2), (3);",
        )
        .expect("fixture should succeed");
        let mut transaction = conn
            .begin_transaction_sync()
            .expect("owned transaction should begin");

        transaction
            .prepare_sync("SELECT id FROM t WHERE id >= ?1")
            .expect("prepare validation should retain token ownership");
        assert!(matches!(
            transaction.prepare_sync("EXPLAIN COMMIT"),
            Err(FrankenError::Busy)
        ));

        let mut streamed = Vec::new();
        transaction
            .query_with_params_for_each_sync(
                "SELECT id FROM t WHERE id >= ?1 ORDER BY id",
                &[SqliteValue::Integer(2)],
                |row| {
                    streamed.push(row.get(0).cloned());
                    assert!(
                        matches!(conn.query_sync("SELECT 1"), Err(FrankenError::Busy)),
                        "same-connection stream reentry must fail before admission"
                    );
                    Ok(())
                },
            )
            .expect("token-scoped stream should complete");
        assert_eq!(
            streamed,
            vec![Some(SqliteValue::Integer(2)), Some(SqliteValue::Integer(3))]
        );
        assert!(matches!(
            transaction.query_with_params_for_each_sync("BEGIN", &[], |_| Ok(())),
            Err(FrankenError::Busy)
        ));

        transaction
            .rollback_sync()
            .expect("owned transaction should roll back");
        drop(transaction);
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn implicit_rollback_finalizes_transaction_handle() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_sync("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .expect("schema should succeed");
        let lifecycle = Arc::clone(&conn.lifecycle);
        let transaction = conn
            .begin_transaction_sync()
            .expect("owned transaction should begin");
        transaction
            .execute_sync("INSERT INTO t VALUES (1)")
            .expect("first insert should succeed");

        assert!(
            transaction
                .execute_sync("INSERT OR ROLLBACK INTO t VALUES (1)")
                .is_err(),
            "constraint failure should end the engine transaction"
        );
        assert!(
            transaction.finalized.load(Ordering::Acquire),
            "ownership-ended metadata must make the token handle terminal"
        );
        assert!(!conn.in_transaction());
        drop(transaction);
        assert_eq!(
            lifecycle.drop_rollback_calls.load(Ordering::Acquire),
            0,
            "dropping an implicitly finalized handle must not schedule stale cleanup"
        );
        assert!(
            conn.query_sync("SELECT * FROM t")
                .expect("connection should remain usable")
                .is_empty(),
            "OR ROLLBACK must roll back the full transaction"
        );
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn nonterminal_error_preserves_active_generation_without_unknown_outcome() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        conn.execute_sync("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .expect("schema should succeed");
        let mut transaction = conn
            .begin_transaction_sync()
            .expect("owned transaction should begin");
        transaction
            .execute_sync("INSERT INTO t VALUES (1)")
            .expect("first insert should succeed");

        let duplicate = transaction
            .execute_sync("INSERT OR ABORT INTO t VALUES (1)")
            .expect_err("ordinary constraint failure should be reported");
        assert!(
            !matches!(duplicate, FrankenError::TransactionOutcomeUnknown { .. }),
            "a still-active generation proves that no terminal outcome exists"
        );
        assert!(conn.in_transaction());
        assert!(transaction.terminal.outcome().is_none());
        assert!(!transaction.finalized.load(Ordering::Acquire));

        transaction
            .execute_sync("INSERT INTO t VALUES (2)")
            .expect("the same generation must remain usable");
        transaction
            .commit_sync()
            .expect("the still-active generation should commit normally");
        assert_eq!(
            conn.query_sync("SELECT * FROM t")
                .expect("committed rows should remain queryable")
                .len(),
            2
        );
        drop(transaction);
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn same_connection_sync_stream_reentry_fails_before_admission() {
        let mut conn = AsyncConnection::open_sync(":memory:").expect("open should succeed");
        let mut other = AsyncConnection::open_sync(":memory:").expect("peer open should succeed");
        conn.execute_batch_sync("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1), (2);")
            .expect("fixture should succeed");
        let mut callbacks = 0;

        conn.query_with_params_for_each_sync("SELECT x FROM t ORDER BY x", &[], |_| {
            callbacks += 1;
            assert!(
                matches!(conn.query_sync("SELECT 1"), Err(FrankenError::Busy)),
                "same-connection callback reentry must fail instead of deadlocking"
            );
            assert_eq!(
                other
                    .query_row_sync("SELECT 7")
                    .expect("a different connection must remain usable")
                    .get(0),
                Some(&SqliteValue::Integer(7))
            );
            Ok(())
        })
        .expect("stream should complete after rejected reentry");

        assert_eq!(callbacks, 2);
        conn.close_sync().expect("close should succeed");
        other.close_sync().expect("peer close should succeed");
    }

    #[test]
    fn nested_cross_connection_stream_keeps_outer_reentry_guard_active() {
        let mut outer = AsyncConnection::open_sync(":memory:").expect("outer open should succeed");
        let mut inner = AsyncConnection::open_sync(":memory:").expect("inner open should succeed");
        outer
            .execute_batch_sync("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1), (2), (3);")
            .expect("outer fixture should succeed");
        inner
            .execute_batch_sync("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (7);")
            .expect("inner fixture should succeed");
        let mut outer_callbacks = 0;
        let mut inner_callbacks = 0;

        outer
            .query_with_params_for_each_sync("SELECT x FROM t ORDER BY x", &[], |_| {
                outer_callbacks += 1;
                inner.query_with_params_for_each_sync("SELECT x FROM t", &[], |_| {
                    inner_callbacks += 1;
                    assert!(
                        matches!(outer.query_sync("SELECT 1"), Err(FrankenError::Busy)),
                        "nested peer callback must not hide the outer connection guard"
                    );
                    assert!(
                        matches!(inner.query_sync("SELECT 1"), Err(FrankenError::Busy)),
                        "the innermost connection must remain guarded too"
                    );
                    Ok(())
                })
            })
            .expect("nested peer stream should complete after rejected reentry");

        assert_eq!(outer_callbacks, 3);
        assert_eq!(inner_callbacks, 3);
        outer.close_sync().expect("outer close should succeed");
        inner.close_sync().expect("inner close should succeed");
    }

    #[test]
    fn detached_custom_environment_is_accepted_by_actor() {
        let runtime = Arc::new(RuntimeContext::new(RuntimeConfig::default()));
        let mut env = ConnectionEnv::new(runtime);
        env.set_page_buffer_max(128);

        let mut conn = AsyncConnection::open_sync_with_env(":memory:", env)
            .expect("detached custom environment should be accepted");
        conn.close_sync().expect("close should succeed");
    }

    #[test]
    fn caller_rooted_environment_is_rejected_before_sync_worker_spawn() {
        let parent = Cx::new();
        let runtime = Arc::new(RuntimeContext::new_with_root_cx(
            RuntimeConfig::default(),
            &parent,
        ));
        let env = ConnectionEnv::new(runtime);

        let error = match AsyncConnection::open_sync_with_env(":memory:", env) {
            Ok(_) => panic!("caller-rooted environment must be rejected"),
            Err(error) => error,
        };
        assert!(
            matches!(error, FrankenError::NotImplemented(_)),
            "sync open must reject caller-rooted provenance before spawning: {error}"
        );
    }

    #[test]
    fn caller_rooted_environment_is_rejected_before_async_cancellation() {
        test_runtime().block_on(async {
            let parent = Cx::new();
            let runtime = Arc::new(RuntimeContext::new_with_root_cx(
                RuntimeConfig::default(),
                &parent,
            ));
            let env = ConnectionEnv::new(runtime);
            let cancelled_cx = Cx::new();
            cancelled_cx.cancel();

            let error = match AsyncConnection::open_with_env(&cancelled_cx, ":memory:", env).await {
                Ok(_) => panic!("caller-rooted environment must be rejected"),
                Err(error) => error,
            };
            assert!(
                matches!(error, FrankenError::NotImplemented(_)),
                "environment provenance rejection must happen before cancellation: {error}"
            );
        });
    }
}
