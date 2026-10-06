//! Caller-owned automatic batching for native pager commits.
//!
//! Producers reserve bounded mailbox capacity BEFORE moving a transaction.
//! Sending commits that intent to the service; abandoning a ticket does not
//! cancel an accepted write. The service owns the original handles until it
//! returns their individual outcomes, including indeterminate commit states.
//!
//! `run` is an ordinary future for a caller-owned scope/task. This module does
//! not spawn a thread, create a runtime, detach work, or change Connection's
//! storage selector. The host must drive the worker alongside its producers
//! and retain the pager's external namespace/append authority until outstanding
//! source writes settle. Dropping the worker cannot settle a physical write.

use std::fmt;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use asupersync::Cx as NativeCx;
use asupersync::channel::{mpsc, oneshot};
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_vfs::VfsFile;
use fsqlite_wal::native_commit::durable::NativeObjectCodec;
use fsqlite_wal::native_pages::NativePageStore;

use crate::native::{NativePager, NativeTransaction};
use crate::traits::{PagerCommitState, TransactionHandle};

/// The exact original transaction and its individual publication outcome.
///
/// A rejection before I/O leaves its private overlay available for rollback or
/// retry. A storage error can leave `InDoubt`; consult `pager_commit_state`
/// before deciding that rollback/retry is allowed. The original error is
/// shared, not stringified or converted into a false transaction-abort verdict.
pub struct NativeCommitCompletion<T: TransactionHandle> {
    pub transaction: T,
    pub result: std::result::Result<(), Arc<FrankenError>>,
}

impl<T: TransactionHandle> fmt::Debug for NativeCommitCompletion<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeCommitCompletion")
            .field("state", &self.transaction.pager_commit_state())
            .field("result", &self.result)
            .finish_non_exhaustive()
    }
}

/// Failed mailbox admission returns the transaction without publishing it.
pub struct NativeCommitSubmitError<T: TransactionHandle> {
    pub transaction: T,
    pub error: FrankenError,
}

impl<T: TransactionHandle> fmt::Debug for NativeCommitSubmitError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeCommitSubmitError")
            .field("state", &self.transaction.pager_commit_state())
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

struct Request<T: TransactionHandle> {
    transaction: T,
    now_unix_ns: u64,
    reply: oneshot::SendPermit<NativeCommitCompletion<T>>,
    outcome: Option<std::result::Result<(), Arc<FrankenError>>>,
}

impl<T: TransactionHandle> Request<T> {
    fn finish(self, stopped: &Arc<FrankenError>) {
        let state = self.transaction.pager_commit_state();
        let result = if state == PagerCommitState::Committed {
            // A worker/drop boundary cannot revoke a completed publication.
            Ok(())
        } else if let Some(Err(error)) = self.outcome {
            Err(error)
        } else if state.retains_commit_obligation() {
            Err(Arc::new(FrankenError::BusyRecovery))
        } else {
            Err(Arc::clone(stopped))
        };
        // The reply permit was reserved before acceptance. Completing it is
        // synchronous and does not consult a now-cancelled client/worker Cx.
        // A disconnected client releases only its own result/handle; it does
        // not turn an indeterminate operation into a rollback.
        let _undelivered = self.reply.send(NativeCommitCompletion {
            transaction: self.transaction,
            result,
        });
    }
}

/// Cloneable producer for one native pager's automatic commit worker.
///
/// Mailbox capacity bounds queued requests and reserved unsent slots. At most
/// `max_batch` additional requests are held by the worker. Caller-owned handles,
/// waiting producers, and completed tickets are not covered by that bound;
/// page overlays also retain the native page store's own resource limits.
pub struct NativeCommitSender<T: TransactionHandle> {
    sender: mpsc::Sender<Request<T>>,
    closing: Arc<AtomicBool>,
}

impl<T: TransactionHandle> Clone for NativeCommitSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            closing: Arc::clone(&self.closing),
        }
    }
}

impl<T: TransactionHandle> NativeCommitSender<T> {
    /// Wait for a mailbox slot without borrowing or consuming a transaction.
    /// Dropping/cancelling this future releases its reservation/waiter only.
    /// Use the caller's existing Asupersync capability context, not a new root.
    ///
    /// # Errors
    /// Returns cancellation, shutdown, or checked runtime-admission failure.
    pub async fn reserve<'a>(&'a self, cx: &'a NativeCx) -> Result<NativeCommitPermit<'a, T>> {
        self.check_open()?;
        let permit = self.sender.reserve_checked(cx).await.map_err(admission_error)?;
        self.check_open()?;
        Ok(NativeCommitPermit {
            permit,
            closing: &self.closing,
            cx,
        })
    }

    /// Nonblocking reservation with the same ownership and admission rules.
    ///
    /// # Errors
    /// Also returns `Busy` when capacity belongs to queued/reserved producers.
    pub fn try_reserve<'a>(&'a self, cx: &'a NativeCx) -> Result<NativeCommitPermit<'a, T>> {
        self.check_open()?;
        let permit = self.sender.try_reserve_checked(cx).map_err(admission_error)?;
        self.check_open()?;
        Ok(NativeCommitPermit {
            permit,
            closing: &self.closing,
            cx,
        })
    }

    fn check_open(&self) -> Result<()> {
        if self.closing.load(Ordering::Acquire) || self.sender.is_closed() {
            return Err(FrankenError::Abort);
        }
        Ok(())
    }

    /// Seal producer admission and wake the worker to drain accepted requests.
    /// Already reserved but unsent permits return their transactions on send.
    /// Accepted intents still commit even when their tickets have been dropped.
    pub fn request_shutdown(&self) {
        self.closing.store(true, Ordering::Release);
        self.sender.wake_receiver();
    }
}

/// A reserved mailbox slot; dropping it never consumes the caller's transaction.
pub struct NativeCommitPermit<'a, T: TransactionHandle> {
    permit: mpsc::SendPermit<'a, Request<T>>,
    closing: &'a AtomicBool,
    cx: &'a NativeCx,
}

impl<T: TransactionHandle> NativeCommitPermit<'_, T> {
    /// Transfer one exact handle to the worker and return its completion ticket.
    /// This is acceptance, NOT a durable acknowledgement. There is no await
    /// between the final admission checks and the channel publication.
    ///
    /// # Errors
    /// Returns the untouched handle for cancellation, shutdown, disconnection,
    /// or reply-obligation admission failure before mailbox publication.
    pub fn send(
        self,
        transaction: T,
        now_unix_ns: u64,
    ) -> std::result::Result<NativeCommitTicket<T>, NativeCommitSubmitError<T>> {
        if self.closing.load(Ordering::Acquire) {
            return Err(NativeCommitSubmitError { transaction, error: FrankenError::Abort });
        }
        let state = transaction.pager_commit_state();
        if state != PagerCommitState::NotCommitted {
            let error = if state == PagerCommitState::Committed {
                FrankenError::Abort
            } else {
                FrankenError::BusyRecovery
            };
            return Err(NativeCommitSubmitError { transaction, error });
        }
        let (reply, receiver) = oneshot::channel();
        let reply = match reply.reserve_checked(self.cx) {
            Ok(reply) => reply,
            Err(error) => {
                let error = match error {
                    oneshot::CheckedSendError::Channel(oneshot::SendError::Cancelled(())) => {
                        FrankenError::Interrupt
                    }
                    other => FrankenError::BackgroundWorkerFailed(other.to_string()),
                };
                return Err(NativeCommitSubmitError { transaction, error });
            }
        };
        if self.closing.load(Ordering::Acquire) {
            return Err(NativeCommitSubmitError { transaction, error: FrankenError::Abort });
        }
        let request = Request { transaction, now_unix_ns, reply, outcome: None };
        match self.permit.try_send(request) {
            Ok(()) => Ok(NativeCommitTicket { receiver }),
            Err(error) => {
                let (request, error) = match error {
                    mpsc::SendError::Disconnected(request) => (request, FrankenError::Abort),
                    mpsc::SendError::Cancelled(request) => (request, FrankenError::Interrupt),
                    mpsc::SendError::Full(request) => (request, FrankenError::Busy),
                };
                Err(NativeCommitSubmitError { transaction: request.transaction, error })
            }
        }
    }
}

/// One accepted commit's reply, independent of how many peers share its syncs.
///
/// Waiting borrows the ticket. Cancelling or dropping a wait future leaves the
/// ticket reusable and does not cancel the accepted commit. Dropping the ticket
/// itself abandons the reply only; reconciliation may require storage recovery.
pub struct NativeCommitTicket<T: TransactionHandle> {
    receiver: oneshot::Receiver<NativeCommitCompletion<T>>,
}

impl<T: TransactionHandle> NativeCommitTicket<T> {
    /// Await one completion using the caller's existing capability context.
    ///
    /// # Errors
    /// A cancelled wait returns `Interrupt` without consuming a stored result.
    /// A closed reply channel is a worker failure, never evidence of rollback.
    pub async fn wait(&mut self, cx: &NativeCx) -> Result<NativeCommitCompletion<T>> {
        self.receiver.recv(cx).await.map_err(|error| match error {
            oneshot::RecvError::Cancelled => FrankenError::Interrupt,
            other => FrankenError::BackgroundWorkerFailed(format!("native commit reply: {other}")),
        })
    }
}

/// Single-consumer worker over the existing native page-group publication path.
///
/// A bounded mailbox gathers independent producers. After the first request it
/// yields one scheduler turn, then drains available peers up to `max_batch`.
/// There is no artificial millisecond delay, timer, spin loop, or hidden task.
/// A batch is not a cross-transaction crash-atomic record: its separate markers
/// may recover as a prefix. The caller must run this future in an owned scope.
pub struct NativeCommitService<S, M, C>
where
    S: VfsFile + Send + Sync,
    M: VfsFile + Send + Sync,
    C: NativeObjectCodec + Send + Sync,
{
    pager: NativePager<S, M, C>,
    receiver: mpsc::Receiver<Request<NativeTransaction<S, M, C>>>,
    closing: Arc<AtomicBool>,
    batch: Vec<Request<NativeTransaction<S, M, C>>>,
    max_batch: usize,
    stopped: Arc<FrankenError>,
}

impl<S, M, C> NativeCommitService<S, M, C>
where
    S: VfsFile + Send + Sync,
    M: VfsFile + Send + Sync,
    C: NativeObjectCodec + Send + Sync,
{
    /// Build a worker and its producer. This does not spawn or close the pager.
    /// The caller retains its original pager for close/recovery obligations.
    ///
    /// # Errors
    /// Refuses zero/excessive bounds or failure to reserve the worker batch.
    pub fn new(
        pager: &NativePager<S, M, C>,
        queue_capacity: usize,
        max_batch: usize,
    ) -> Result<(NativeCommitSender<NativeTransaction<S, M, C>>, Self)> {
        if queue_capacity == 0 || queue_capacity > 1024
            || max_batch == 0 || max_batch > NativePageStore::<S, M, C>::MAX_COMMIT_GROUP
        {
            return Err(FrankenError::TooBig);
        }
        let mut batch = Vec::new();
        batch.try_reserve_exact(max_batch).map_err(|_| FrankenError::OutOfMemory)?;
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let closing = Arc::new(AtomicBool::new(false));
        Ok((
            NativeCommitSender { sender, closing: Arc::clone(&closing) },
            Self {
                pager: pager.clone(), receiver, closing, batch, max_batch,
                stopped: Arc::new(FrankenError::Abort),
            },
        ))
    }

    /// Drive accepted commits until all producers close or request shutdown.
    /// Normal shutdown drains accepted intents; cancellation stops admission
    /// and returns unattempted handles. Dropping even an unpolled worker future
    /// reports queued handles without claiming a rollback of in-flight writes.
    ///
    /// Once physical I/O may have started, no failed group is split or retried.
    /// A purely rejected group may be tried member-by-member so one stale or
    /// oversized writer cannot prevent unrelated valid writers from committing.
    /// Each such attempt revalidates against the newly committed history.
    ///
    /// # Errors
    /// Cancellation or an indeterminate pager stops the worker. Per-transaction
    /// conflicts/resource errors are delivered in tickets rather than killing it.
    pub async fn run(mut self, cx: &Cx) -> Result<()> {
        let native = cx.attached_native_cx().or_else(NativeCx::current).ok_or_else(|| {
            FrankenError::BackgroundWorkerFailed("native commit worker requires the caller runtime context".to_owned())
        })?;
        loop {
            let first = poll_fn(|task_cx| {
                if self.closing.load(Ordering::Acquire) {
                    self.receiver.close();
                }
                if cx.checkpoint().is_err() {
                    return Poll::Ready(Err(mpsc::RecvError::Cancelled));
                }
                self.receiver.poll_recv(&native, task_cx)
            }).await;
            match first {
                Ok(request) => self.batch.push(request),
                Err(mpsc::RecvError::Disconnected) => return Ok(()),
                Err(_) => {
                    self.stopped = Arc::new(FrankenError::Interrupt);
                    return Err(FrankenError::Interrupt);
                }
            }
            yield_once().await;
            while self.batch.len() < self.max_batch {
                match self.receiver.try_recv() {
                    Ok(request) => self.batch.push(request),
                    Err(mpsc::RecvError::Empty | mpsc::RecvError::Disconnected) => break,
                    Err(mpsc::RecvError::Cancelled) => {
                        self.stopped = Arc::new(FrankenError::Interrupt);
                        return Err(FrankenError::Interrupt);
                    }
                }
            }
            if cx.checkpoint().is_err() || native.checkpoint().is_err() {
                self.stopped = Arc::new(FrankenError::Interrupt);
                return Err(FrankenError::Interrupt);
            }
            self.publish(cx).await?;
            let uncertain = self.batch.iter().any(|request| matches!(
                request.transaction.pager_commit_state(),
                PagerCommitState::InDoubt | PagerCommitState::DurableNeedsPublication
            ));
            self.finish_batch();
            if uncertain || matches!(self.pager.needs_recovery(), Ok(true)) {
                self.stopped = Arc::new(FrankenError::BusyRecovery);
                return Err(FrankenError::BusyRecovery);
            }
            // Let producers/readers progress even when the mailbox stays full.
            yield_once().await;
        }
    }

    async fn publish(&mut self, cx: &Cx) -> Result<()> {
        let now = self.batch.iter().map(|request| request.now_unix_ns).max().unwrap_or(0);
        let result = {
            let mut transactions = Vec::new();
            transactions.try_reserve_exact(self.batch.len()).map_err(|_| FrankenError::OutOfMemory)?;
            for request in &mut self.batch {
                transactions.push(&mut request.transaction);
            }
            self.pager.commit_batch_at(cx, &mut transactions, now).await
        };
        let Err(error) = result else {
            for request in &mut self.batch { request.outcome = Some(Ok(())); }
            return Ok(());
        };
        let can_split = self.batch.len() > 1
            && matches!(error, FrankenError::BusySnapshot { .. } | FrankenError::TooBig | FrankenError::Abort)
            && self.batch.iter().all(|request| request.transaction.pager_commit_state() == PagerCommitState::NotCommitted)
            && matches!(self.pager.needs_recovery(), Ok(false));
        if !can_split {
            let error = Arc::new(error);
            for request in &mut self.batch { request.outcome = Some(Err(Arc::clone(&error))); }
            return Ok(());
        }
        for request in &mut self.batch {
            // Always use THIS service's owner, including in the fallback.
            // Calling transaction.commit() here would commit a foreign pager.
            let result = self.pager.commit_batch_at(
                cx, &mut [&mut request.transaction], request.now_unix_ns,
            ).await;
            request.outcome = Some(result.map_err(Arc::new));
            if request.transaction.pager_commit_state() != PagerCommitState::Committed
                && request.transaction.pager_commit_state().retains_commit_obligation()
            {
                self.stopped = Arc::new(FrankenError::BusyRecovery);
                break;
            }
            if cx.checkpoint().is_err() {
                self.stopped = Arc::new(FrankenError::Interrupt);
                break;
            }
        }
        Ok(())
    }

    fn finish_batch(&mut self) {
        for request in self.batch.drain(..) { request.finish(&self.stopped); }
    }
}

impl<S, M, C> Drop for NativeCommitService<S, M, C>
where
    S: VfsFile + Send + Sync,
    M: VfsFile + Send + Sync,
    C: NativeObjectCodec + Send + Sync,
{
    fn drop(&mut self) {
        self.closing.store(true, Ordering::Release);
        self.receiver.close();
        // Handles live in the worker, not in a temporary receive/flush future.
        // On unwind/drop, their authoritative states determine every reply.
        self.finish_batch();
        while let Ok(request) = self.receiver.try_recv() {
            request.finish(&self.stopped);
        }
    }
}

fn admission_error(error: mpsc::CheckedSendError<()>) -> FrankenError {
    match error {
        mpsc::CheckedSendError::Channel(mpsc::SendError::Full(())) => FrankenError::Busy,
        mpsc::CheckedSendError::Channel(mpsc::SendError::Cancelled(())) => FrankenError::Interrupt,
        mpsc::CheckedSendError::Channel(mpsc::SendError::Disconnected(())) => FrankenError::Abort,
        other => FrankenError::BackgroundWorkerFailed(other.to_string()),
    }
}

async fn yield_once() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded { Poll::Ready(()) } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }).await;
}
