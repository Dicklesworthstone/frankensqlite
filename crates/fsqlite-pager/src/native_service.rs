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
use std::future::{Future, poll_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use asupersync::Cx as NativeCx;
use asupersync::channel::{mpsc, oneshot};
use asupersync::sync::Notify;
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
    shutdown: Arc<Notify>,
}

impl<T: TransactionHandle> Clone for NativeCommitSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            closing: Arc::clone(&self.closing),
            shutdown: Arc::clone(&self.shutdown),
        }
    }
}

impl<T: TransactionHandle> NativeCommitSender<T> {
    /// Wait for a mailbox slot without borrowing or consuming a transaction.
    /// Dropping/cancelling this future releases its reservation/waiter only.
    /// Use the caller's existing Asupersync capability context, not a new root.
    /// Native cancellation wakes the wait even while the mailbox stays full.
    /// Shutdown also wakes it without waiting for the worker's storage I/O.
    ///
    /// # Errors
    /// Returns cancellation, shutdown, or checked runtime-admission failure.
    pub async fn reserve<'a>(&'a self, cx: &'a NativeCx) -> Result<NativeCommitPermit<'a, T>> {
        self.check_open()?;
        let permit = {
            let mut reservation = std::pin::pin!(self.sender.reserve_checked(cx));
            let mut cancelled = std::pin::pin!(wait_for_native_cancellation(cx));
            // Notify::wait_until arms its generation before checking the flag,
            // so shutdown between that check and waiter registration is not
            // lost. Only a backpressured reservation registers this waiter.
            let mut shutdown = std::pin::pin!(self.shutdown.wait_until(|| {
                self.closing.load(Ordering::Acquire)
            }));
            poll_fn(|task_cx| {
                if let Err(error) = self.check_open() {
                    return Poll::Ready(Err(error));
                }
                match reservation.as_mut().poll(task_cx) {
                    Poll::Ready(result) => Poll::Ready(result.map_err(admission_error)),
                    Poll::Pending => {
                        if shutdown.as_mut().poll(task_cx).is_ready() {
                            Poll::Ready(Err(FrankenError::Abort))
                        } else if cancelled.as_mut().poll(task_cx).is_ready() {
                            Poll::Ready(Err(FrankenError::Interrupt))
                        } else {
                            Poll::Pending
                        }
                    }
                }
            }).await?
        };
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
    /// A send racing this call either returns its handle or joins that drain.
    /// Backpressured producers wake even if the worker has not started or is
    /// awaiting a source-owned write. This never cancels that write or its Cx.
    pub fn request_shutdown(&self) {
        self.closing.store(true, Ordering::Release);
        self.shutdown.notify_waiters();
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
    #[allow(clippy::type_complexity)]
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
            NativeCommitSender {
                sender,
                closing: Arc::clone(&closing),
                shutdown: Arc::new(Notify::new()),
            },
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
    /// An idle worker is woken by either local or attached native cancellation.
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
        // Local Cx cancellation need not cancel the current runtime context
        // used by the mailbox. Register its wakeup independently; checking a
        // flag only on receipt would leave an idle service asleep indefinitely.
        let mut local_cancel = std::pin::pin!(cx.wait_for_local_cancellation());
        let mut native_cancel = std::pin::pin!(wait_for_native_cancellation(&native));
        loop {
            let first = poll_fn(|task_cx| {
                if self.closing.load(Ordering::Acquire) {
                    self.receiver.close();
                }
                if local_cancel.as_mut().poll(task_cx).is_ready() || cx.checkpoint().is_err() {
                    return Poll::Ready(Err(mpsc::RecvError::Cancelled));
                }
                let received = self.receiver.poll_recv(&native, task_cx);
                // Shutdown may race the flag check above before the channel
                // registers its waker. Recheck after Pending: from here the
                // registered waker covers any later shutdown request.
                let received = if received.is_pending() && self.closing.load(Ordering::Acquire) {
                    self.receiver.close();
                    self.receiver.poll_recv(&native, task_cx)
                } else {
                    received
                };
                // MPSC's pending receive does not register a native cancel
                // wake. Do not rely on unrelated mailbox traffic or the
                // polling task sharing this attached context's runtime waker.
                if received.is_pending() && native_cancel.as_mut().poll(task_cx).is_ready() {
                    Poll::Ready(Err(mpsc::RecvError::Cancelled))
                } else {
                    received
                }
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
            self.publish(cx, &native).await?;
            let uncertain = self.batch.iter().any(|request| matches!(
                request.transaction.pager_commit_state(),
                PagerCommitState::InDoubt | PagerCommitState::DurableNeedsPublication
            ));
            if uncertain || matches!(self.pager.needs_recovery(), Ok(true)) {
                self.stopped = Arc::new(FrankenError::BusyRecovery);
                // Reply delivery invokes caller wakers synchronously. Keep
                // the batch owned until Drop seals admission and the mailbox;
                // otherwise an error reply can admit new work to this failed
                // service before its closing flag becomes visible.
                return Err(FrankenError::BusyRecovery);
            }
            if cx.checkpoint().is_err() || native.checkpoint().is_err() {
                // Cancellation can arrive during a successful final barrier
                // or between split members. Seal before invoking any reply
                // waker, but never replace a committed handle's receipt with
                // an interrupt. Uncertainty above takes priority over cancel.
                self.stopped = Arc::new(FrankenError::Interrupt);
                return Err(FrankenError::Interrupt);
            }
            self.finish_batch();
            // Preserve the post-delivery health check too: a healthy reply
            // callback can drive another publisher sharing this pager.
            if matches!(self.pager.needs_recovery(), Ok(true)) {
                self.stopped = Arc::new(FrankenError::BusyRecovery);
                return Err(FrankenError::BusyRecovery);
            }
            // Let producers/readers progress even when the mailbox stays full.
            yield_once().await;
        }
    }

    async fn publish(&mut self, cx: &Cx, native: &NativeCx) -> Result<()> {
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
            // Each fallback member is a new publication attempt. The initial
            // group can reject after cancelling the worker, and an earlier
            // singleton can finish after cancellation. Check the same native
            // context run resolved, even when cx has no native attachment.
            // Never race this check against an already admitted source write.
            if cx.checkpoint().is_err() || native.checkpoint().is_err() {
                self.stopped = Arc::new(FrankenError::Interrupt);
                break;
            }
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

// The pinned Asupersync MPSC waits check cancellation only when polled.
// Its held-open oneshot receive also owns a cancellation-waker registration:
// it rechecks after registration and unregisters exactly its own entry on
// Drop. Use that public cancellation-aware wait without creating a runtime,
// spawning a task, reserving a send obligation, or touching storage. The
// channel is allocated only when a mailbox operation actually returns Pending.
async fn wait_for_native_cancellation(cx: &NativeCx) {
    let (keep_open, mut receiver) = oneshot::channel::<()>();
    let _ = receiver.recv(cx).await;
    drop(keep_open);
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

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_types::{
        COMMIT_MARKER_RECORD_V1_SIZE, CommitSeq, ObjectId, Oti, PageNumber, SymbolRecord,
        SymbolRecordFlags, reconstruct_systematic_happy_path,
    };
    use fsqlite_vfs::{MemoryVfs, Vfs};
    use fsqlite_wal::native_durability::{NativeDurabilityLimits, NativeDurabilityLog};
    use fsqlite_wal::native_pages::NativePageLimits;

    use super::*;
    use crate::TransactionMode;

    // A deterministic scheduling fixture, not an erasure-code or power-loss
    // certificate. A codec is not required to inspect the service's mailbox
    // context; the service must enforce its own cancellation boundary.
    struct CancelOnceCodec {
        source: NativeCx,
        calls: Arc<AtomicUsize>,
        reject_first: bool,
    }

    impl NativeObjectCodec for CancelOnceCodec {
        fn encode(&self, cx: &Cx, bytes: &[u8]) -> Result<Vec<SymbolRecord>> {
            cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.source.cancel_with(asupersync::types::CancelKind::User, None);
                if self.reject_first {
                    return Err(FrankenError::TooBig);
                }
            }
            let size = u32::try_from(bytes.len()).map_err(|_| FrankenError::TooBig)?;
            Ok(vec![SymbolRecord::new(
                ObjectId::derive_from_canonical_bytes(bytes),
                Oti { f: u64::from(size), al: 1, t: size, z: 1, n: 1 },
                0,
                bytes.to_vec(),
                SymbolRecordFlags::SYSTEMATIC_RUN_START,
            )])
        }

        fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
            let bytes = reconstruct_systematic_happy_path(records)
                .map_err(|error| FrankenError::Internal(error.to_string()))?;
            if ObjectId::derive_from_canonical_bytes(&bytes) != id {
                return Err(FrankenError::Abort);
            }
            Ok(bytes)
        }
    }

    fn page(number: u32) -> PageNumber {
        PageNumber::new(number).unwrap()
    }

    fn file(vfs: &MemoryVfs, cx: &Cx, name: &str, create: bool) -> <MemoryVfs as Vfs>::File {
        let mut flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
        if create {
            flags |= VfsOpenFlags::CREATE;
        }
        vfs.open(cx, Some(Path::new(name)), flags).unwrap().0
    }

    #[test]
    fn native_cancel_during_group_rejection_never_starts_split_publication() {
        check_fallback_cancellation(true);
    }

    #[test]
    fn native_cancel_during_split_member_preserves_only_that_committed_receipt() {
        check_fallback_cancellation(false);
    }

    fn check_fallback_cancellation(reject_group: bool) {
        RuntimeBuilder::current_thread()
            .blocking_threads(1, 2)
            .build()
            .unwrap()
            .block_on(async {
                let producer = NativeCx::current().expect("caller runtime");
                let cx = Cx::new();
                cx.set_native_cx(producer.clone());
                let source = NativeCx::for_testing();
                let worker_cx = Cx::new();
                assert!(worker_cx.attached_native_cx().is_none());
                let calls = Arc::new(AtomicUsize::new(0));
                let codec = || CancelOnceCodec {
                    source: source.clone(),
                    calls: Arc::clone(&calls),
                    reject_first: reject_group,
                };
                let vfs = MemoryVfs::new();
                let log = NativeDurabilityLog::create(
                    &cx,
                    file(&vfs, &cx, "objects", true),
                    file(&vfs, &cx, "markers", true),
                    NativeDurabilityLimits::default(),
                ).unwrap();
                let owner = NativePager::new(
                    NativePageStore::new(log, codec(), 512, NativePageLimits::default()).unwrap(),
                ).unwrap();
                // With rejection, the group's first encode cancels the native
                // context and refuses before I/O. Otherwise overlap rejects the
                // group before encoding; its first singleton cancels while
                // running and successfully completes its own publication.
                let pages = if reject_group { [2, 3, 4, 5] } else { [2, 2, 3, 4] };
                let committed = if reject_group { 0 } else { 1 };
                let (sender, worker) = NativeCommitService::new(&owner, 4, 3).unwrap();
                let mut tickets = Vec::new();
                for (index, number) in pages.iter().copied().enumerate() {
                    let mut transaction = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
                    transaction.savepoint(&cx, "private").unwrap();
                    transaction.write_page(
                        &cx, page(number), &[u8::try_from(index + 2).unwrap(); 512],
                    ).await.unwrap();
                    tickets.push(sender.reserve(&producer).await.unwrap().send(transaction, 100).unwrap());
                }
                let mut running = std::pin::pin!(worker.run(&worker_cx));
                let result = poll_fn(|task| {
                    // Install only for this poll, as an executor does. Do not
                    // attach it to worker_cx or cancel the outer test runtime.
                    let _current = NativeCx::set_current(Some(source.clone()));
                    running.as_mut().poll(task)
                }).await;
                assert!(matches!(result, Err(FrankenError::Interrupt)));
                assert!(source.checkpoint().is_err());
                assert!(worker_cx.checkpoint().is_ok(), "storage cancellation is independent");
                assert!(worker_cx.attached_native_cx().is_none());
                assert!(producer.checkpoint().is_ok());
                assert!(NativeCx::current().unwrap().checkpoint().is_ok());
                assert!(!owner.needs_recovery().unwrap());
                assert_eq!(owner.committed_tip().unwrap().get(), committed);
                let encoded = calls.load(Ordering::SeqCst);
                assert_eq!(encoded, if reject_group { 1 } else { 2 });
                assert!(matches!(sender.try_reserve(&producer), Err(FrankenError::Abort)));
                assert!(matches!(owner.close(&cx), Err(FrankenError::Busy)));
                for (index, mut ticket) in tickets.into_iter().enumerate() {
                    let mut completion = ticket.wait(&producer).await.unwrap();
                    let transaction = &mut completion.transaction;
                    assert_eq!(transaction.mode(), TransactionMode::Concurrent);
                    assert_eq!(transaction.published_visible_commit_seq_hint(), Some(CommitSeq::ZERO));
                    if !reject_group && index == 0 {
                        completion.result.as_ref().unwrap();
                        assert_eq!(transaction.pager_commit_state(), PagerCommitState::Committed);
                        assert_eq!(transaction.acknowledgement().unwrap().commit_seq, CommitSeq::new(1));
                        assert!(!transaction.has_pending_writes());
                    } else {
                        assert!(matches!(completion.result.as_ref().unwrap_err().as_ref(), FrankenError::Interrupt));
                        assert_eq!(transaction.pager_commit_state(), PagerCommitState::NotCommitted);
                        assert!(transaction.acknowledgement().is_none());
                        assert_eq!(transaction.pending_commit_pages().unwrap(), [page(pages[index])]);
                        assert_eq!(transaction.get_page(&cx, page(pages[index])).await.unwrap().as_bytes(),
                            &[u8::try_from(index + 2).unwrap(); 512]);
                        transaction.rollback_to_savepoint(&cx, "private").unwrap();
                        assert!(!transaction.has_pending_writes());
                        transaction.rollback(&cx).await.unwrap();
                    }
                }
                assert_eq!(calls.load(Ordering::SeqCst), encoded, "cleanup must not encode or retry");
                let mut markers = file(&vfs, &cx, "markers", false);
                assert_eq!(markers.file_size(&cx).unwrap(),
                    committed * u64::try_from(COMMIT_MARKER_RECORD_V1_SIZE).unwrap());
                markers.close(&cx).unwrap();

                // The worker is terminal, not the pager. New work on the same
                // healthy owner needs no recovery and must not replay any of
                // the returned private overlays.
                let mut next = owner.begin(&cx, TransactionMode::Concurrent).unwrap();
                next.write_page(&cx, page(100), &[91; 512]).await.unwrap();
                let (fresh, worker) = NativeCommitService::new(&owner, 1, 1).unwrap();
                let mut ticket = fresh.reserve(&producer).await.unwrap().send(next, 1000).unwrap();
                fresh.request_shutdown();
                worker.run(&cx).await.unwrap();
                let completed = ticket.wait(&producer).await.unwrap();
                completed.result.unwrap();
                assert_eq!(completed.transaction.acknowledgement().unwrap().commit_seq.get(), committed + 1);
                owner.close(&cx).unwrap();
                let (mut recovered, report) = NativePageStore::recover(
                    &cx,
                    file(&vfs, &cx, "objects", false),
                    file(&vfs, &cx, "markers", false),
                    codec(),
                    512,
                    NativeDurabilityLimits::default(),
                    NativePageLimits::default(),
                ).await.unwrap();
                assert_eq!(report.markers.len(), usize::try_from(committed + 1).unwrap());
                assert_eq!(recovered.committed_tip().get(), committed + 1);
                let mut view = recovered.begin(&cx).unwrap();
                for number in pages {
                    let actual = recovered.read_page(&cx, &mut view, page(number)).unwrap();
                    let expected = if !reject_group && number == 2 { Some(&[2_u8; 512][..]) } else { None };
                    assert_eq!(actual.as_deref(), expected, "unattempted overlays must not be replayed");
                }
                assert_eq!(recovered.read_page(&cx, &mut view, page(100)).unwrap().as_deref(),
                    Some(&[91_u8; 512][..]));
                recovered.rollback(&mut view).unwrap();
                recovered.close(&cx).unwrap();
            });
    }
}
