//! Cancellation for reversible pager admission and retry waits.

use std::future::Future;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;

/// Race a reversible admission wait against the explicit operation's local
/// cancellation. Dropping a pending native lock future retires its waiter;
/// mandatory settlement and physical writes must not use this helper.
pub(crate) async fn await_pager_admission<F: Future>(cx: &Cx, wait: F) -> Result<F::Output> {
    let mut cancellation = std::pin::pin!(cx.wait_for_local_cancellation());
    let mut wait = std::pin::pin!(wait);
    std::future::poll_fn(|task_cx| {
        if cx.checkpoint().is_err() {
            return std::task::Poll::Ready(Err(FrankenError::Abort));
        }
        if let std::task::Poll::Ready(output) = wait.as_mut().poll(task_cx) {
            return std::task::Poll::Ready(
                cx.checkpoint()
                    .map(|()| output)
                    .map_err(|_| FrankenError::Abort),
            );
        }
        // Uncontended locks need no local waiter registration. Once the
        // native future suspends, register the operation's local relay and
        // recheck: cancellation between its poll and registration cannot be
        // lost, and cancellation after the recheck wakes this task.
        let _ = cancellation.as_mut().poll(task_cx);
        if cx.checkpoint().is_err() {
            std::task::Poll::Ready(Err(FrankenError::Abort))
        } else {
            std::task::Poll::Pending
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake, Waker};

    use fsqlite_types::cx::CancelReason;

    use super::*;

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Default)]
    struct Gate {
        ready: AtomicBool,
        polls: AtomicUsize,
        waker: Mutex<Option<Waker>>,
    }

    impl Gate {
        fn release(&self) {
            self.ready.store(true, Ordering::Release);
            let waker = self.waker.lock().expect("gate waker").take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    struct GatedWait(Arc<Gate>);

    impl Future for GatedWait {
        type Output = usize;

        fn poll(self: Pin<&mut Self>, task_cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.0.polls.fetch_add(1, Ordering::Relaxed);
            if self.0.ready.load(Ordering::Acquire) {
                Poll::Ready(42)
            } else {
                *self.0.waker.lock().expect("gate waker") = Some(task_cx.waker().clone());
                Poll::Pending
            }
        }
    }

    struct AcquiredGuard(Arc<AtomicUsize>);

    impl Drop for AcquiredGuard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn local_cancellation_wakes_pending_admission() {
        let cx = Cx::new();
        let relay = cx.local_cancel_relay();
        let gate = Arc::new(Gate::default());
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let mut task_cx = Context::from_waker(&waker);
        let mut admission = Box::pin(await_pager_admission(&cx, GatedWait(Arc::clone(&gate))));

        assert!(admission.as_mut().poll(&mut task_cx).is_pending());
        assert_eq!(gate.polls.load(Ordering::Relaxed), 1);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
        assert!(relay.cancel_local(CancelReason::UserInterrupt));
        assert!(
            wakes.0.load(Ordering::Relaxed) > 0,
            "the local relay must wake a waiter whose underlying operation is still pending"
        );
        assert!(matches!(
            admission.as_mut().poll(&mut task_cx),
            Poll::Ready(Err(FrankenError::Abort))
        ));
        assert!(!gate.ready.load(Ordering::Acquire));
        assert_eq!(gate.polls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn precancelled_admission_does_not_poll_wait() {
        let cx = Cx::new();
        assert!(cx.local_cancel_relay().cancel_local(CancelReason::UserInterrupt));
        let polls = AtomicUsize::new(0);
        let wait = std::future::poll_fn(|_| {
            polls.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(())
        });
        let mut admission = Box::pin(await_pager_admission(&cx, wait));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(
            admission.as_mut().poll(&mut task_cx),
            Poll::Ready(Err(FrankenError::Abort))
        ));
        assert_eq!(polls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn masked_cancellation_allows_pending_admission_to_finish() {
        let cx = Cx::new();
        let mask = cx.masked();
        let gate = Arc::new(Gate::default());
        let mut admission = Box::pin(await_pager_admission(&cx, GatedWait(Arc::clone(&gate))));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(admission.as_mut().poll(&mut task_cx).is_pending());
        assert!(cx.local_cancel_relay().cancel_local(CancelReason::UserInterrupt));
        assert!(admission.as_mut().poll(&mut task_cx).is_pending());
        gate.release();
        assert!(matches!(
            admission.as_mut().poll(&mut task_cx),
            Poll::Ready(Ok(42))
        ));
        drop(admission);
        assert!(cx.checkpoint().is_ok());
        drop(mask);
        assert!(cx.checkpoint().is_err());
    }

    #[test]
    fn uncancelled_admission_preserves_wait_and_output() {
        let cx = Cx::new();
        let gate = Arc::new(Gate::default());
        let mut admission = Box::pin(await_pager_admission(&cx, GatedWait(Arc::clone(&gate))));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(admission.as_mut().poll(&mut task_cx).is_pending());
        assert_eq!(gate.polls.load(Ordering::Relaxed), 1);
        gate.release();
        assert!(matches!(
            admission.as_mut().poll(&mut task_cx),
            Poll::Ready(Ok(42))
        ));
        assert_eq!(gate.polls.load(Ordering::Relaxed), 2);
        assert!(cx.checkpoint().is_ok());
    }

    #[test]
    fn cancellation_at_completion_releases_acquired_guard() {
        let cx = Cx::new();
        let relay = cx.local_cancel_relay();
        let drops = Arc::new(AtomicUsize::new(0));
        let wait = std::future::poll_fn(|_| {
            assert!(relay.cancel_local(CancelReason::UserInterrupt));
            Poll::Ready(AcquiredGuard(Arc::clone(&drops)))
        });
        let mut admission = Box::pin(await_pager_admission(&cx, wait));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(
            admission.as_mut().poll(&mut task_cx),
            Poll::Ready(Err(FrankenError::Abort))
        ));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        drop(admission);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn cancellation_during_first_poll_is_not_lost() {
        let cx = Cx::new();
        let relay = cx.local_cancel_relay();
        let wait = std::future::poll_fn(|_| {
            assert!(relay.cancel_local(CancelReason::UserInterrupt));
            Poll::<()>::Pending
        });
        let mut admission = Box::pin(await_pager_admission(&cx, wait));
        let mut task_cx = Context::from_waker(Waker::noop());

        assert!(matches!(
            admission.as_mut().poll(&mut task_cx),
            Poll::Ready(Err(FrankenError::Abort))
        ));
    }
}
