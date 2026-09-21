//! One lazy busy-timeout window shared by a statement's retry layers.
//!
//! Each statement future owns a `Cell<BusyTimeoutState>`. Install it only for
//! the duration of a poll with `BusyTimeoutScope::enter_poll`, not across an
//! await. Nested polls inherit the parent's window, and a first contention in
//! a child propagates back to its parent. Independent interleaved futures keep
//! separate windows. No allocation or clock read is required until contention.
//!
//! This bounds retry waiting, not total execution or mandatory rollback work.
//! The caller must still make its initial admission attempt when the timeout
//! is zero and must retain its existing error/idempotence/cancellation checks.

use std::cell::Cell;
use std::time::Duration;

use fsqlite_types::sync_primitives::Instant;

/// A start time and duration avoid overflow from adding a large timeout to now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BusyTimeoutWindow {
    pub(super) started: Instant,
    pub(super) timeout: Duration,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) enum BusyTimeoutState {
    #[default]
    Unstarted,
    Started(BusyTimeoutWindow),
}

impl BusyTimeoutState {
    /// An already-started parent is authoritative. An unstarted parent adopts
    /// a child's window, including a child suspended by an earlier poll.
    const fn inherit(self, child: Self) -> Self {
        match self {
            Self::Unstarted => child,
            Self::Started(_) => self,
        }
    }
}

/// The connection's currently-polled statement, absent between polls.
#[derive(Debug, Default)]
pub(super) struct BusyTimeoutScope {
    active: Cell<Option<BusyTimeoutState>>,
}

impl BusyTimeoutScope {
    pub(super) fn enter_poll<'a>(
        &'a self,
        owned: &'a Cell<BusyTimeoutState>,
    ) -> BusyTimeoutPollGuard<'a> {
        let previous = self.active.get();
        let state = previous.map_or_else(|| owned.get(), |parent| parent.inherit(owned.get()));
        self.active.set(Some(state));
        BusyTimeoutPollGuard {
            scope: self,
            owned,
            previous,
        }
    }

    /// Called only after a retryable failure. Once started, later layers use
    /// the original start AND timeout, rather than granting a fresh duration.
    pub(super) fn window(&self, timeout: Duration) -> BusyTimeoutWindow {
        self.window_with(timeout, Instant::now)
    }

    fn window_with(&self, timeout: Duration, now: impl FnOnce() -> Instant) -> BusyTimeoutWindow {
        let active = self.active.get();
        if let Some(BusyTimeoutState::Started(window)) = active {
            return window;
        }
        let window = BusyTimeoutWindow {
            started: now(),
            timeout,
        };
        if active.is_some() {
            self.active.set(Some(BusyTimeoutState::Started(window)));
        }
        // Bootstrap and other non-statement callers retain a local window;
        // they must not leave a deadline installed on the connection.
        window
    }
}

/// Restore the connection slot on Ready, Pending, early return, or unwind.
/// A suspended future retains its own state without installing it globally.
#[derive(Debug)]
#[must_use = "the guard must remain alive for the duration of the statement poll"]
pub(super) struct BusyTimeoutPollGuard<'a> {
    scope: &'a BusyTimeoutScope,
    owned: &'a Cell<BusyTimeoutState>,
    previous: Option<BusyTimeoutState>,
}

impl Drop for BusyTimeoutPollGuard<'_> {
    fn drop(&mut self) {
        // No panicking assertion in Drop: restoration must work during unwind.
        let observed = self.scope.active.get().unwrap_or_default();
        self.owned.set(observed);
        self.scope
            .active
            .set(self.previous.map(|parent| parent.inherit(observed)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{Future, poll_fn};
    use std::task::{Context, Poll, Waker};

    fn remaining(window: BusyTimeoutWindow, now: Instant) -> Duration {
        window
            .timeout
            .saturating_sub(now.saturating_duration_since(window.started))
    }

    #[test]
    fn uncontended_polls_do_not_start_a_budget() {
        let scope = BusyTimeoutScope::default();
        let owned = Cell::new(BusyTimeoutState::Unstarted);
        {
            let _poll = scope.enter_poll(&owned);
            assert_eq!(scope.active.get(), Some(BusyTimeoutState::Unstarted));
        }
        assert_eq!(owned.get(), BusyTimeoutState::Unstarted);
        assert_eq!(scope.active.get(), None);
    }

    #[test]
    fn first_contention_freezes_the_window_for_later_layers() {
        let scope = BusyTimeoutScope::default();
        let owned = Cell::new(BusyTimeoutState::Unstarted);
        let start = Instant::now();
        let _poll = scope.enter_poll(&owned);
        let first = scope.window_with(Duration::from_millis(250), || start);
        let later = scope.window_with(Duration::from_secs(10), || {
            panic!("an already-started retry must not ask for a new start time")
        });
        assert_eq!(later, first);
        assert_eq!(later.timeout, Duration::from_millis(250));
    }

    #[test]
    fn publication_then_admission_share_the_remaining_budget() {
        let scope = BusyTimeoutScope::default();
        let outer = Cell::new(BusyTimeoutState::Unstarted);
        let inner = Cell::new(BusyTimeoutState::Unstarted);
        let start = Instant::now();
        let timeout = Duration::from_millis(1_000);
        let _outer_poll = scope.enter_poll(&outer);
        let publication = scope.window_with(timeout, || start);
        {
            let _inner_poll = scope.enter_poll(&inner);
            let admission = scope.window_with(timeout, || start + Duration::from_millis(700));
            assert_eq!(admission, publication);
            assert_eq!(
                remaining(admission, start + Duration::from_millis(700)),
                Duration::from_millis(300)
            );
            assert_eq!(remaining(admission, start + timeout), Duration::ZERO);
        }
        assert_eq!(scope.window_with(timeout, || start + timeout), publication);
    }

    #[test]
    fn admission_exhaustion_does_not_rearm_outer_retry_at_either_budget() {
        for timeout in [Duration::from_millis(250), Duration::from_millis(1_000)] {
            let scope = BusyTimeoutScope::default();
            let outer = Cell::new(BusyTimeoutState::Unstarted);
            let inner = Cell::new(BusyTimeoutState::Unstarted);
            let start = Instant::now();
            let _outer_poll = scope.enter_poll(&outer);
            let admission;
            {
                let _inner_poll = scope.enter_poll(&inner);
                admission = scope.window_with(timeout, || start);
            }
            let retry = scope.window_with(timeout, || start + timeout);
            assert_eq!(retry, admission);
            assert_eq!(remaining(retry, start + timeout), Duration::ZERO);
        }
    }

    #[test]
    fn interleaved_statement_polls_keep_independent_deadlines() {
        let scope = BusyTimeoutScope::default();
        let first = Cell::new(BusyTimeoutState::Unstarted);
        let second = Cell::new(BusyTimeoutState::Unstarted);
        let start = Instant::now();
        let timeout = Duration::from_millis(250);
        let first_window = {
            let _poll = scope.enter_poll(&first);
            scope.window_with(timeout, || start)
        };
        assert_eq!(scope.active.get(), None);
        let second_window = {
            let _poll = scope.enter_poll(&second);
            scope.window_with(timeout, || start + Duration::from_millis(200))
        };
        assert_eq!(scope.active.get(), None);
        {
            let _poll = scope.enter_poll(&first);
            let resumed = scope.window_with(timeout, || start + Duration::from_millis(300));
            assert_eq!(resumed, first_window);
            assert_eq!(
                remaining(resumed, start + Duration::from_millis(300)),
                Duration::ZERO
            );
        }
        {
            let _poll = scope.enter_poll(&second);
            let resumed = scope.window_with(timeout, || start + Duration::from_millis(300));
            assert_eq!(resumed, second_window);
            assert_eq!(
                remaining(resumed, start + Duration::from_millis(300)),
                Duration::from_millis(150)
            );
        }
        assert_eq!(scope.active.get(), None);
    }

    #[test]
    fn pending_future_cancellation_does_not_leak_a_deadline() {
        let scope = BusyTimeoutScope::default();
        let owned = Cell::new(BusyTimeoutState::Unstarted);
        let start = Instant::now();
        let timeout = Duration::from_millis(250);
        let mut future = Box::pin(poll_fn(|_| {
            let _poll = scope.enter_poll(&owned);
            let _ = scope.window_with(timeout, || start);
            Poll::<()>::Pending
        }));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert_eq!(scope.active.get(), None);
        drop(future);
        let next = Cell::new(BusyTimeoutState::Unstarted);
        let _poll = scope.enter_poll(&next);
        let fresh = scope.window_with(timeout, || start + timeout);
        assert_eq!(fresh.started, start + timeout);
        assert_eq!(remaining(fresh, start + timeout), timeout);
    }

    #[test]
    fn ready_future_restores_the_connection_slot() {
        let scope = BusyTimeoutScope::default();
        let owned = Cell::new(BusyTimeoutState::Unstarted);
        let start = Instant::now();
        let timeout = Duration::from_millis(250);
        let mut future = Box::pin(poll_fn(|_| {
            let _poll = scope.enter_poll(&owned);
            Poll::Ready(scope.window_with(timeout, || start))
        }));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_ready());
        assert_eq!(scope.active.get(), None);
        let next = Cell::new(BusyTimeoutState::Unstarted);
        let _poll = scope.enter_poll(&next);
        let fresh = scope.window_with(timeout, || start + timeout);
        assert_eq!(fresh.started, start + timeout);
    }

    #[test]
    fn unwinding_restores_the_slot_and_preserves_parent_contention() {
        let scope = BusyTimeoutScope::default();
        let outer = Cell::new(BusyTimeoutState::Unstarted);
        let inner = Cell::new(BusyTimeoutState::Unstarted);
        let start = Instant::now();
        let timeout = Duration::from_millis(250);
        {
            let _outer_poll = scope.enter_poll(&outer);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _inner_poll = scope.enter_poll(&inner);
                let _ = scope.window_with(timeout, || start);
                panic!("simulated dispatch panic");
            }));
            assert!(result.is_err());
            assert_eq!(scope.window_with(timeout, || start + timeout).started, start);
        }
        assert_eq!(scope.active.get(), None);
        let next = Cell::new(BusyTimeoutState::Unstarted);
        let _poll = scope.enter_poll(&next);
        assert_eq!(
            scope.window_with(timeout, || start + timeout).started,
            start + timeout
        );
    }

    #[test]
    fn zero_timeout_is_exhausted_without_a_second_window() {
        let scope = BusyTimeoutScope::default();
        let owned = Cell::new(BusyTimeoutState::Unstarted);
        let start = Instant::now();
        let _poll = scope.enter_poll(&owned);
        let window = scope.window_with(Duration::ZERO, || start);
        assert_eq!(remaining(window, start), Duration::ZERO);
        assert_eq!(scope.window_with(Duration::from_secs(1), || start), window);
    }

    #[test]
    fn large_timeout_does_not_require_absolute_deadline_addition() {
        let scope = BusyTimeoutScope::default();
        let owned = Cell::new(BusyTimeoutState::Unstarted);
        let start = Instant::now();
        let _poll = scope.enter_poll(&owned);
        let window = scope.window_with(Duration::MAX, || start);
        assert_eq!(remaining(window, start), Duration::MAX);
        assert_eq!(
            remaining(window, start + Duration::from_secs(1)),
            Duration::MAX - Duration::from_secs(1)
        );
    }

    #[test]
    fn calls_outside_a_statement_do_not_install_shared_state() {
        let scope = BusyTimeoutScope::default();
        let start = Instant::now();
        let timeout = Duration::from_millis(250);
        let first = scope.window_with(timeout, || start);
        let second = scope.window_with(timeout, || start + timeout);
        assert_ne!(first.started, second.started);
        assert_eq!(scope.active.get(), None);
        assert_eq!(scope.window(timeout).timeout, timeout);
        assert_eq!(scope.active.get(), None);
    }

    #[test]
    fn unstarted_parent_adopts_a_previously_suspended_child_window() {
        let scope = BusyTimeoutScope::default();
        let parent = Cell::new(BusyTimeoutState::Unstarted);
        let child = Cell::new(BusyTimeoutState::Unstarted);
        let start = Instant::now();
        let timeout = Duration::from_millis(250);
        let first = {
            let _poll = scope.enter_poll(&child);
            scope.window_with(timeout, || start)
        };
        {
            let _parent_poll = scope.enter_poll(&parent);
            {
                let _child_poll = scope.enter_poll(&child);
                assert_eq!(scope.window_with(timeout, || start + timeout), first);
            }
            assert_eq!(scope.window_with(timeout, || start + timeout), first);
        }
        assert_eq!(parent.get(), BusyTimeoutState::Started(first));
        assert_eq!(scope.active.get(), None);
    }
}
