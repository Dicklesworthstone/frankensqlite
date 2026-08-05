//! Obligation (linear resource) tracking for cancellation-safe two-phase protocols (§4.13).
//!
//! Every reserved obligation MUST reach a terminal state (`Committed` or `Aborted`).
//! Non-terminal drop from `Reserved` = `Leaked` = correctness bug (INV-NO-OBLIGATION-LEAKS).

use std::collections::VecDeque;
use std::fmt::Write as FmtWrite;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Mode (lab vs production)
// ---------------------------------------------------------------------------

/// Controls how obligation leaks are reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObligationMode {
    /// Leak = test failure (panic). §4.13.2 lab default.
    Lab,
    /// Leak = diagnostic bundle + connection close. §4.13.2 production default.
    Production,
}

// ---------------------------------------------------------------------------
// Obligation kind + state
// ---------------------------------------------------------------------------

/// The five normative FrankenSQLite obligation types (§4.13).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObligationKind {
    /// Commit pipeline `SendPermit` reservation (two-phase MPSC).
    SendPermit,
    /// Reply obligation on oneshot/session replies.
    CommitResponse,
    /// Transaction slot lease (abort on expiry).
    TxnSlot,
    /// Witness-plane reservation token (reserve/commit for publication).
    WitnessReservation,
    /// Name/registration in shared state (deregister on crash).
    SharedStateRegistration,
}

/// Observable state of an obligation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObligationState {
    /// Initial state after reservation.
    Reserved,
    /// Terminal: obligation fulfilled.
    Committed,
    /// Terminal: obligation explicitly released.
    Aborted,
    /// Terminal (bug): dropped without resolution.
    Leaked,
}

impl ObligationState {
    /// Whether this is a terminal state.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::Aborted | Self::Leaked)
    }
}

// ---------------------------------------------------------------------------
// Obligation
// ---------------------------------------------------------------------------

/// A tracked linear resource that MUST reach a terminal state.
///
/// Dropping an `Obligation` in `Reserved` state triggers leak detection:
/// - Lab mode: panics with diagnostic message.
/// - Production mode: records the leak in the ledger.
pub struct Obligation {
    id: u64,
    kind: ObligationKind,
    state: ObligationState,
    created_at: String,
    mode: ObligationMode,
    ledger: Option<Arc<ObligationLedger>>,
}

impl std::fmt::Debug for Obligation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Obligation")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("state", &self.state)
            .field("created_at", &self.created_at)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl Obligation {
    /// Reserve a new obligation (enters `Reserved` state).
    #[must_use]
    pub fn reserve(
        kind: ObligationKind,
        mode: ObligationMode,
        created_at: impl Into<String>,
        ledger: Option<Arc<ObligationLedger>>,
    ) -> Self {
        let id = ledger
            .as_ref()
            .map_or(0, |l| l.next_id.fetch_add(1, Ordering::Relaxed));
        let created = created_at.into();
        if let Some(ref l) = ledger {
            l.record_reserve(id, kind, &created);
        }
        Self {
            id,
            kind,
            state: ObligationState::Reserved,
            created_at: created,
            mode,
            ledger,
        }
    }

    /// Commit this obligation (terminal state).
    pub fn commit(&mut self) {
        assert_eq!(
            self.state,
            ObligationState::Reserved,
            "obligation {} ({:?}): commit called on non-Reserved state {:?}",
            self.id,
            self.kind,
            self.state,
        );
        self.state = ObligationState::Committed;
        if let Some(ref ledger) = self.ledger {
            ledger.record_terminal(self.id, ObligationState::Committed);
        }
    }

    /// Abort this obligation (terminal state).
    pub fn abort(&mut self) {
        assert_eq!(
            self.state,
            ObligationState::Reserved,
            "obligation {} ({:?}): abort called on non-Reserved state {:?}",
            self.id,
            self.kind,
            self.state,
        );
        self.state = ObligationState::Aborted;
        if let Some(ref ledger) = self.ledger {
            ledger.record_terminal(self.id, ObligationState::Aborted);
        }
    }

    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }

    #[must_use]
    pub fn kind(&self) -> ObligationKind {
        self.kind
    }

    #[must_use]
    pub fn state(&self) -> ObligationState {
        self.state
    }

    #[must_use]
    pub fn created_at(&self) -> &str {
        &self.created_at
    }
}

impl Drop for Obligation {
    fn drop(&mut self) {
        if self.state == ObligationState::Reserved {
            self.state = ObligationState::Leaked;
            if let Some(ref ledger) = self.ledger {
                ledger.record_terminal(self.id, ObligationState::Leaked);
                ledger.record_leak(self.id, self.kind, &self.created_at);
            }
            match self.mode {
                ObligationMode::Lab => {
                    // Panicking during unwinding aborts the process; only raise
                    // a hard failure when we're not already handling a panic.
                    if !std::thread::panicking() {
                        std::panic::panic_any(format!(
                            "obligation leak: {:?} id={} created_at={}",
                            self.kind, self.id, self.created_at
                        ));
                    }
                }
                ObligationMode::Production => {
                    // In production, the leak is recorded in the ledger.
                    // The caller (connection/region) is responsible for checking
                    // the ledger and closing the affected connection.
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Obligation Ledger
// ---------------------------------------------------------------------------

/// Entry in the obligation ledger.
#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub id: u64,
    pub kind: ObligationKind,
    pub state: ObligationState,
    pub created_at: String,
}

/// Diagnostic record for a leaked obligation.
#[derive(Debug, Clone)]
pub struct LeakRecord {
    pub id: u64,
    pub kind: ObligationKind,
    pub created_at: String,
}

/// Global tracker of all outstanding obligations for diagnostic dumps.
pub struct ObligationLedger {
    entries: Mutex<Vec<LedgerEntry>>,
    leaks: Mutex<Vec<LeakRecord>>,
    next_id: AtomicU64,
}

impl std::fmt::Debug for ObligationLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObligationLedger")
            .field("next_id", &self.next_id.load(Ordering::Relaxed))
            .field("entries_count", &self.snapshot().len())
            .field("leaks_count", &self.leaked().len())
            .finish_non_exhaustive()
    }
}

impl Default for ObligationLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl ObligationLedger {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            leaks: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(0),
        }
    }

    fn record_reserve(&self, id: u64, kind: ObligationKind, created_at: &str) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.push(LedgerEntry {
            id,
            kind,
            state: ObligationState::Reserved,
            created_at: created_at.to_owned(),
        });
    }

    fn record_terminal(&self, id: u64, state: ObligationState) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
            entry.state = state;
        }
    }

    fn record_leak(&self, id: u64, kind: ObligationKind, created_at: &str) {
        let mut leaks = self
            .leaks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        leaks.push(LeakRecord {
            id,
            kind,
            created_at: created_at.to_owned(),
        });
    }

    /// Snapshot of all ledger entries.
    #[must_use]
    pub fn snapshot(&self) -> Vec<LedgerEntry> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Leaked obligations recorded so far.
    #[must_use]
    pub fn leaked(&self) -> Vec<LeakRecord> {
        self.leaks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Count of entries by state.
    #[must_use]
    pub fn count_by_state(&self, state: ObligationState) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|e| e.state == state)
            .count()
    }

    /// Produce a diagnostic dump string.
    #[must_use]
    pub fn diagnostic_dump(&self) -> String {
        let entries = self.snapshot();
        let leaks = self.leaked();
        let mut out = String::new();
        let _ = writeln!(out, "=== Obligation Ledger Dump ===");
        let _ = writeln!(out, "Total entries: {}", entries.len());
        let _ = writeln!(
            out,
            "Committed: {}",
            entries
                .iter()
                .filter(|e| e.state == ObligationState::Committed)
                .count()
        );
        let _ = writeln!(
            out,
            "Aborted: {}",
            entries
                .iter()
                .filter(|e| e.state == ObligationState::Aborted)
                .count()
        );
        let _ = writeln!(out, "Leaked: {}", leaks.len());
        for leak in &leaks {
            let _ = writeln!(
                out,
                "  LEAK id={} kind={:?} created_at={}",
                leak.id, leak.kind, leak.created_at
            );
        }
        out
    }
}

// ---------------------------------------------------------------------------
// TrackedSender (§4.13.1)
// ---------------------------------------------------------------------------

/// A sender that wraps a channel and holds an obligation.
///
/// Sending a value commits the obligation. Dropping without sending triggers
/// leak detection per the obligation's mode.
pub struct TrackedSender<T> {
    obligation: Option<Obligation>,
    sender: Option<std::sync::mpsc::Sender<T>>,
}

impl<T> std::fmt::Debug for TrackedSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackedSender")
            .field("obligation", &self.obligation)
            .field("has_sender", &self.sender.is_some())
            .finish()
    }
}

impl<T> TrackedSender<T> {
    /// Create a tracked sender wrapping an `mpsc::Sender` with an obligation.
    #[must_use]
    pub fn new(
        sender: std::sync::mpsc::Sender<T>,
        kind: ObligationKind,
        mode: ObligationMode,
        created_at: impl Into<String>,
        ledger: Option<Arc<ObligationLedger>>,
    ) -> Self {
        let obligation = Obligation::reserve(kind, mode, created_at, ledger);
        Self {
            obligation: Some(obligation),
            sender: Some(sender),
        }
    }

    /// Send a value, committing the obligation.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the receiver has been dropped.
    pub fn send(mut self, value: T) -> Result<(), std::sync::mpsc::SendError<T>> {
        let sender = self
            .sender
            .take()
            .expect("TrackedSender: sender already consumed");
        let result = sender.send(value);
        if result.is_ok()
            && let Some(ref mut ob) = self.obligation
        {
            ob.commit();
        }
        result
    }

    /// Explicitly abort this sender's obligation without sending.
    pub fn abort(mut self) {
        if let Some(ref mut ob) = self.obligation {
            ob.abort();
        }
    }
}

impl<T> Drop for TrackedSender<T> {
    fn drop(&mut self) {
        // If the obligation is still Some and Reserved, Obligation::drop will
        // handle leak detection.
    }
}

// ---------------------------------------------------------------------------
// EvictChannel — non-critical channel with send_evict_oldest (§4.13.1)
// ---------------------------------------------------------------------------

/// A bounded channel that evicts the oldest message when full.
///
/// For non-critical telemetry channels only. Safety-critical channels MUST NOT
/// use this; use `TrackedSender` instead.
pub struct EvictChannel<T> {
    buffer: Mutex<VecDeque<T>>,
    capacity: usize,
    evicted: AtomicU64,
}

impl<T: std::fmt::Debug> std::fmt::Debug for EvictChannel<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvictChannel")
            .field("capacity", &self.capacity)
            .field("evicted", &self.evicted.load(Ordering::Relaxed))
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<T> EvictChannel<T> {
    /// Create a new evict channel with the given capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            evicted: AtomicU64::new(0),
        }
    }

    /// Send a value, evicting the oldest if at capacity.
    pub fn send_evict_oldest(&self, value: T) {
        let mut buf = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buf.len() >= self.capacity {
            buf.pop_front();
            self.evicted.fetch_add(1, Ordering::Relaxed);
        }
        buf.push_back(value);
    }

    /// Receive the oldest message, if any.
    pub fn recv(&self) -> Option<T> {
        let mut buf = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        buf.pop_front()
    }

    /// Number of evictions that have occurred.
    #[must_use]
    pub fn eviction_count(&self) -> u64 {
        self.evicted.load(Ordering::Relaxed)
    }

    /// Current number of buffered messages.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Whether the channel is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    const BEAD_ID: &str = "bd-3j1j";

    fn make_ledger() -> Arc<ObligationLedger> {
        Arc::new(ObligationLedger::new())
    }

    #[test]
    fn test_obligation_commit_reaches_terminal() {
        // Test 1: SendPermit commit → Committed, no leak.
        let ledger = make_ledger();
        let mut ob = Obligation::reserve(
            ObligationKind::SendPermit,
            ObligationMode::Lab,
            "test_commit",
            Some(Arc::clone(&ledger)),
        );
        ob.commit();
        assert_eq!(
            ob.state(),
            ObligationState::Committed,
            "bead_id={BEAD_ID} commit_terminal"
        );
        drop(ob);
        assert_eq!(
            ledger.count_by_state(ObligationState::Committed),
            1,
            "bead_id={BEAD_ID} ledger_shows_committed"
        );
        assert!(ledger.leaked().is_empty(), "bead_id={BEAD_ID} no_leaks");
    }

    #[test]
    fn test_obligation_abort_reaches_terminal() {
        // Test 2: TxnSlot abort → Aborted, no leak.
        let ledger = make_ledger();
        let mut ob = Obligation::reserve(
            ObligationKind::TxnSlot,
            ObligationMode::Lab,
            "test_abort",
            Some(Arc::clone(&ledger)),
        );
        ob.abort();
        assert_eq!(
            ob.state(),
            ObligationState::Aborted,
            "bead_id={BEAD_ID} abort_terminal"
        );
        drop(ob);
        assert_eq!(ledger.count_by_state(ObligationState::Aborted), 1);
        assert!(ledger.leaked().is_empty());
    }

    #[test]
    #[should_panic(expected = "obligation leak")]
    fn test_obligation_leak_panics_in_lab() {
        // Test 3: Witness reservation dropped without resolution panics in lab.
        let _ob = Obligation::reserve(
            ObligationKind::WitnessReservation,
            ObligationMode::Lab,
            "test_leak_lab",
            None,
        );
        // Drop without commit or abort → panic.
    }

    #[test]
    fn test_obligation_leak_diagnostic_in_production() {
        // Test 4: Production mode — drop without resolution records leak,
        // no panic, diagnostic bundle available.
        let ledger = make_ledger();
        let leaked_flag = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&leaked_flag);

        // Create and immediately drop an obligation in production mode.
        {
            let _ob = Obligation::reserve(
                ObligationKind::CommitResponse,
                ObligationMode::Production,
                "test_leak_prod",
                Some(Arc::clone(&ledger)),
            );
            // Drop without resolution — no panic in production.
        }

        // Verify leak was recorded.
        let leaks = ledger.leaked();
        assert_eq!(leaks.len(), 1, "bead_id={BEAD_ID} production_leak_recorded");
        assert_eq!(leaks[0].kind, ObligationKind::CommitResponse);
        assert_eq!(leaks[0].created_at, "test_leak_prod");

        // Diagnostic dump contains the leak.
        let dump = ledger.diagnostic_dump();
        assert!(
            dump.contains("LEAK"),
            "bead_id={BEAD_ID} diagnostic_contains_leak"
        );
        assert!(dump.contains("test_leak_prod"));

        // Simulate connection close (caller responsibility).
        flag.store(true, Ordering::Release);
        assert!(leaked_flag.load(Ordering::Acquire));
    }

    #[test]
    fn test_tracked_sender_commit_on_send() {
        // Test 5: TrackedSender commit on send.
        let ledger = make_ledger();
        let (tx, rx) = std::sync::mpsc::channel();
        let tracked = TrackedSender::new(
            tx,
            ObligationKind::SendPermit,
            ObligationMode::Lab,
            "test_tracked_send",
            Some(Arc::clone(&ledger)),
        );

        tracked.send(42).expect("send should succeed");
        assert_eq!(rx.recv().unwrap(), 42);
        assert_eq!(
            ledger.count_by_state(ObligationState::Committed),
            1,
            "bead_id={BEAD_ID} tracked_sender_committed"
        );
        assert!(ledger.leaked().is_empty());
    }

    #[test]
    #[should_panic(expected = "obligation leak")]
    fn test_tracked_sender_leak_on_drop() {
        // Test 6: TrackedSender dropped without sending → leak in lab.
        let (tx, _rx) = std::sync::mpsc::channel::<i32>();
        let _tracked = TrackedSender::new(
            tx,
            ObligationKind::SendPermit,
            ObligationMode::Lab,
            "test_tracked_leak",
            None,
        );
        // Drop without send → obligation leak → panic.
    }

    #[test]
    fn test_five_obligation_types_registered() {
        // Test 7: All 5 obligation types committed, 0 leaked in ledger.
        let ledger = make_ledger();
        let kinds = [
            ObligationKind::SendPermit,
            ObligationKind::CommitResponse,
            ObligationKind::TxnSlot,
            ObligationKind::WitnessReservation,
            ObligationKind::SharedStateRegistration,
        ];

        for kind in &kinds {
            let mut ob = Obligation::reserve(
                *kind,
                ObligationMode::Lab,
                format!("test_{kind:?}"),
                Some(Arc::clone(&ledger)),
            );
            ob.commit();
        }

        assert_eq!(
            ledger.count_by_state(ObligationState::Committed),
            5,
            "bead_id={BEAD_ID} all_five_committed"
        );
        assert!(ledger.leaked().is_empty(), "bead_id={BEAD_ID} zero_leaked");
    }

    #[test]
    fn test_obligation_ledger_diagnostic_dump() {
        // Test 8: 3 obligations (1 committed, 1 aborted, 1 leaked) —
        // dump contains exactly 1 leaked entry with creation context.
        let ledger = make_ledger();

        // Committed.
        let mut ob1 = Obligation::reserve(
            ObligationKind::SendPermit,
            ObligationMode::Production,
            "ob1_commit",
            Some(Arc::clone(&ledger)),
        );
        ob1.commit();

        // Aborted.
        let mut ob2 = Obligation::reserve(
            ObligationKind::TxnSlot,
            ObligationMode::Production,
            "ob2_abort",
            Some(Arc::clone(&ledger)),
        );
        ob2.abort();

        // Leaked (production mode — no panic).
        {
            let _ob3 = Obligation::reserve(
                ObligationKind::WitnessReservation,
                ObligationMode::Production,
                "ob3_leaked_from_line_42",
                Some(Arc::clone(&ledger)),
            );
        }

        let dump = ledger.diagnostic_dump();
        assert!(
            dump.contains("Committed: 1"),
            "bead_id={BEAD_ID} dump_committed_count"
        );
        assert!(
            dump.contains("Aborted: 1"),
            "bead_id={BEAD_ID} dump_aborted_count"
        );
        assert!(
            dump.contains("Leaked: 1"),
            "bead_id={BEAD_ID} dump_leaked_count"
        );
        assert!(
            dump.contains("ob3_leaked_from_line_42"),
            "bead_id={BEAD_ID} dump_has_creation_context"
        );

        let leaks = ledger.leaked();
        assert_eq!(leaks.len(), 1);
        assert_eq!(leaks[0].kind, ObligationKind::WitnessReservation);
    }

    #[test]
    fn test_cancel_resolves_obligations() {
        // Test 9: Task with 2 obligations cancelled and drained → both Aborted.
        let ledger = make_ledger();

        let mut ob1 = Obligation::reserve(
            ObligationKind::SendPermit,
            ObligationMode::Lab,
            "cancel_ob1",
            Some(Arc::clone(&ledger)),
        );
        let mut ob2 = Obligation::reserve(
            ObligationKind::TxnSlot,
            ObligationMode::Lab,
            "cancel_ob2",
            Some(Arc::clone(&ledger)),
        );

        // Simulate cancellation → drain phase aborts all obligations.
        ob1.abort();
        ob2.abort();

        assert_eq!(ob1.state(), ObligationState::Aborted);
        assert_eq!(ob2.state(), ObligationState::Aborted);
        assert_eq!(ledger.count_by_state(ObligationState::Aborted), 2);
        assert!(
            ledger.leaked().is_empty(),
            "bead_id={BEAD_ID} cancel_no_leaks"
        );
    }

    #[test]
    fn test_non_critical_channel_evict_oldest() {
        // Test 10: Telemetry channel with capacity 2. Send 3 → oldest evicted,
        // no obligation leak.
        let ch = EvictChannel::new(2);

        ch.send_evict_oldest("msg_1");
        ch.send_evict_oldest("msg_2");
        assert_eq!(ch.len(), 2);

        // Third send evicts oldest.
        ch.send_evict_oldest("msg_3");
        assert_eq!(ch.len(), 2);
        assert_eq!(ch.eviction_count(), 1, "bead_id={BEAD_ID} one_eviction");

        // Remaining messages are msg_2, msg_3.
        assert_eq!(ch.recv(), Some("msg_2"));
        assert_eq!(ch.recv(), Some("msg_3"));
        assert!(ch.is_empty());
    }
}
