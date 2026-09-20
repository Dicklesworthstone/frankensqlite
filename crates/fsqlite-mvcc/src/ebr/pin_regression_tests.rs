//! Regression coverage for the lifetime of a reader's snapshot retention bound.

use std::sync::Arc;

use fsqlite_types::CommitSeq;

use super::{StaleReaderConfig, VersionGuard, VersionGuardRegistry, VersionGuardTicket};
use crate::core_types::{ReaderPinCommitSeq, raise_gc_horizon_with_reader_clamp};

#[test]
fn ebr_pin_repeated_registration_cannot_advance_live_snapshot() {
    let registry = Arc::new(VersionGuardRegistry::default());
    let guard = VersionGuard::pin(Arc::clone(&registry));
    guard.set_pinned_commit_seq(5);
    for sequence in [5, 6, 100, u64::MAX] {
        guard.set_pinned_commit_seq(sequence);
        assert_eq!(registry.min_pinned_commit_seq(), Some(5));
        let pins = registry.all_reader_pins();
        assert_eq!(pins.len(), 1);
        assert_eq!(pins[0].pinned_commit_seq, Some(5));
    }

    let pins: Vec<_> = registry
        .all_reader_pins()
        .into_iter()
        .map(|pin| ReaderPinCommitSeq {
            guard_id: pin.guard_id,
            pinned_commit_seq: pin.pinned_commit_seq.map(CommitSeq::new),
            pinned_for: pin.pinned_for,
        })
        .collect();
    let raised =
        raise_gc_horizon_with_reader_clamp(&[], CommitSeq::new(0), CommitSeq::new(100), &pins, 1);
    assert_eq!(raised.new_horizon, CommitSeq::new(4));
    drop(guard);
    assert_eq!(registry.min_pinned_commit_seq(), None);
}

#[test]
fn ebr_pin_older_registration_tightens_but_abort_does_not_remove_it() {
    let registry = Arc::new(VersionGuardRegistry::new(StaleReaderConfig {
        warn_after: std::time::Duration::ZERO,
        ..StaleReaderConfig::default()
    }));
    let older = VersionGuardTicket::register(Arc::clone(&registry));
    let newer = VersionGuardTicket::register(Arc::clone(&registry));
    older.set_pinned_commit_seq(40);
    newer.set_pinned_commit_seq(30);
    older.set_pinned_commit_seq(7);
    assert!(registry.mark_force_abort(older.guard_id()));
    older.set_pinned_commit_seq(90);
    assert!(older.is_force_aborted());
    assert_eq!(registry.min_pinned_commit_seq(), Some(7));
    let stale = registry.stale_reader_snapshots(fsqlite_types::sync_primitives::Instant::now());
    assert!(stale.iter().any(|pin| {
        pin.guard_id == older.guard_id() && pin.pinned_commit_seq == Some(7) && pin.force_abort
    }));
    drop(older);
    assert_eq!(registry.min_pinned_commit_seq(), Some(30));
    drop(newer);
    assert_eq!(registry.min_pinned_commit_seq(), None);
}

#[test]
fn ebr_pin_zero_and_maximum_sequences_are_real_values_not_sentinels() {
    let registry = Arc::new(VersionGuardRegistry::default());
    let ticket = VersionGuardTicket::register(Arc::clone(&registry));
    assert_eq!(registry.min_pinned_commit_seq(), None);
    ticket.set_pinned_commit_seq(u64::MAX);
    assert_eq!(registry.min_pinned_commit_seq(), Some(u64::MAX));
    ticket.set_pinned_commit_seq(0);
    ticket.set_pinned_commit_seq(u64::MAX);
    assert_eq!(registry.min_pinned_commit_seq(), Some(0));
}

#[test]
fn ebr_pin_late_registration_cannot_resurrect_a_dropped_guard() {
    let registry = Arc::new(VersionGuardRegistry::default());
    let ticket = VersionGuardTicket::register(Arc::clone(&registry));
    let id = ticket.guard_id();
    ticket.set_pinned_commit_seq(1);
    drop(ticket);
    registry.set_pinned_commit_seq(id, 0);
    assert_eq!(registry.active_guard_count(), 0);
    assert_eq!(registry.min_pinned_commit_seq(), None);
    assert!(registry.all_reader_pins().is_empty());
}

#[test]
fn ebr_pin_concurrent_ticket_updates_preserve_oldest_declaration() {
    let registry = Arc::new(VersionGuardRegistry::default());
    let ticket = Arc::new(VersionGuardTicket::register(Arc::clone(&registry)));
    ticket.set_pinned_commit_seq(5);
    std::thread::scope(|scope| {
        for worker in 0..8_u64 {
            let ticket = Arc::clone(&ticket);
            let registry = Arc::clone(&registry);
            scope.spawn(move || {
                for step in 0..128_u64 {
                    ticket.set_pinned_commit_seq(100 + worker * 128 + step);
                    assert_eq!(registry.min_pinned_commit_seq(), Some(5));
                }
            });
        }
    });
    assert_eq!(registry.active_guard_count(), 1);
    assert_eq!(registry.min_pinned_commit_seq(), Some(5));
    drop(ticket);
    assert_eq!(registry.active_guard_count(), 0);
}
