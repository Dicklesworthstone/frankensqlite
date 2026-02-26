//! Transaction state machine (§12.10, bd-7pxb).
//!
//! Implements BEGIN/COMMIT/ROLLBACK with four transaction modes (DEFERRED,
//! IMMEDIATE, EXCLUSIVE, CONCURRENT) and a LIFO savepoint stack.

use std::collections::HashMap;

use fsqlite_ast::TransactionMode;
use fsqlite_error::{FrankenError, Result};
use tracing::{debug, error, info};

// ---------------------------------------------------------------------------
// Lock state
// ---------------------------------------------------------------------------

/// SQLite-compatible lock level for the transaction state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockLevel {
    /// No lock held.
    None,
    /// Shared lock (readers).
    Shared,
    /// Reserved lock (pending writer).
    Reserved,
    /// Exclusive lock (active writer, blocks readers in rollback journal mode).
    Exclusive,
}

// ---------------------------------------------------------------------------
// Transaction state
// ---------------------------------------------------------------------------

/// Current state of a connection's transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    /// No active transaction (autocommit mode).
    Idle,
    /// Transaction is active.
    Active,
    /// Transaction is in error state (needs ROLLBACK).
    Error,
}

// ---------------------------------------------------------------------------
// Savepoint
// ---------------------------------------------------------------------------

/// A savepoint on the LIFO stack.
///
/// RELEASE X commits work since SAVEPOINT X and removes X and all later
/// savepoints. ROLLBACK TO X undoes work since X but leaves X on the stack.
#[derive(Debug, Clone)]
pub struct SavepointEntry {
    /// User-visible savepoint name.
    pub name: String,
    /// Write-set snapshot (page_number → data copy) for partial rollback.
    write_set_snapshot: HashMap<u64, Vec<u8>>,
}

// ---------------------------------------------------------------------------
// TransactionController
// ---------------------------------------------------------------------------

/// Manages the transaction lifecycle for a single connection.
///
/// Tracks the current transaction mode, lock level, and savepoint stack.
/// This is the "SQL layer" state machine; the underlying MVCC machinery
/// lives in `fsqlite_mvcc::lifecycle::TransactionManager`.
#[derive(Debug)]
pub struct TransactionController {
    /// Current transaction state.
    state: TxnState,
    /// Transaction mode (set at BEGIN time).
    mode: Option<TransactionMode>,
    /// Current lock level.
    lock_level: LockLevel,
    /// LIFO savepoint stack.
    savepoints: Vec<SavepointEntry>,
    /// Write-set tracking for savepoint rollback support.
    write_set: HashMap<u64, Vec<u8>>,
    /// Whether we are in CONCURRENT (MVCC) mode.
    concurrent: bool,
    /// Whether the transaction was implicitly started by a SAVEPOINT.
    implicit_txn: bool,
}

impl TransactionController {
    /// Create a new transaction controller in idle state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: TxnState::Idle,
            mode: None,
            lock_level: LockLevel::None,
            savepoints: Vec::new(),
            write_set: HashMap::new(),
            concurrent: false,
            implicit_txn: false,
        }
    }

    /// Current transaction state.
    #[must_use]
    pub const fn state(&self) -> TxnState {
        self.state
    }

    /// Current lock level.
    #[must_use]
    pub const fn lock_level(&self) -> LockLevel {
        self.lock_level
    }

    /// Current transaction mode.
    #[must_use]
    pub const fn mode(&self) -> Option<TransactionMode> {
        self.mode
    }

    /// Whether we are in CONCURRENT (MVCC) mode.
    #[must_use]
    pub const fn is_concurrent(&self) -> bool {
        self.concurrent
    }

    /// Number of savepoints on the stack.
    #[must_use]
    pub fn savepoint_depth(&self) -> usize {
        self.savepoints.len()
    }

    // -----------------------------------------------------------------------
    // BEGIN
    // -----------------------------------------------------------------------

    /// Begin a transaction with the given mode.
    ///
    /// # Errors
    /// Returns `FrankenError::Busy` if a transaction is already active.
    pub fn begin(&mut self, mode: Option<TransactionMode>) -> Result<()> {
        if self.state != TxnState::Idle {
            error!(
                begin_mode = ?mode,
                "BEGIN failed: transaction already active"
            );
            return Err(FrankenError::Busy);
        }

        let resolved_mode = mode.unwrap_or(TransactionMode::Deferred);

        // Acquire locks based on mode.
        let (lock, concurrent) = match resolved_mode {
            TransactionMode::Deferred => {
                // DEFERRED: no lock until first read/write.
                (LockLevel::None, false)
            }
            TransactionMode::Immediate => {
                // IMMEDIATE: acquire RESERVED lock immediately.
                (LockLevel::Reserved, false)
            }
            TransactionMode::Exclusive => {
                // EXCLUSIVE: acquire EXCLUSIVE lock immediately.
                (LockLevel::Exclusive, false)
            }
            TransactionMode::Concurrent => {
                // CONCURRENT: enter MVCC concurrent writer mode with snapshot.
                (LockLevel::Shared, true)
            }
        };

        self.state = TxnState::Active;
        self.mode = Some(resolved_mode);
        self.lock_level = lock;
        self.concurrent = concurrent;
        self.write_set.clear();

        info!(
            begin_mode = ?resolved_mode,
            lock_level = ?lock,
            concurrent,
            "transaction started"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // COMMIT / END
    // -----------------------------------------------------------------------

    /// Commit the active transaction.
    ///
    /// END TRANSACTION is a synonym for COMMIT (invariant #5).
    ///
    /// # Errors
    /// Returns error if no transaction is active or if in error state.
    pub fn commit(&mut self) -> Result<()> {
        match self.state {
            TxnState::Idle => {
                return Err(FrankenError::NoActiveTransaction);
            }
            TxnState::Error => {
                error!("COMMIT failed: transaction is in error state, must ROLLBACK");
                return Err(FrankenError::Busy);
            }
            TxnState::Active => {}
        }

        info!(
            mode = ?self.mode,
            savepoint_depth = self.savepoints.len(),
            "commit"
        );

        self.reset();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // ROLLBACK
    // -----------------------------------------------------------------------

    /// Roll back the active transaction, undoing all changes since BEGIN.
    ///
    /// # Errors
    /// Returns error if no transaction is active.
    pub fn rollback(&mut self) -> Result<()> {
        if self.state == TxnState::Idle {
            return Err(FrankenError::NoActiveTransaction);
        }

        info!(
            mode = ?self.mode,
            savepoint_depth = self.savepoints.len(),
            "rollback"
        );

        self.reset();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // SAVEPOINT
    // -----------------------------------------------------------------------

    /// Create a named savepoint (pushes onto LIFO stack).
    ///
    /// If no transaction is active, implicitly starts a DEFERRED transaction
    /// (per SQLite semantics: SAVEPOINT outside a transaction starts one).
    #[allow(clippy::needless_pass_by_value)]
    pub fn savepoint(&mut self, name: String) -> Result<()> {
        if self.state == TxnState::Idle {
            self.begin(Some(TransactionMode::Deferred))?;
            self.implicit_txn = true;
        }

        let entry = SavepointEntry {
            name: name.clone(),
            write_set_snapshot: self.write_set.clone(),
        };
        self.savepoints.push(entry);

        debug!(
            savepoint = %name,
            depth = self.savepoints.len(),
            "savepoint created"
        );

        Ok(())
    }

    /// RELEASE savepoint: commits all work since SAVEPOINT X and removes
    /// X and all more recent savepoints from the stack (invariant #6).
    ///
    /// # Errors
    /// Returns error if the named savepoint is not on the stack.
    pub fn release(&mut self, name: &str) -> Result<()> {
        let pos = self.find_savepoint(name)?;

        // Remove the named savepoint and all more recent ones.
        let removed = self.savepoints.len() - pos;
        self.savepoints.truncate(pos);

        debug!(
            savepoint = %name,
            removed,
            remaining = self.savepoints.len(),
            "savepoint released"
        );

        // If releasing the last savepoint and we implicitly began a
        // transaction, commit it.
        if self.savepoints.is_empty() && self.state == TxnState::Active && self.implicit_txn {
            // Per SQLite: RELEASE of the outermost savepoint is equivalent to COMMIT.
            self.commit()?;
        }

        Ok(())
    }

    /// ROLLBACK TO savepoint: undoes all work since SAVEPOINT X but
    /// leaves X on the stack for further use (invariant #7).
    ///
    /// # Errors
    /// Returns error if the named savepoint is not on the stack.
    pub fn rollback_to(&mut self, name: &str) -> Result<()> {
        let pos = self.find_savepoint(name)?;

        // Remove all savepoints more recent than X (but keep X itself).
        self.savepoints.truncate(pos + 1);

        // Restore write set to the snapshot taken when X was created.
        let sp = &self.savepoints[pos];
        self.write_set = sp.write_set_snapshot.clone();

        // If we were in error state, ROLLBACK TO clears it.
        if self.state == TxnState::Error {
            self.state = TxnState::Active;
        }

        info!(
            savepoint = %name,
            depth = self.savepoints.len(),
            "rollback to savepoint"
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Write-set tracking (for savepoint rollback)
    // -----------------------------------------------------------------------

    /// Record a page write in the write set (for savepoint rollback support).
    pub fn record_write(&mut self, page_number: u64, data: Vec<u8>) {
        // Only record if not already present (we want the original pre-image).
        self.write_set.entry(page_number).or_insert(data);
    }

    /// Promote lock level on first read (DEFERRED → SHARED) or first write
    /// (SHARED/RESERVED → appropriate level).
    pub fn promote_on_read(&mut self) {
        if self.state == TxnState::Active && self.lock_level == LockLevel::None {
            self.lock_level = LockLevel::Shared;
            debug!("DEFERRED transaction promoted to SHARED on first read");
        }
    }

    /// Promote lock level on first write.
    pub fn promote_on_write(&mut self) {
        if self.state == TxnState::Active {
            match self.lock_level {
                LockLevel::None | LockLevel::Shared => {
                    if self.concurrent {
                        // CONCURRENT mode: stay at SHARED, use page-level locks.
                        self.lock_level = LockLevel::Shared;
                    } else {
                        self.lock_level = LockLevel::Reserved;
                    }
                    debug!(
                        lock_level = ?self.lock_level,
                        concurrent = self.concurrent,
                        "transaction promoted on first write"
                    );
                }
                LockLevel::Reserved | LockLevel::Exclusive => {
                    // Already at or above RESERVED, no promotion needed.
                }
            }
        }
    }

    /// Mark transaction as in error state (e.g., after a constraint violation).
    pub fn set_error(&mut self) {
        if self.state == TxnState::Active {
            self.state = TxnState::Error;
            error!("transaction entered error state");
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Find a savepoint by name (case-insensitive, searches from top of stack).
    fn find_savepoint(&self, name: &str) -> Result<usize> {
        for (i, sp) in self.savepoints.iter().enumerate().rev() {
            if sp.name.eq_ignore_ascii_case(name) {
                return Ok(i);
            }
        }
        Err(FrankenError::internal(format!("no such savepoint: {name}")))
    }

    /// Reset all transaction state back to idle.
    fn reset(&mut self) {
        self.state = TxnState::Idle;
        self.mode = None;
        self.lock_level = LockLevel::None;
        self.savepoints.clear();
        self.write_set.clear();
        self.concurrent = false;
        self.implicit_txn = false;
    }
}

impl Default for TransactionController {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // === Test 1: BEGIN DEFERRED ===
    #[test]
    fn test_begin_deferred() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Deferred)).unwrap();
        assert_eq!(tc.state(), TxnState::Active);
        // DEFERRED: no lock until first read/write (invariant #1).
        assert_eq!(tc.lock_level(), LockLevel::None);
    }

    // === Test 2: BEGIN IMMEDIATE ===
    #[test]
    fn test_begin_immediate() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Immediate)).unwrap();
        assert_eq!(tc.state(), TxnState::Active);
        // IMMEDIATE: RESERVED lock immediately (invariant #2).
        assert_eq!(tc.lock_level(), LockLevel::Reserved);
    }

    // === Test 3: BEGIN EXCLUSIVE ===
    #[test]
    fn test_begin_exclusive() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Exclusive)).unwrap();
        assert_eq!(tc.state(), TxnState::Active);
        // EXCLUSIVE: EXCLUSIVE lock immediately (invariant #3).
        assert_eq!(tc.lock_level(), LockLevel::Exclusive);
    }

    // === Test 4: BEGIN CONCURRENT ===
    #[test]
    fn test_begin_concurrent() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Concurrent)).unwrap();
        assert_eq!(tc.state(), TxnState::Active);
        // CONCURRENT: enters MVCC mode (invariant #4).
        assert!(tc.is_concurrent());
        assert_eq!(tc.lock_level(), LockLevel::Shared);
    }

    // === Test 5: CONCURRENT no conflict (two controllers, different pages) ===
    #[test]
    fn test_concurrent_no_conflict() {
        let mut tc1 = TransactionController::new();
        let mut tc2 = TransactionController::new();

        tc1.begin(Some(TransactionMode::Concurrent)).unwrap();
        tc2.begin(Some(TransactionMode::Concurrent)).unwrap();

        // Writer 1 modifies page 1.
        tc1.promote_on_write();
        tc1.record_write(1, vec![0xAA; 4096]);

        // Writer 2 modifies page 2 (different page, no conflict).
        tc2.promote_on_write();
        tc2.record_write(2, vec![0xBB; 4096]);

        // Both commit successfully.
        tc1.commit().unwrap();
        tc2.commit().unwrap();
    }

    // === Test 6: CONCURRENT page conflict detection ===
    // Note: Full page-level conflict detection with SQLITE_BUSY_SNAPSHOT
    // requires the MVCC TransactionManager from fsqlite-mvcc. This test
    // verifies the state machine correctly tracks concurrent mode.
    #[test]
    fn test_concurrent_page_conflict() {
        let mut tc1 = TransactionController::new();
        let mut tc2 = TransactionController::new();

        tc1.begin(Some(TransactionMode::Concurrent)).unwrap();
        tc2.begin(Some(TransactionMode::Concurrent)).unwrap();

        assert!(tc1.is_concurrent());
        assert!(tc2.is_concurrent());

        // Both write to the same page — conflict detection would happen at
        // the MVCC layer (TransactionManager). Here we verify state tracking.
        tc1.record_write(1, vec![0xAA; 4096]);
        tc2.record_write(1, vec![0xBB; 4096]);

        // In the full system, tc2.commit() would return SQLITE_BUSY_SNAPSHOT.
        // At this layer, both commits succeed; the MVCC layer enforces conflicts.
        tc1.commit().unwrap();
        tc2.commit().unwrap();
    }

    // === Test 7: END TRANSACTION is synonym for COMMIT (invariant #5) ===
    #[test]
    fn test_commit_end_synonym() {
        let mut tc = TransactionController::new();
        tc.begin(None).unwrap();
        assert_eq!(tc.state(), TxnState::Active);
        // COMMIT and END are the same operation.
        tc.commit().unwrap();
        assert_eq!(tc.state(), TxnState::Idle);
    }

    // === Test 8: ROLLBACK undoes all changes ===
    #[test]
    fn test_rollback() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Immediate)).unwrap();
        tc.record_write(1, vec![0xAA; 100]);
        tc.rollback().unwrap();
        assert_eq!(tc.state(), TxnState::Idle);
        assert_eq!(tc.lock_level(), LockLevel::None);
    }

    // === Test 9: SAVEPOINT creates named savepoint ===
    #[test]
    fn test_savepoint_basic() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Deferred)).unwrap();
        tc.savepoint("sp1".to_owned()).unwrap();
        assert_eq!(tc.savepoint_depth(), 1);
    }

    // === Test 10: RELEASE commits work and removes savepoint ===
    #[test]
    fn test_savepoint_release() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Immediate)).unwrap();
        tc.savepoint("sp1".to_owned()).unwrap();
        tc.record_write(1, vec![0xAA; 100]);
        tc.release("sp1").unwrap();
        // Savepoint removed.
        assert_eq!(tc.savepoint_depth(), 0);
    }

    // === Test 11: RELEASE X removes X and all more recent savepoints (invariant #6) ===
    #[test]
    fn test_savepoint_release_removes_later() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Immediate)).unwrap();
        tc.savepoint("sp1".to_owned()).unwrap();
        tc.savepoint("sp2".to_owned()).unwrap();
        tc.savepoint("sp3".to_owned()).unwrap();
        assert_eq!(tc.savepoint_depth(), 3);

        // RELEASE sp1 removes sp1, sp2, sp3.
        tc.release("sp1").unwrap();
        assert_eq!(tc.savepoint_depth(), 0);
    }

    // === Test 12: ROLLBACK TO undoes work since savepoint but preserves it (invariant #7) ===
    #[test]
    fn test_savepoint_rollback_to() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Immediate)).unwrap();
        tc.savepoint("sp1".to_owned()).unwrap();
        tc.record_write(1, vec![0xAA; 100]);
        tc.rollback_to("sp1").unwrap();
        // Savepoint still on stack.
        assert_eq!(tc.savepoint_depth(), 1);
    }

    // === Test 13: Multiple nested savepoints form a stack ===
    #[test]
    fn test_savepoint_nested() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Immediate)).unwrap();
        tc.savepoint("sp1".to_owned()).unwrap();
        tc.savepoint("sp2".to_owned()).unwrap();
        tc.savepoint("sp3".to_owned()).unwrap();
        assert_eq!(tc.savepoint_depth(), 3);

        // ROLLBACK TO sp2 removes sp3 but keeps sp1 and sp2.
        tc.rollback_to("sp2").unwrap();
        assert_eq!(tc.savepoint_depth(), 2);
    }

    // === Test 14: After ROLLBACK TO, further operations within same scope ===
    #[test]
    fn test_savepoint_rollback_then_continue() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Immediate)).unwrap();
        tc.savepoint("sp1".to_owned()).unwrap();
        tc.record_write(1, vec![0xAA; 100]);
        tc.rollback_to("sp1").unwrap();

        // Can continue operating after ROLLBACK TO.
        tc.record_write(2, vec![0xBB; 100]);
        tc.commit().unwrap();
        assert_eq!(tc.state(), TxnState::Idle);
    }

    // === Test: DEFERRED lock promotion ===
    #[test]
    fn test_deferred_lock_promotion() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Deferred)).unwrap();
        assert_eq!(tc.lock_level(), LockLevel::None);

        // First read promotes to SHARED.
        tc.promote_on_read();
        assert_eq!(tc.lock_level(), LockLevel::Shared);

        // First write promotes to RESERVED.
        tc.promote_on_write();
        assert_eq!(tc.lock_level(), LockLevel::Reserved);
    }

    // === Test: Error state requires ROLLBACK ===
    #[test]
    fn test_error_state_requires_rollback() {
        let mut tc = TransactionController::new();
        tc.begin(None).unwrap();
        tc.set_error();
        assert_eq!(tc.state(), TxnState::Error);

        // COMMIT should fail in error state.
        assert!(tc.commit().is_err());

        // ROLLBACK succeeds.
        tc.rollback().unwrap();
        assert_eq!(tc.state(), TxnState::Idle);
    }

    // === Test: Cannot begin within a transaction ===
    #[test]
    fn test_begin_within_transaction() {
        let mut tc = TransactionController::new();
        tc.begin(None).unwrap();
        assert!(tc.begin(None).is_err());
    }

    // === Test: SAVEPOINT outside transaction starts one ===
    #[test]
    fn test_savepoint_starts_transaction() {
        let mut tc = TransactionController::new();
        assert_eq!(tc.state(), TxnState::Idle);
        tc.savepoint("sp1".to_owned()).unwrap();
        assert_eq!(tc.state(), TxnState::Active);
        assert_eq!(tc.savepoint_depth(), 1);
        tc.release("sp1").unwrap();
        assert_eq!(tc.state(), TxnState::Idle);
    }

    // === Test: Explicit transaction does not commit on outermost release ===
    #[test]
    fn test_savepoint_explicit_transaction_no_commit_on_release() {
        let mut tc = TransactionController::new();
        tc.begin(Some(TransactionMode::Deferred)).unwrap();
        tc.savepoint("sp1".to_owned()).unwrap();
        assert_eq!(tc.state(), TxnState::Active);
        tc.release("sp1").unwrap();
        assert_eq!(tc.state(), TxnState::Active); // Remains active
        tc.commit().unwrap();
        assert_eq!(tc.state(), TxnState::Idle);
    }

    // === Test: ROLLBACK TO clears error state ===
    #[test]
    fn test_rollback_to_clears_error() {
        let mut tc = TransactionController::new();
        tc.begin(None).unwrap();
        tc.savepoint("sp1".to_owned()).unwrap();
        tc.set_error();
        assert_eq!(tc.state(), TxnState::Error);
        tc.rollback_to("sp1").unwrap();
        assert_eq!(tc.state(), TxnState::Active);
    }
}
