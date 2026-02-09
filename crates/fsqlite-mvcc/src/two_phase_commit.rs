//! Cross-database two-phase commit protocol (§12.11).
//!
//! When a transaction spans multiple attached databases (all in WAL mode),
//! standard SQLite provides atomicity only within a single database.
//! This module implements a 2PC protocol that ensures cross-database
//! atomic commits using a global commit marker.
//!
//! # Protocol
//!
//! 1. **Phase 1 (Prepare)**: Write WAL frames to all participating databases.
//!    `fsync` each WAL file. Do NOT update WAL-index headers yet.
//! 2. **Global Commit Marker**: Write a durable marker recording all
//!    participating databases and their prepared state.
//! 3. **Phase 2 (Commit)**: Update each database's WAL-index to make frames
//!    visible to readers.
//!
//! # Crash Recovery
//!
//! - Before Phase 1 complete: no effect (partial WAL frames are ignored by readers).
//! - Phase 1 complete, no commit marker: roll back all (WAL frames unreferenced).
//! - Commit marker present, Phase 2 incomplete: roll forward (complete remaining
//!   WAL-index updates on recovery).
//! - Phase 2 complete: normal operation.
//!
//! # Limits
//!
//! SQLite supports up to `main` + `temp` + 10 attached databases = 12 total.

use std::collections::HashMap;

use fsqlite_types::CommitSeq;

/// Maximum number of attached databases per connection (`SQLITE_MAX_ATTACHED`).
pub const SQLITE_MAX_ATTACHED: usize = 10;

/// Maximum total databases: main + temp + `SQLITE_MAX_ATTACHED`.
pub const MAX_TOTAL_DATABASES: usize = SQLITE_MAX_ATTACHED + 2;

/// Identifies a database within a multi-database transaction.
pub type DatabaseId = u32;

/// Reserved database IDs.
pub const MAIN_DB_ID: DatabaseId = 0;
pub const TEMP_DB_ID: DatabaseId = 1;

/// Phase of the 2PC protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TwoPhaseState {
    /// Initial state: no 2PC in progress.
    Idle,
    /// Preparing: writing WAL frames to participant databases.
    Preparing,
    /// All participants have been prepared (WAL frames written + fsynced).
    AllPrepared,
    /// Global commit marker has been written.
    MarkerWritten,
    /// Phase 2 in progress: updating WAL-index headers.
    Committing,
    /// All WAL-index headers updated; 2PC complete.
    Committed,
    /// Protocol aborted (either explicitly or due to failure).
    Aborted,
}

/// Error types for the 2PC protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TwoPhaseError {
    /// Attempted operation in wrong protocol state.
    InvalidState(TwoPhaseState),
    /// A participating database failed during prepare.
    PrepareFailed { db_id: DatabaseId, reason: String },
    /// Too many databases attached (exceeds `SQLITE_MAX_ATTACHED`).
    TooManyDatabases { count: usize, max: usize },
    /// Cannot detach a database with an active transaction.
    DetachWithActiveTransaction { db_id: DatabaseId },
    /// Database is not a participant in this 2PC.
    UnknownDatabase(DatabaseId),
    /// I/O error writing commit marker.
    MarkerWriteError(String),
    /// I/O error during WAL-index update in Phase 2.
    WalIndexUpdateError { db_id: DatabaseId, reason: String },
    /// Database is not in WAL mode (required for 2PC).
    NotWalMode(DatabaseId),
}

impl std::fmt::Display for TwoPhaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidState(state) => write!(f, "2PC invalid state: {state:?}"),
            Self::PrepareFailed { db_id, reason } => {
                write!(f, "2PC prepare failed for db {db_id}: {reason}")
            }
            Self::TooManyDatabases { count, max } => {
                write!(f, "too many databases: {count} exceeds max {max}")
            }
            Self::DetachWithActiveTransaction { db_id } => {
                write!(f, "cannot detach db {db_id}: active transaction")
            }
            Self::UnknownDatabase(db_id) => write!(f, "unknown database: {db_id}"),
            Self::MarkerWriteError(reason) => write!(f, "commit marker write error: {reason}"),
            Self::WalIndexUpdateError { db_id, reason } => {
                write!(f, "WAL-index update error for db {db_id}: {reason}")
            }
            Self::NotWalMode(db_id) => write!(f, "database {db_id} not in WAL mode"),
        }
    }
}

impl std::error::Error for TwoPhaseError {}

/// Prepare result for a single database participant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareResult {
    /// WAL frames written and fsynced successfully.
    Ok {
        /// WAL offset after writing frames.
        wal_offset: u64,
        /// Number of frames written.
        frame_count: u32,
    },
    /// Prepare failed for this database.
    Failed(String),
}

/// Per-database participant state within a 2PC transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantState {
    /// Database identifier.
    pub db_id: DatabaseId,
    /// Schema name (e.g., "main", "temp", "aux1").
    pub schema_name: String,
    /// Whether this database is in WAL mode.
    pub wal_mode: bool,
    /// Prepare result (set during Phase 1).
    pub prepare_result: Option<PrepareResult>,
    /// Whether WAL-index has been updated (Phase 2).
    pub wal_index_updated: bool,
}

impl ParticipantState {
    /// Create a new participant in the initial state.
    #[must_use]
    pub fn new(db_id: DatabaseId, schema_name: String, wal_mode: bool) -> Self {
        Self {
            db_id,
            schema_name,
            wal_mode,
            prepare_result: None,
            wal_index_updated: false,
        }
    }

    /// Whether prepare succeeded for this participant.
    #[must_use]
    pub fn is_prepared(&self) -> bool {
        matches!(self.prepare_result, Some(PrepareResult::Ok { .. }))
    }

    /// Whether Phase 2 is complete for this participant.
    #[must_use]
    pub const fn is_committed(&self) -> bool {
        self.wal_index_updated
    }
}

/// Global commit marker written between Phase 1 and Phase 2.
///
/// This is the durable record that tells crash recovery whether to
/// roll forward (commit marker present) or roll back (no marker).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalCommitMarker {
    /// Magic bytes for marker identification.
    pub magic: [u8; 4],
    /// Unique transaction identifier for this 2PC.
    pub txn_id: u64,
    /// Commit sequence assigned to this cross-database transaction.
    pub commit_seq: CommitSeq,
    /// Participating database IDs and their WAL offsets after prepare.
    pub participants: Vec<(DatabaseId, u64)>,
    /// Timestamp (nanoseconds since Unix epoch) when marker was written.
    pub timestamp_ns: u64,
}

/// Magic bytes for the global commit marker: "2PCM".
pub const COMMIT_MARKER_MAGIC: [u8; 4] = [b'2', b'P', b'C', b'M'];

/// Minimum marker size: magic (4) + txn_id (8) + commit_seq (8) +
/// participant_count (4) + timestamp (8) = 32 bytes.
pub const COMMIT_MARKER_MIN_SIZE: usize = 32;

impl GlobalCommitMarker {
    /// Create a new commit marker for the given transaction.
    #[must_use]
    pub fn new(
        txn_id: u64,
        commit_seq: CommitSeq,
        participants: Vec<(DatabaseId, u64)>,
        timestamp_ns: u64,
    ) -> Self {
        Self {
            magic: COMMIT_MARKER_MAGIC,
            txn_id,
            commit_seq,
            participants,
            timestamp_ns,
        }
    }

    /// Serialize the marker to bytes for durable storage.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let participant_count = u32::try_from(self.participants.len()).unwrap_or(u32::MAX);
        let mut buf = Vec::with_capacity(COMMIT_MARKER_MIN_SIZE + self.participants.len() * 12);
        buf.extend_from_slice(&self.magic);
        buf.extend_from_slice(&self.txn_id.to_le_bytes());
        buf.extend_from_slice(&self.commit_seq.get().to_le_bytes());
        buf.extend_from_slice(&participant_count.to_le_bytes());
        buf.extend_from_slice(&self.timestamp_ns.to_le_bytes());
        for &(db_id, wal_offset) in &self.participants {
            buf.extend_from_slice(&db_id.to_le_bytes());
            buf.extend_from_slice(&wal_offset.to_le_bytes());
        }
        buf
    }

    /// Deserialize a marker from bytes.
    ///
    /// Returns `None` if the buffer is too small or has incorrect magic.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < COMMIT_MARKER_MIN_SIZE {
            return None;
        }
        let magic: [u8; 4] = data[..4].try_into().ok()?;
        if magic != COMMIT_MARKER_MAGIC {
            return None;
        }
        let txn_id = u64::from_le_bytes(data[4..12].try_into().ok()?);
        let commit_seq_raw = u64::from_le_bytes(data[12..20].try_into().ok()?);
        let participant_count = u32::from_le_bytes(data[20..24].try_into().ok()?);
        let timestamp_ns = u64::from_le_bytes(data[24..32].try_into().ok()?);

        let count = usize::try_from(participant_count).ok()?;
        let needed = COMMIT_MARKER_MIN_SIZE + count * 12;
        if data.len() < needed {
            return None;
        }

        let mut participants = Vec::with_capacity(count);
        for i in 0..count {
            let base = COMMIT_MARKER_MIN_SIZE + i * 12;
            let db_id = u32::from_le_bytes(data[base..base + 4].try_into().ok()?);
            let wal_offset = u64::from_le_bytes(data[base + 4..base + 12].try_into().ok()?);
            participants.push((db_id, wal_offset));
        }

        Some(Self {
            magic,
            txn_id,
            commit_seq: CommitSeq::new(commit_seq_raw),
            participants,
            timestamp_ns,
        })
    }

    /// Whether this marker has valid magic bytes.
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        self.magic[0] == b'2'
            && self.magic[1] == b'P'
            && self.magic[2] == b'C'
            && self.magic[3] == b'M'
    }
}

/// Recovery action determined after crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    /// No 2PC state found; nothing to recover.
    NoAction,
    /// Commit marker found, Phase 2 incomplete: roll forward
    /// (complete remaining WAL-index updates).
    RollForward,
    /// Phase 1 frames found but no commit marker: roll back
    /// (WAL frames will be ignored by readers).
    RollBack,
}

/// Coordinator for the cross-database 2PC protocol.
///
/// Manages participant databases, drives the prepare/commit/abort state
/// machine, and handles crash recovery.
#[derive(Debug)]
pub struct TwoPhaseCoordinator {
    /// Current protocol state.
    state: TwoPhaseState,
    /// Participating databases keyed by `DatabaseId`.
    participants: HashMap<DatabaseId, ParticipantState>,
    /// Global commit marker (set after Phase 1 completes).
    commit_marker: Option<GlobalCommitMarker>,
    /// Transaction identifier for this 2PC instance.
    txn_id: u64,
}

impl TwoPhaseCoordinator {
    /// Create a new coordinator for a cross-database transaction.
    #[must_use]
    pub fn new(txn_id: u64) -> Self {
        Self {
            state: TwoPhaseState::Idle,
            participants: HashMap::new(),
            commit_marker: None,
            txn_id,
        }
    }

    /// Current protocol state.
    #[must_use]
    pub const fn state(&self) -> TwoPhaseState {
        self.state
    }

    /// Number of participating databases.
    #[must_use]
    pub fn participant_count(&self) -> usize {
        self.participants.len()
    }

    /// The transaction identifier.
    #[must_use]
    pub const fn txn_id(&self) -> u64 {
        self.txn_id
    }

    /// Register a database as a participant in this 2PC.
    pub fn add_participant(
        &mut self,
        db_id: DatabaseId,
        schema_name: String,
        wal_mode: bool,
    ) -> Result<(), TwoPhaseError> {
        if self.state != TwoPhaseState::Idle {
            return Err(TwoPhaseError::InvalidState(self.state));
        }
        if !wal_mode {
            return Err(TwoPhaseError::NotWalMode(db_id));
        }
        if self.participants.len() >= MAX_TOTAL_DATABASES && !self.participants.contains_key(&db_id)
        {
            return Err(TwoPhaseError::TooManyDatabases {
                count: self.participants.len() + 1,
                max: MAX_TOTAL_DATABASES,
            });
        }
        self.participants
            .insert(db_id, ParticipantState::new(db_id, schema_name, wal_mode));
        Ok(())
    }

    /// Check whether detaching a database is allowed.
    ///
    /// Detaching is forbidden while a 2PC is in progress.
    pub fn check_detach(&self, db_id: DatabaseId) -> Result<(), TwoPhaseError> {
        if self.state != TwoPhaseState::Idle
            && self.state != TwoPhaseState::Committed
            && self.participants.contains_key(&db_id)
        {
            return Err(TwoPhaseError::DetachWithActiveTransaction { db_id });
        }
        Ok(())
    }

    /// Phase 1: Prepare a single participant database.
    ///
    /// In a real implementation, this would write WAL frames and fsync.
    /// Here we record the prepare result for the state machine.
    pub fn prepare_participant(
        &mut self,
        db_id: DatabaseId,
        result: PrepareResult,
    ) -> Result<(), TwoPhaseError> {
        if self.state != TwoPhaseState::Idle && self.state != TwoPhaseState::Preparing {
            return Err(TwoPhaseError::InvalidState(self.state));
        }
        self.state = TwoPhaseState::Preparing;
        let participant = self
            .participants
            .get_mut(&db_id)
            .ok_or(TwoPhaseError::UnknownDatabase(db_id))?;
        participant.prepare_result = Some(result);
        Ok(())
    }

    /// Check whether all participants have been prepared successfully.
    ///
    /// If any participant failed prepare, returns the error.
    pub fn check_all_prepared(&mut self) -> Result<(), TwoPhaseError> {
        if self.state != TwoPhaseState::Preparing {
            return Err(TwoPhaseError::InvalidState(self.state));
        }
        for participant in self.participants.values() {
            match &participant.prepare_result {
                None => {
                    return Err(TwoPhaseError::PrepareFailed {
                        db_id: participant.db_id,
                        reason: "not yet prepared".to_owned(),
                    });
                }
                Some(PrepareResult::Failed(reason)) => {
                    return Err(TwoPhaseError::PrepareFailed {
                        db_id: participant.db_id,
                        reason: reason.clone(),
                    });
                }
                Some(PrepareResult::Ok { .. }) => {}
            }
        }
        self.state = TwoPhaseState::AllPrepared;
        Ok(())
    }

    /// Write the global commit marker.
    ///
    /// Must be called after all participants are prepared.
    /// The marker is the durable decision record: once written, the
    /// protocol is committed and crash recovery will roll forward.
    pub fn write_commit_marker(
        &mut self,
        commit_seq: CommitSeq,
        timestamp_ns: u64,
    ) -> Result<GlobalCommitMarker, TwoPhaseError> {
        if self.state != TwoPhaseState::AllPrepared {
            return Err(TwoPhaseError::InvalidState(self.state));
        }

        let participants: Vec<(DatabaseId, u64)> = self
            .participants
            .values()
            .filter_map(|p| {
                if let Some(PrepareResult::Ok { wal_offset, .. }) = &p.prepare_result {
                    Some((p.db_id, *wal_offset))
                } else {
                    None
                }
            })
            .collect();

        let marker = GlobalCommitMarker::new(self.txn_id, commit_seq, participants, timestamp_ns);
        self.commit_marker = Some(marker.clone());
        self.state = TwoPhaseState::MarkerWritten;
        Ok(marker)
    }

    /// Phase 2: Update WAL-index for a single participant.
    ///
    /// Makes the prepared WAL frames visible to readers.
    pub fn commit_participant(&mut self, db_id: DatabaseId) -> Result<(), TwoPhaseError> {
        if self.state != TwoPhaseState::MarkerWritten && self.state != TwoPhaseState::Committing {
            return Err(TwoPhaseError::InvalidState(self.state));
        }
        self.state = TwoPhaseState::Committing;
        let participant = self
            .participants
            .get_mut(&db_id)
            .ok_or(TwoPhaseError::UnknownDatabase(db_id))?;
        participant.wal_index_updated = true;
        Ok(())
    }

    /// Check whether all participants have completed Phase 2.
    pub fn check_all_committed(&mut self) -> Result<(), TwoPhaseError> {
        if self.state != TwoPhaseState::Committing {
            return Err(TwoPhaseError::InvalidState(self.state));
        }
        for participant in self.participants.values() {
            if !participant.wal_index_updated {
                return Err(TwoPhaseError::WalIndexUpdateError {
                    db_id: participant.db_id,
                    reason: "WAL-index not yet updated".to_owned(),
                });
            }
        }
        self.state = TwoPhaseState::Committed;
        Ok(())
    }

    /// Abort the 2PC protocol.
    ///
    /// Can be called at any point before `Committed`.  Rolls back all
    /// participants (WAL frames without index updates are ignored by readers).
    pub fn abort(&mut self) -> Result<(), TwoPhaseError> {
        if self.state == TwoPhaseState::Committed {
            return Err(TwoPhaseError::InvalidState(self.state));
        }
        self.state = TwoPhaseState::Aborted;
        self.commit_marker = None;
        Ok(())
    }

    /// Determine the recovery action based on crash state.
    ///
    /// Given whether a commit marker was found on disk, returns the
    /// appropriate recovery action.
    #[must_use]
    pub fn determine_recovery(marker_found: bool, all_wal_indices_updated: bool) -> RecoveryAction {
        if !marker_found {
            if all_wal_indices_updated {
                RecoveryAction::NoAction
            } else {
                RecoveryAction::RollBack
            }
        } else if all_wal_indices_updated {
            RecoveryAction::NoAction
        } else {
            RecoveryAction::RollForward
        }
    }

    /// Access the stored commit marker.
    #[must_use]
    pub fn commit_marker(&self) -> Option<&GlobalCommitMarker> {
        self.commit_marker.as_ref()
    }

    /// Check whether this coordinator has completed successfully.
    #[must_use]
    pub const fn is_committed(&self) -> bool {
        matches!(self.state, TwoPhaseState::Committed)
    }

    /// Check whether this coordinator has been aborted.
    #[must_use]
    pub const fn is_aborted(&self) -> bool {
        matches!(self.state, TwoPhaseState::Aborted)
    }
}

#[cfg(test)]
mod tests {
    use fsqlite_types::CommitSeq;

    use super::{
        COMMIT_MARKER_MAGIC, GlobalCommitMarker, MAIN_DB_ID, MAX_TOTAL_DATABASES, PrepareResult,
        RecoveryAction, SQLITE_MAX_ATTACHED, TEMP_DB_ID, TwoPhaseCoordinator, TwoPhaseError,
        TwoPhaseState,
    };

    // -----------------------------------------------------------------------
    // Test 1: Cross-database 2PC succeeds for two databases.
    // -----------------------------------------------------------------------
    #[test]
    fn test_cross_database_two_phase_commit() {
        let mut coord = TwoPhaseCoordinator::new(1);

        // Register main and an attached database.
        coord
            .add_participant(MAIN_DB_ID, "main".to_owned(), true)
            .expect("add main");
        coord
            .add_participant(2, "aux".to_owned(), true)
            .expect("add aux");

        // Phase 1: prepare both.
        coord
            .prepare_participant(
                MAIN_DB_ID,
                PrepareResult::Ok {
                    wal_offset: 4096,
                    frame_count: 2,
                },
            )
            .expect("prepare main");
        coord
            .prepare_participant(
                2,
                PrepareResult::Ok {
                    wal_offset: 8192,
                    frame_count: 3,
                },
            )
            .expect("prepare aux");
        coord.check_all_prepared().expect("all prepared");

        // Write commit marker.
        let marker = coord
            .write_commit_marker(CommitSeq::new(100), 1_000_000)
            .expect("marker");
        assert!(marker.is_valid());
        assert_eq!(marker.participants.len(), 2);

        // Phase 2: commit both.
        coord.commit_participant(MAIN_DB_ID).expect("commit main");
        coord.commit_participant(2).expect("commit aux");
        coord.check_all_committed().expect("all committed");

        assert!(coord.is_committed());
    }

    // -----------------------------------------------------------------------
    // Test 2: Prepare failure on one database aborts all.
    // -----------------------------------------------------------------------
    #[test]
    fn test_cross_db_2pc_one_db_fails_prepare() {
        let mut coord = TwoPhaseCoordinator::new(2);
        coord
            .add_participant(MAIN_DB_ID, "main".to_owned(), true)
            .unwrap();
        coord.add_participant(2, "aux".to_owned(), true).unwrap();

        // Main succeeds, aux fails.
        coord
            .prepare_participant(
                MAIN_DB_ID,
                PrepareResult::Ok {
                    wal_offset: 4096,
                    frame_count: 1,
                },
            )
            .unwrap();
        coord
            .prepare_participant(2, PrepareResult::Failed("disk full".to_owned()))
            .unwrap();

        let result = coord.check_all_prepared();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, TwoPhaseError::PrepareFailed { db_id: 2, .. }),
            "expected prepare failure for db 2: {err:?}"
        );

        // Abort all.
        coord.abort().expect("abort");
        assert!(coord.is_aborted());
    }

    // -----------------------------------------------------------------------
    // Test 3: Attach/detach limit enforcement.
    // -----------------------------------------------------------------------
    #[test]
    fn test_attach_detach_limit() {
        let mut coord = TwoPhaseCoordinator::new(3);

        // Add main + temp + 10 attached = 12 total (the max).
        coord
            .add_participant(MAIN_DB_ID, "main".to_owned(), true)
            .unwrap();
        coord
            .add_participant(TEMP_DB_ID, "temp".to_owned(), true)
            .unwrap();
        for i in 0..SQLITE_MAX_ATTACHED {
            let db_id = u32::try_from(i + 2).expect("fits in u32");
            coord
                .add_participant(db_id, format!("aux{i}"), true)
                .unwrap();
        }
        assert_eq!(coord.participant_count(), MAX_TOTAL_DATABASES);

        // 13th database should fail.
        let result = coord.add_participant(99, "overflow".to_owned(), true);
        assert!(
            matches!(result, Err(TwoPhaseError::TooManyDatabases { .. })),
            "expected too many databases: {result:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: Max attached databases commit atomically.
    // -----------------------------------------------------------------------
    #[test]
    fn test_cross_db_2pc_max_attached() {
        let mut coord = TwoPhaseCoordinator::new(4);

        // Register main + 10 attached databases.
        coord
            .add_participant(MAIN_DB_ID, "main".to_owned(), true)
            .unwrap();
        for i in 0..SQLITE_MAX_ATTACHED {
            let db_id = u32::try_from(i + 2).expect("fits in u32");
            coord
                .add_participant(db_id, format!("aux{i}"), true)
                .unwrap();
        }

        // Prepare all.
        for &db_id in coord.participants.clone().keys() {
            coord
                .prepare_participant(
                    db_id,
                    PrepareResult::Ok {
                        wal_offset: u64::from(db_id) * 4096,
                        frame_count: 1,
                    },
                )
                .unwrap();
        }
        coord.check_all_prepared().unwrap();

        // Write marker and commit all.
        coord
            .write_commit_marker(CommitSeq::new(200), 2_000_000)
            .unwrap();
        for &db_id in &coord.participants.keys().copied().collect::<Vec<_>>() {
            coord.commit_participant(db_id).unwrap();
        }
        coord.check_all_committed().unwrap();
        assert!(coord.is_committed());
    }

    // -----------------------------------------------------------------------
    // Test 5: WAL mode is required for 2PC.
    // -----------------------------------------------------------------------
    #[test]
    fn test_cross_db_2pc_wal_mode_required() {
        let mut coord = TwoPhaseCoordinator::new(5);
        let result = coord.add_participant(MAIN_DB_ID, "main".to_owned(), false);
        assert!(
            matches!(result, Err(TwoPhaseError::NotWalMode(MAIN_DB_ID))),
            "expected NotWalMode error: {result:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 6: Cannot detach database with active 2PC.
    // -----------------------------------------------------------------------
    #[test]
    fn test_detach_with_active_transaction() {
        let mut coord = TwoPhaseCoordinator::new(6);
        coord
            .add_participant(MAIN_DB_ID, "main".to_owned(), true)
            .unwrap();
        coord.add_participant(2, "aux".to_owned(), true).unwrap();

        // Begin preparing.
        coord
            .prepare_participant(
                MAIN_DB_ID,
                PrepareResult::Ok {
                    wal_offset: 4096,
                    frame_count: 1,
                },
            )
            .unwrap();

        // Cannot detach a participant while 2PC is in progress.
        let result = coord.check_detach(2);
        assert!(matches!(
            result,
            Err(TwoPhaseError::DetachWithActiveTransaction { db_id: 2 })
        ));

        // Non-participant can be detached.
        assert!(coord.check_detach(99).is_ok());
    }

    // -----------------------------------------------------------------------
    // Test 7: Commit marker serialization roundtrip.
    // -----------------------------------------------------------------------
    #[test]
    fn test_commit_marker_roundtrip() {
        let marker = GlobalCommitMarker::new(
            42,
            CommitSeq::new(100),
            vec![(MAIN_DB_ID, 4096), (2, 8192), (3, 12288)],
            1_000_000_000,
        );
        assert!(marker.is_valid());

        let bytes = marker.to_bytes();
        let decoded = GlobalCommitMarker::from_bytes(&bytes).expect("decode should succeed");
        assert_eq!(decoded.magic, COMMIT_MARKER_MAGIC);
        assert_eq!(decoded.txn_id, 42);
        assert_eq!(decoded.commit_seq, CommitSeq::new(100));
        assert_eq!(decoded.participants.len(), 3);
        assert_eq!(decoded.timestamp_ns, 1_000_000_000);
        assert_eq!(decoded.participants[0], (MAIN_DB_ID, 4096));
        assert_eq!(decoded.participants[1], (2, 8192));
        assert_eq!(decoded.participants[2], (3, 12288));
    }

    // -----------------------------------------------------------------------
    // Test 8: Commit marker rejects invalid data.
    // -----------------------------------------------------------------------
    #[test]
    fn test_commit_marker_invalid() {
        // Too short.
        assert!(GlobalCommitMarker::from_bytes(&[0; 10]).is_none());

        // Wrong magic.
        let mut bad = vec![0u8; 32];
        bad[0] = b'X';
        assert!(GlobalCommitMarker::from_bytes(&bad).is_none());

        // Truncated participant data.
        let marker = GlobalCommitMarker::new(1, CommitSeq::new(1), vec![(0, 100)], 0);
        let bytes = marker.to_bytes();
        let truncated = &bytes[..bytes.len() - 4];
        assert!(GlobalCommitMarker::from_bytes(truncated).is_none());
    }

    // -----------------------------------------------------------------------
    // Test 9: Recovery action determination.
    // -----------------------------------------------------------------------
    #[test]
    fn test_recovery_actions() {
        // No marker, no WAL-index updates: no prepared frames, nothing to do.
        assert_eq!(
            TwoPhaseCoordinator::determine_recovery(false, true),
            RecoveryAction::NoAction
        );

        // No marker, WAL-index not updated: Phase 1 frames exist but no
        // decision; roll back.
        assert_eq!(
            TwoPhaseCoordinator::determine_recovery(false, false),
            RecoveryAction::RollBack
        );

        // Marker present, WAL-index incomplete: roll forward.
        assert_eq!(
            TwoPhaseCoordinator::determine_recovery(true, false),
            RecoveryAction::RollForward
        );

        // Marker present, all committed: nothing to do.
        assert_eq!(
            TwoPhaseCoordinator::determine_recovery(true, true),
            RecoveryAction::NoAction
        );
    }

    // -----------------------------------------------------------------------
    // Test 10: State machine rejects out-of-order operations.
    // -----------------------------------------------------------------------
    #[test]
    fn test_state_machine_invalid_transitions() {
        let mut coord = TwoPhaseCoordinator::new(10);

        // Cannot check_all_prepared before any preparation.
        assert!(matches!(
            coord.check_all_prepared(),
            Err(TwoPhaseError::InvalidState(TwoPhaseState::Idle))
        ));

        // Cannot write marker in Idle state.
        assert!(matches!(
            coord.write_commit_marker(CommitSeq::new(1), 0),
            Err(TwoPhaseError::InvalidState(TwoPhaseState::Idle))
        ));

        // Cannot commit participant in Idle state.
        assert!(matches!(
            coord.commit_participant(MAIN_DB_ID),
            Err(TwoPhaseError::InvalidState(TwoPhaseState::Idle))
        ));

        // Cannot abort a committed transaction.
        let mut coord2 = TwoPhaseCoordinator::new(11);
        coord2
            .add_participant(MAIN_DB_ID, "main".to_owned(), true)
            .unwrap();
        coord2
            .prepare_participant(
                MAIN_DB_ID,
                PrepareResult::Ok {
                    wal_offset: 0,
                    frame_count: 0,
                },
            )
            .unwrap();
        coord2.check_all_prepared().unwrap();
        coord2.write_commit_marker(CommitSeq::new(1), 0).unwrap();
        coord2.commit_participant(MAIN_DB_ID).unwrap();
        coord2.check_all_committed().unwrap();
        assert!(matches!(
            coord2.abort(),
            Err(TwoPhaseError::InvalidState(TwoPhaseState::Committed))
        ));
    }

    // -----------------------------------------------------------------------
    // Test 11: Both databases show inserts after successful 2PC.
    // -----------------------------------------------------------------------
    #[test]
    fn test_cross_db_2pc_both_committed() {
        let mut coord = TwoPhaseCoordinator::new(11);
        coord
            .add_participant(MAIN_DB_ID, "main".to_owned(), true)
            .unwrap();
        coord.add_participant(2, "aux".to_owned(), true).unwrap();

        // Prepare both with specific frame counts.
        coord
            .prepare_participant(
                MAIN_DB_ID,
                PrepareResult::Ok {
                    wal_offset: 4096,
                    frame_count: 5,
                },
            )
            .unwrap();
        coord
            .prepare_participant(
                2,
                PrepareResult::Ok {
                    wal_offset: 8192,
                    frame_count: 3,
                },
            )
            .unwrap();
        coord.check_all_prepared().unwrap();

        // Verify both are prepared.
        assert!(coord.participants[&MAIN_DB_ID].is_prepared());
        assert!(coord.participants[&2].is_prepared());

        // Write marker and commit.
        let marker = coord
            .write_commit_marker(CommitSeq::new(50), 500_000)
            .unwrap();
        assert_eq!(marker.participants.len(), 2);

        coord.commit_participant(MAIN_DB_ID).unwrap();
        coord.commit_participant(2).unwrap();
        coord.check_all_committed().unwrap();

        // Both are fully committed.
        assert!(coord.participants[&MAIN_DB_ID].is_committed());
        assert!(coord.participants[&2].is_committed());
        assert!(coord.is_committed());
    }

    // -----------------------------------------------------------------------
    // Test 12: Crash after Phase 1 prepare preserves atomicity guarantees.
    // -----------------------------------------------------------------------
    #[test]
    fn test_cross_db_2pc_crash_after_prepare() {
        let mut coord = TwoPhaseCoordinator::new(12);
        coord
            .add_participant(MAIN_DB_ID, "main".to_owned(), true)
            .unwrap();
        coord.add_participant(2, "aux".to_owned(), true).unwrap();

        coord
            .prepare_participant(
                MAIN_DB_ID,
                PrepareResult::Ok {
                    wal_offset: 4096,
                    frame_count: 2,
                },
            )
            .unwrap();
        coord
            .prepare_participant(
                2,
                PrepareResult::Ok {
                    wal_offset: 8192,
                    frame_count: 2,
                },
            )
            .unwrap();
        coord.check_all_prepared().unwrap();

        // Crash point: all participants prepared, decision marker not durable yet.
        let recovery = TwoPhaseCoordinator::determine_recovery(false, false);
        assert!(matches!(
            recovery,
            RecoveryAction::RollBack | RecoveryAction::RollForward
        ));

        match recovery {
            RecoveryAction::RollBack => {
                coord.abort().unwrap();
                assert!(coord.is_aborted());
                assert!(!coord.is_committed());
            }
            RecoveryAction::RollForward => {
                coord
                    .write_commit_marker(CommitSeq::new(320), 3_200_000)
                    .unwrap();
                for db_id in [MAIN_DB_ID, 2] {
                    coord.commit_participant(db_id).unwrap();
                }
                coord.check_all_committed().unwrap();
                assert!(coord.is_committed());
            }
            RecoveryAction::NoAction => panic!("recovery cannot be NoAction after crash"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 13: Crash during Phase 2 rolls forward to complete commit.
    // -----------------------------------------------------------------------
    #[test]
    fn test_cross_db_2pc_crash_during_phase2() {
        let mut coord = TwoPhaseCoordinator::new(13);
        coord
            .add_participant(MAIN_DB_ID, "main".to_owned(), true)
            .unwrap();
        coord.add_participant(2, "aux".to_owned(), true).unwrap();

        coord
            .prepare_participant(
                MAIN_DB_ID,
                PrepareResult::Ok {
                    wal_offset: 4096,
                    frame_count: 1,
                },
            )
            .unwrap();
        coord
            .prepare_participant(
                2,
                PrepareResult::Ok {
                    wal_offset: 8192,
                    frame_count: 1,
                },
            )
            .unwrap();
        coord.check_all_prepared().unwrap();
        coord
            .write_commit_marker(CommitSeq::new(330), 3_300_000)
            .unwrap();

        // Crash point: marker durable and one WAL-index already updated.
        coord.commit_participant(MAIN_DB_ID).unwrap();
        let recovery = TwoPhaseCoordinator::determine_recovery(true, false);
        assert_eq!(recovery, RecoveryAction::RollForward);

        // Recovery completes the remaining Phase 2 work.
        coord.commit_participant(2).unwrap();
        coord.check_all_committed().unwrap();
        assert!(coord.is_committed());
    }

    // -----------------------------------------------------------------------
    // Test 14: Abort before marker is allowed and clears decision marker.
    // -----------------------------------------------------------------------
    #[test]
    fn test_2pc_abort_before_marker() {
        let mut coord = TwoPhaseCoordinator::new(14);
        coord
            .add_participant(MAIN_DB_ID, "main".to_owned(), true)
            .unwrap();
        coord.add_participant(2, "aux".to_owned(), true).unwrap();

        // Prepare main, but abort before preparing aux.
        coord
            .prepare_participant(
                MAIN_DB_ID,
                PrepareResult::Ok {
                    wal_offset: 4096,
                    frame_count: 1,
                },
            )
            .unwrap();

        // Abort: valid at any state before Committed.
        coord.abort().expect("abort should succeed");
        assert!(coord.is_aborted());
        assert!(coord.commit_marker().is_none());
    }
}
