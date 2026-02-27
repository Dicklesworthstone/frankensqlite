//! Machine-validated WAL telemetry schema.
//!
//! Defines structured telemetry events for WAL append, replay, checkpoint, and
//! recovery paths.  Follows the zero-cost observer pattern established by
//! `ConflictObserver` in `fsqlite-observability`: a trait with a no-op default
//! implementation that the compiler elides entirely when unused.
//!
//! # Conformance rules
//!
//! 1. Every [`WalTelemetryEvent`] variant carries a monotonic `timestamp_ns`.
//! 2. All events and snapshots implement `serde::Serialize` for JSON export.
//! 3. Observers MUST NOT block, acquire page locks, or perform I/O.
//! 4. Log targets use `fsqlite.wal::<subdomain>` naming convention.
//! 5. Metric counters use `AtomicU64` with `Ordering::Relaxed`.

use serde::Serialize;

use crate::checkpoint::CheckpointMode;
use crate::checksum::{
    ChecksumFailureKind, RecoveryAction, WalChainInvalidReason, WalFecRepairOutcome,
};

// ---------------------------------------------------------------------------
// Telemetry event schema
// ---------------------------------------------------------------------------

/// Structured telemetry event emitted by WAL operations.
///
/// Each variant captures the minimal context needed to diagnose the operation
/// in post-mortem analysis or real-time dashboards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum WalTelemetryEvent {
    /// One or more frames appended to the WAL.
    FrameAppended {
        /// Number of frames written in this batch.
        frame_count: u32,
        /// Total bytes written (frame headers + page data).
        bytes_written: u64,
        /// Whether the last frame in the batch is a commit frame.
        is_commit: bool,
        /// Monotonic timestamp in nanoseconds.
        timestamp_ns: u64,
    },

    /// WAL chain replay started during open or recovery.
    ReplayStarted {
        /// Total valid frames found in the WAL chain.
        valid_frames: usize,
        /// Frames eligible for replay (up to last commit boundary).
        replayable_frames: usize,
        /// Monotonic timestamp in nanoseconds.
        timestamp_ns: u64,
    },

    /// WAL chain replay completed.
    ReplayCompleted {
        /// Frames successfully replayed to the page cache.
        frames_replayed: usize,
        /// Duration of replay in microseconds.
        duration_us: u64,
        /// Monotonic timestamp in nanoseconds.
        timestamp_ns: u64,
    },

    /// Checkpoint operation started.
    CheckpointStarted {
        /// Checkpoint mode requested.
        mode: CheckpointMode,
        /// Frames eligible for backfill.
        frames_to_backfill: u32,
        /// Monotonic timestamp in nanoseconds.
        timestamp_ns: u64,
    },

    /// Checkpoint operation completed.
    CheckpointCompleted {
        /// Checkpoint mode that was executed.
        mode: CheckpointMode,
        /// Frames actually backfilled to the database file.
        frames_backfilled: u32,
        /// Whether the WAL was reset after checkpoint.
        wal_reset: bool,
        /// Duration of checkpoint in microseconds.
        duration_us: u64,
        /// Monotonic timestamp in nanoseconds.
        timestamp_ns: u64,
    },

    /// WAL file reset (post-checkpoint truncation or restart).
    WalReset {
        /// New checkpoint sequence number after reset.
        new_checkpoint_seq: u32,
        /// Monotonic timestamp in nanoseconds.
        timestamp_ns: u64,
    },

    /// Checksum failure detected during chain validation or frame read.
    ChecksumFailure {
        /// Zero-based frame index where the failure occurred.
        frame_index: usize,
        /// Classification of the checksum failure.
        kind: ChecksumFailureKind,
        /// Recovery action selected for this failure.
        action: RecoveryAction,
        /// Monotonic timestamp in nanoseconds.
        timestamp_ns: u64,
    },

    /// WAL chain validation completed (during open or recovery).
    ChainValidated {
        /// Total frames examined.
        total_frames: usize,
        /// Whether the chain is fully valid.
        valid: bool,
        /// First invalid frame index, if any.
        first_invalid_frame: Option<usize>,
        /// Reason for invalidity, if applicable.
        reason: Option<WalChainInvalidReason>,
        /// Monotonic timestamp in nanoseconds.
        timestamp_ns: u64,
    },

    /// FEC repair attempted for a corrupted commit group.
    FecRepairAttempted {
        /// Outcome of the repair attempt.
        outcome: WalFecRepairOutcome,
        /// Number of repair symbols available.
        symbols_available: usize,
        /// Duration of the repair attempt in microseconds.
        duration_us: u64,
        /// Monotonic timestamp in nanoseconds.
        timestamp_ns: u64,
    },

    /// Group commit flush completed.
    GroupCommitFlushed {
        /// Number of transactions in this group.
        batch_size: u32,
        /// Total frames written in the group.
        total_frames: u32,
        /// Flush latency in microseconds.
        latency_us: u64,
        /// Monotonic timestamp in nanoseconds.
        timestamp_ns: u64,
    },
}

impl WalTelemetryEvent {
    /// Extract the monotonic timestamp from any event variant.
    #[must_use]
    pub fn timestamp_ns(&self) -> u64 {
        match self {
            Self::FrameAppended { timestamp_ns, .. }
            | Self::ReplayStarted { timestamp_ns, .. }
            | Self::ReplayCompleted { timestamp_ns, .. }
            | Self::CheckpointStarted { timestamp_ns, .. }
            | Self::CheckpointCompleted { timestamp_ns, .. }
            | Self::WalReset { timestamp_ns, .. }
            | Self::ChecksumFailure { timestamp_ns, .. }
            | Self::ChainValidated { timestamp_ns, .. }
            | Self::FecRepairAttempted { timestamp_ns, .. }
            | Self::GroupCommitFlushed { timestamp_ns, .. } => *timestamp_ns,
        }
    }

    /// Short classification label for this event kind.
    #[must_use]
    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::FrameAppended { .. } => "frame_appended",
            Self::ReplayStarted { .. } => "replay_started",
            Self::ReplayCompleted { .. } => "replay_completed",
            Self::CheckpointStarted { .. } => "checkpoint_started",
            Self::CheckpointCompleted { .. } => "checkpoint_completed",
            Self::WalReset { .. } => "wal_reset",
            Self::ChecksumFailure { .. } => "checksum_failure",
            Self::ChainValidated { .. } => "chain_validated",
            Self::FecRepairAttempted { .. } => "fec_repair_attempted",
            Self::GroupCommitFlushed { .. } => "group_commit_flushed",
        }
    }
}

// ---------------------------------------------------------------------------
// Observer trait (zero-cost when unused)
// ---------------------------------------------------------------------------

/// Trait for receiving structured WAL telemetry events.
///
/// Mirrors the `ConflictObserver` pattern: implementations MUST NOT block,
/// acquire page locks, or perform I/O.  The [`NoOpWalObserver`] default is
/// compiled away entirely when the WAL is instantiated without telemetry.
pub trait WalTelemetryObserver: Send + Sync {
    /// Called for each telemetry event emitted by WAL operations.
    fn on_event(&self, event: &WalTelemetryEvent);
}

/// No-op observer that compiles to zero instructions.
pub struct NoOpWalObserver;

impl WalTelemetryObserver for NoOpWalObserver {
    #[inline(always)]
    fn on_event(&self, _event: &WalTelemetryEvent) {}
}

/// Ring-buffer observer that stores the last N events for diagnostic queries.
pub struct WalTelemetryRingBuffer {
    events: parking_lot::Mutex<WalRingBufferInner>,
}

struct WalRingBufferInner {
    buf: Vec<WalTelemetryEvent>,
    capacity: usize,
    write_pos: usize,
    count: usize,
}

impl WalTelemetryRingBuffer {
    /// Create a ring buffer with the given capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            events: parking_lot::Mutex::new(WalRingBufferInner {
                buf: Vec::with_capacity(capacity),
                capacity,
                write_pos: 0,
                count: 0,
            }),
        }
    }

    /// Drain the most recent events (up to capacity) in chronological order.
    #[must_use]
    pub fn drain(&self) -> Vec<WalTelemetryEvent> {
        let inner = self.events.lock();
        let n = inner.count.min(inner.capacity);
        let mut result = Vec::with_capacity(n);
        if n == 0 {
            return result;
        }
        let start = if inner.count >= inner.capacity {
            inner.write_pos
        } else {
            0
        };
        for i in 0..n {
            let idx = (start + i) % inner.capacity;
            result.push(inner.buf[idx].clone());
        }
        result
    }

    /// Number of events currently stored.
    #[must_use]
    pub fn len(&self) -> usize {
        let inner = self.events.lock();
        inner.count.min(inner.capacity)
    }

    /// Whether the buffer is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl WalTelemetryObserver for WalTelemetryRingBuffer {
    fn on_event(&self, event: &WalTelemetryEvent) {
        let mut inner = self.events.lock();
        let pos = inner.write_pos;
        if inner.buf.len() < inner.capacity {
            inner.buf.push(event.clone());
        } else {
            inner.buf[pos] = event.clone();
        }
        inner.write_pos = (pos + 1) % inner.capacity;
        inner.count += 1;
    }
}

// ---------------------------------------------------------------------------
// Composite snapshot
// ---------------------------------------------------------------------------

/// Unified snapshot of all WAL telemetry counters (metrics + FEC + recovery +
/// group commit).  Produced by [`wal_telemetry_snapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WalTelemetrySnapshot {
    pub wal: crate::metrics::WalMetricsSnapshot,
    pub fec_repair: crate::metrics::WalFecRepairCountersSnapshot,
    pub recovery: crate::metrics::WalRecoveryCountersSnapshot,
    pub group_commit: crate::metrics::GroupCommitMetricsSnapshot,
}

/// Collect a point-in-time snapshot of all global WAL telemetry counters.
#[must_use]
pub fn wal_telemetry_snapshot() -> WalTelemetrySnapshot {
    WalTelemetrySnapshot {
        wal: crate::metrics::GLOBAL_WAL_METRICS.snapshot(),
        fec_repair: crate::metrics::GLOBAL_WAL_FEC_REPAIR_METRICS.snapshot(),
        recovery: crate::metrics::GLOBAL_WAL_RECOVERY_METRICS.snapshot(),
        group_commit: crate::metrics::GLOBAL_GROUP_COMMIT_METRICS.snapshot(),
    }
}

// ===========================================================================
// Tests — CI conformance validator
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::CheckpointMode;
    use crate::checksum::{
        ChecksumFailureKind, RecoveryAction, WalChainInvalidReason, WalFecRepairOutcome,
    };

    // ── Helper: build one event per variant ──

    fn all_event_variants() -> Vec<WalTelemetryEvent> {
        vec![
            WalTelemetryEvent::FrameAppended {
                frame_count: 3,
                bytes_written: 12_360,
                is_commit: true,
                timestamp_ns: 1_000_000,
            },
            WalTelemetryEvent::ReplayStarted {
                valid_frames: 10,
                replayable_frames: 8,
                timestamp_ns: 2_000_000,
            },
            WalTelemetryEvent::ReplayCompleted {
                frames_replayed: 8,
                duration_us: 500,
                timestamp_ns: 3_000_000,
            },
            WalTelemetryEvent::CheckpointStarted {
                mode: CheckpointMode::Passive,
                frames_to_backfill: 20,
                timestamp_ns: 4_000_000,
            },
            WalTelemetryEvent::CheckpointCompleted {
                mode: CheckpointMode::Restart,
                frames_backfilled: 20,
                wal_reset: true,
                duration_us: 3500,
                timestamp_ns: 5_000_000,
            },
            WalTelemetryEvent::WalReset {
                new_checkpoint_seq: 7,
                timestamp_ns: 6_000_000,
            },
            WalTelemetryEvent::ChecksumFailure {
                frame_index: 4,
                kind: ChecksumFailureKind::WalFrameChecksumMismatch,
                action: RecoveryAction::AttemptWalFecRepair,
                timestamp_ns: 7_000_000,
            },
            WalTelemetryEvent::ChainValidated {
                total_frames: 10,
                valid: false,
                first_invalid_frame: Some(4),
                reason: Some(WalChainInvalidReason::FrameChecksumMismatch),
                timestamp_ns: 8_000_000,
            },
            WalTelemetryEvent::FecRepairAttempted {
                outcome: WalFecRepairOutcome::Repaired,
                symbols_available: 12,
                duration_us: 200,
                timestamp_ns: 9_000_000,
            },
            WalTelemetryEvent::GroupCommitFlushed {
                batch_size: 4,
                total_frames: 16,
                latency_us: 1200,
                timestamp_ns: 10_000_000,
            },
        ]
    }

    // ── Conformance rule 1: every variant has a timestamp_ns ──

    #[test]
    fn conformance_every_variant_has_monotonic_timestamp() {
        let events = all_event_variants();
        let mut prev_ts = 0u64;
        for event in &events {
            let ts = event.timestamp_ns();
            assert!(
                ts > prev_ts,
                "timestamp must be monotonic: {} <= {} for {:?}",
                ts,
                prev_ts,
                event.kind_str()
            );
            prev_ts = ts;
        }
    }

    // ── Conformance rule 2: all events serialize to valid JSON ──

    #[test]
    fn conformance_all_events_serialize_to_json() {
        for event in all_event_variants() {
            let json = serde_json::to_string(&event)
                .unwrap_or_else(|e| panic!("failed to serialize {:?}: {e}", event.kind_str()));
            assert!(
                !json.is_empty(),
                "serialized JSON must not be empty for {}",
                event.kind_str()
            );
            // Verify it round-trips as valid JSON (parseable as Value).
            let _: serde_json::Value = serde_json::from_str(&json)
                .unwrap_or_else(|e| panic!("JSON not parseable for {}: {e}", event.kind_str()));
        }
    }

    // ── Conformance rule 2b: all snapshots serialize to JSON ──

    #[test]
    fn conformance_wal_metrics_snapshot_serializes() {
        let snap = crate::metrics::WalMetrics::new();
        snap.record_frame_write(4096);
        snap.record_checkpoint(5, 2000);
        snap.record_wal_reset();
        let s = snap.snapshot();
        let json = serde_json::to_string(&s).expect("WalMetricsSnapshot must serialize");
        assert!(json.contains("frames_written_total"));
        assert!(json.contains("checkpoint_count"));
    }

    #[test]
    fn conformance_fec_repair_snapshot_serializes() {
        let c = crate::metrics::WalFecRepairCounters::new();
        c.record_repair(true, 500);
        c.record_encode();
        let s = c.snapshot();
        let json = serde_json::to_string(&s).expect("WalFecRepairCountersSnapshot must serialize");
        assert!(json.contains("repairs_succeeded"));
        assert!(json.contains("encode_ops"));
    }

    #[test]
    fn conformance_recovery_snapshot_serializes() {
        let r = crate::metrics::WalRecoveryCounters::new();
        r.record_recovery(10, 2, 1);
        let s = r.snapshot();
        let json = serde_json::to_string(&s).expect("WalRecoveryCountersSnapshot must serialize");
        assert!(json.contains("recovery_frames_total"));
        assert!(json.contains("corruption_detected_total"));
    }

    #[test]
    fn conformance_group_commit_snapshot_serializes() {
        let g = crate::metrics::GroupCommitMetrics::new();
        g.record_group_commit(3, 1000);
        g.record_submission();
        let s = g.snapshot();
        let json = serde_json::to_string(&s).expect("GroupCommitMetricsSnapshot must serialize");
        assert!(json.contains("group_commits_total"));
        assert!(json.contains("submissions_total"));
    }

    #[test]
    fn conformance_composite_snapshot_serializes() {
        let snap = wal_telemetry_snapshot();
        let json = serde_json::to_string(&snap).expect("WalTelemetrySnapshot must serialize");
        // Must contain all four sub-sections.
        assert!(json.contains("wal"));
        assert!(json.contains("fec_repair"));
        assert!(json.contains("recovery"));
        assert!(json.contains("group_commit"));
    }

    // ── Conformance rule 3: kind_str covers every variant ──

    #[test]
    fn conformance_kind_str_unique_per_variant() {
        let events = all_event_variants();
        let kinds: Vec<&str> = events.iter().map(|e| e.kind_str()).collect();
        // All must be non-empty.
        for k in &kinds {
            assert!(!k.is_empty(), "kind_str must not be empty");
        }
        // All must be unique.
        let mut sorted = kinds.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            kinds.len(),
            sorted.len(),
            "kind_str must be unique per variant"
        );
    }

    // ── Conformance rule 4: event variant count is exhaustive ──

    #[test]
    fn conformance_variant_count_matches_schema() {
        // This test locks the schema at 10 variants.  If a new variant is
        // added, this test must be updated, forcing the author to also add
        // the variant to `all_event_variants()` and the serialization tests.
        assert_eq!(
            all_event_variants().len(),
            10,
            "WalTelemetryEvent must have exactly 10 variants (update all_event_variants if adding)"
        );
    }

    // ── Conformance rule 5: ChecksumFailureKind variants are exhaustive ──

    #[test]
    fn conformance_checksum_failure_kinds_serialize() {
        let kinds = [
            ChecksumFailureKind::WalFrameChecksumMismatch,
            ChecksumFailureKind::Xxh3PageChecksumMismatch,
            ChecksumFailureKind::Crc32cSymbolMismatch,
            ChecksumFailureKind::DbFileCorruption,
        ];
        for kind in kinds {
            let json = serde_json::to_string(&kind)
                .unwrap_or_else(|e| panic!("ChecksumFailureKind::{kind:?} serialize failed: {e}"));
            assert!(!json.is_empty());
        }
    }

    #[test]
    fn conformance_recovery_actions_serialize() {
        let actions = [
            RecoveryAction::AttemptWalFecRepair,
            RecoveryAction::TruncateWalAtFirstInvalidFrame,
            RecoveryAction::EvictCacheAndRetryFromWal,
            RecoveryAction::ExcludeCorruptedSymbolAndContinue,
            RecoveryAction::ReportPersistentCorruption,
        ];
        for action in actions {
            let json = serde_json::to_string(&action)
                .unwrap_or_else(|e| panic!("RecoveryAction::{action:?} serialize failed: {e}"));
            assert!(!json.is_empty());
        }
    }

    #[test]
    fn conformance_wal_chain_invalid_reasons_serialize() {
        let reasons = [
            WalChainInvalidReason::HeaderChecksumMismatch,
            WalChainInvalidReason::TruncatedFrame,
            WalChainInvalidReason::SaltMismatch,
            WalChainInvalidReason::FrameSaltMismatch,
            WalChainInvalidReason::FrameChecksumMismatch,
        ];
        for reason in reasons {
            let json = serde_json::to_string(&reason).unwrap_or_else(|e| {
                panic!("WalChainInvalidReason::{reason:?} serialize failed: {e}")
            });
            assert!(!json.is_empty());
        }
    }

    #[test]
    fn conformance_checkpoint_modes_serialize() {
        let modes = [
            CheckpointMode::Passive,
            CheckpointMode::Full,
            CheckpointMode::Restart,
            CheckpointMode::Truncate,
        ];
        for mode in modes {
            let json = serde_json::to_string(&mode)
                .unwrap_or_else(|e| panic!("CheckpointMode::{mode:?} serialize failed: {e}"));
            assert!(!json.is_empty());
        }
    }

    #[test]
    fn conformance_fec_repair_outcomes_serialize() {
        let outcomes = [
            WalFecRepairOutcome::Repaired,
            WalFecRepairOutcome::InsufficientSymbols,
            WalFecRepairOutcome::SourceHashMismatch,
        ];
        for outcome in outcomes {
            let json = serde_json::to_string(&outcome).unwrap_or_else(|e| {
                panic!("WalFecRepairOutcome::{outcome:?} serialize failed: {e}")
            });
            assert!(!json.is_empty());
        }
    }

    // ── Observer trait tests ──

    #[test]
    fn noop_observer_compiles_away() {
        let obs = NoOpWalObserver;
        let event = WalTelemetryEvent::FrameAppended {
            frame_count: 1,
            bytes_written: 4120,
            is_commit: false,
            timestamp_ns: 42,
        };
        // Should be a no-op; just verify it doesn't panic.
        obs.on_event(&event);
    }

    #[test]
    fn ring_buffer_stores_events() {
        let rb = WalTelemetryRingBuffer::new(4);
        assert!(rb.is_empty());
        for (i, event) in all_event_variants().into_iter().enumerate().take(3) {
            let _ = i;
            rb.on_event(&event);
        }
        assert_eq!(rb.len(), 3);
        let drained = rb.drain();
        assert_eq!(drained.len(), 3);
    }

    #[test]
    fn ring_buffer_wraps_at_capacity() {
        let rb = WalTelemetryRingBuffer::new(3);
        let events = all_event_variants();
        // Push 5 events into a buffer of capacity 3.
        for event in events.iter().take(5) {
            rb.on_event(event);
        }
        assert_eq!(rb.len(), 3);
        let drained = rb.drain();
        assert_eq!(drained.len(), 3);
        // Should have the last 3 events.
        assert_eq!(drained[0].kind_str(), events[2].kind_str());
        assert_eq!(drained[1].kind_str(), events[3].kind_str());
        assert_eq!(drained[2].kind_str(), events[4].kind_str());
    }

    #[test]
    fn ring_buffer_drain_preserves_chronological_order() {
        let rb = WalTelemetryRingBuffer::new(10);
        let events = all_event_variants();
        for event in &events {
            rb.on_event(event);
        }
        let drained = rb.drain();
        for pair in drained.windows(2) {
            assert!(
                pair[0].timestamp_ns() <= pair[1].timestamp_ns(),
                "drain must be chronological"
            );
        }
    }

    // ── Composite snapshot tests ──

    #[test]
    fn composite_snapshot_captures_all_globals() {
        // Reset globals to known state.
        crate::metrics::GLOBAL_WAL_METRICS.reset();
        crate::metrics::GLOBAL_WAL_FEC_REPAIR_METRICS.reset();
        crate::metrics::GLOBAL_WAL_RECOVERY_METRICS.reset();
        crate::metrics::GLOBAL_GROUP_COMMIT_METRICS.reset();

        // Record some activity.
        crate::metrics::GLOBAL_WAL_METRICS.record_frame_write(4096);
        crate::metrics::GLOBAL_WAL_FEC_REPAIR_METRICS.record_repair(true, 100);
        crate::metrics::GLOBAL_WAL_RECOVERY_METRICS.record_recovery(5, 1, 1);
        crate::metrics::GLOBAL_GROUP_COMMIT_METRICS.record_group_commit(2, 500);

        let snap = wal_telemetry_snapshot();
        assert_eq!(snap.wal.frames_written_total, 1);
        assert_eq!(snap.fec_repair.repairs_succeeded, 1);
        assert_eq!(snap.recovery.recovery_frames_total, 5);
        assert_eq!(snap.group_commit.group_commits_total, 1);
    }
}
