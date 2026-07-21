//! Parallel WAL coordinator (D1: bd-3wop3.1).
//!
//! This module provides a lock-free parallel WAL write path using per-thread
//! buffers and epoch-based group commit. It replaces the global WAL append
//! mutex with cooperative per-thread buffering.
//!
//! # Architecture
//!
//! 1. Each writer thread appends WAL frames to its own buffer with NO global lock.
//! 2. A background epoch ticker advances the global epoch every ~10ms.
//! 3. On epoch advance, slot-local buffer locks make sealing wait for any
//!    in-flight batch append to complete, then the previous epoch is flushed.
//! 4. Commit durability: transaction waits until its epoch is durable.
//!
//! # Key Benefits
//!
//! - Eliminates the #1 contention point (global WAL append mutex).
//! - WAL writes are now embarrassingly parallel.
//! - Epoch mechanism provides natural group commit semantics (Silo/Aether pattern).

use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::hash::BuildHasher;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use asupersync::runtime::{BlockingTaskHandle, RuntimeHandle};
use fsqlite_types::{CommitSeq, PageNumber, TxnToken, cx::Cx, limits};

use crate::group_commit::TransactionFrameBatchContext;
use crate::per_core_buffer::{
    AppendOutcome, BufferConfig, DEFAULT_BUFFER_SLOT_COUNT, EpochConfig, EpochFlushBatch,
    EpochOrderCoordinator, WalRecord, thread_buffer_slot,
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the parallel WAL coordinator.
#[derive(Debug, Clone, Copy)]
pub struct ParallelWalConfig {
    /// Number of buffer slots (typically 128 for 16 threads).
    pub slot_count: usize,
    /// Epoch advance interval in milliseconds (default: 10ms).
    pub epoch_interval_ms: u64,
    /// Buffer capacity in bytes per slot (default: 4MB).
    pub buffer_capacity_bytes: usize,
}

impl Default for ParallelWalConfig {
    fn default() -> Self {
        Self {
            slot_count: DEFAULT_BUFFER_SLOT_COUNT,
            epoch_interval_ms: 10,
            buffer_capacity_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Operator-visible control mode for the D1.a parallel WAL contract.
///
/// `Auto` keeps the deterministic parallel data plane enabled and allows the
/// optional decision plane to tune batching/flush behavior within declared
/// safety bounds. `Conservative` forces the compatibility-safe single ordered
/// path. `ShadowCompare` runs the conservative proof path alongside the
/// parallel data plane and forces a downgrade if the two disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParallelWalOperatingMode {
    #[default]
    Auto,
    Conservative,
    ShadowCompare,
}

/// The irreducible ordered residue that remains even after per-core lane
/// staging removes the global append bottleneck.
///
/// D1.a explicitly constrains the ordered residue to:
/// 1. commit-sequence assignment,
/// 2. commit-certificate durability, and
/// 3. pager visibility publication.
///
/// Everything before that point stays lane-local and parallelizable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParallelWalOrderedResidue {
    #[default]
    CommitCertificateThenPublish,
}

/// Deterministic reasons for forcing the conservative/safe path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelWalFallbackReason {
    OperatorForced,
    LaneOverflow,
    CertificateGap,
    CertificateChecksumMismatch,
    PublicationMismatch,
    RecoveryGap,
    CheckpointConflict,
    ControllerEvidenceLost,
}

/// Explicit operator control surface for the D1.a contract.
///
/// This is intentionally configuration-shaped rather than implementation-
/// shaped so the later D1.b/D1.c/D1.d beads cannot reinterpret the runtime
/// knobs ad hoc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelWalControlSurface {
    /// `Auto`, `Conservative`, or `ShadowCompare`.
    pub mode: ParallelWalOperatingMode,
    /// Optional hard override for the number of active append lanes.
    pub lane_count_override: Option<usize>,
    /// Optional cap for helper/combine lanes assisting with flush work.
    pub helper_lane_budget: Option<usize>,
    /// Optional cap on batch bytes the decision plane may stage before sealing.
    pub max_parallel_commit_bytes: Option<u64>,
    /// Optional cap on how long a batch may wait before being sealed/flushed.
    pub max_flush_delay_ms: Option<u64>,
    /// Shadow-compare sampling rate in per-mille. `None` disables sampling.
    pub shadow_compare_sampling_per_mille: Option<u16>,
}

impl Default for ParallelWalControlSurface {
    fn default() -> Self {
        Self {
            mode: ParallelWalOperatingMode::Auto,
            lane_count_override: None,
            helper_lane_budget: None,
            max_parallel_commit_bytes: None,
            max_flush_delay_ms: None,
            shadow_compare_sampling_per_mille: None,
        }
    }
}

/// Telemetry schema version for lane-local append staging.
pub const PARALLEL_WAL_LANE_POLICY_VERSION: &str = "thread_slot_v1";
/// Compatibility selector bundle required by `bd-db300.7.5.3` / `G5.3`.
pub const PARALLEL_WAL_COMPATIBILITY_SELECTOR: &str = "wal_invariant,integrity_check,row_level";
/// Structured-log scenario id for queue submission.
pub const PARALLEL_WAL_STAGE_SCENARIO_ID: &str = "parallel_wal_lane_stage";
/// Structured-log scenario id for flush-time lane telemetry.
pub const PARALLEL_WAL_FLUSH_SCENARIO_ID: &str = "parallel_wal_lane_flush";
/// Lane ids and commit-certificate lane counts are stored as `u16`, so keep
/// both the count and every generated id representable.
const MAX_PARALLEL_WAL_LANE_COUNT: usize = 65_535;

/// Verdict emitted by shadow-compare lane validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParallelWalShadowVerdict {
    #[default]
    NotRun,
    Clean,
    Diverged,
}

/// Staged lane-local payload awaiting ordered flush consumption.
#[derive(Debug, Clone)]
pub struct ParallelWalLaneBatch<T> {
    /// Monotonic identifier used to correlate queue submission with flush.
    pub batch_id: u64,
    /// Stable lane identity chosen for the submitting writer.
    pub lane_id: u16,
    /// Number of frames staged for this batch.
    pub staged_frame_count: u32,
    /// Time spent staging locally before queue submission.
    pub staging_elapsed_ns: u64,
    /// Shadow-compare outcome for this batch.
    pub shadow_verdict: ParallelWalShadowVerdict,
    /// Caller-owned payload preserved until the ordered residue consumes it.
    pub payload: T,
}

/// Production lane-local staging state for ordinary parallel WAL appends.
///
/// This keeps batch ownership, backlog accounting, and same-lane drain order
/// inside `fsqlite-wal` instead of scattering the logic across pager-only
/// callers. The caller still owns the final ordered durability residue.
#[derive(Debug)]
pub struct ParallelWalLaneStager<T> {
    control: ParallelWalControlSurface,
    next_batch_id: AtomicU64,
    lane_batches: Mutex<HashMap<u16, VecDeque<ParallelWalLaneBatch<T>>>>,
    lane_backlog_frames: Mutex<HashMap<u16, usize>>,
}

impl<T> ParallelWalLaneStager<T> {
    #[must_use]
    pub fn new(control: ParallelWalControlSurface) -> Self {
        Self {
            control,
            next_batch_id: AtomicU64::new(1),
            lane_batches: Mutex::new(HashMap::new()),
            lane_backlog_frames: Mutex::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn control(&self) -> &ParallelWalControlSurface {
        &self.control
    }

    #[must_use]
    pub fn next_batch_id(&self) -> u64 {
        self.next_batch_id.fetch_add(1, Ordering::Relaxed)
    }

    #[must_use]
    pub fn lane_count(&self) -> usize {
        match self.control.mode {
            ParallelWalOperatingMode::Conservative => 1,
            _ => self
                .control
                .lane_count_override
                .unwrap_or_else(default_parallel_wal_lane_count)
                .clamp(1, MAX_PARALLEL_WAL_LANE_COUNT),
        }
    }

    #[must_use]
    pub fn current_lane_id(&self) -> u16 {
        u16::try_from(thread_buffer_slot(self.lane_count()))
            .expect("lane_count is clamped to the u16 lane-id range")
    }

    #[must_use]
    pub fn current_lane_backlog(&self, lane_id: u16) -> usize {
        self.lane_backlog_frames
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&lane_id)
            .copied()
            .unwrap_or(0)
    }

    pub fn record_batch(&self, batch: ParallelWalLaneBatch<T>) -> usize {
        let lane_id = batch.lane_id;
        let staged_frame_count = usize::try_from(batch.staged_frame_count).unwrap_or(0);

        let mut lane_batches = self
            .lane_batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut lane_backlog = self
            .lane_backlog_frames
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lane_batches.entry(lane_id).or_default().push_back(batch);
        let backlog = lane_backlog.entry(lane_id).or_insert(0);
        *backlog = backlog.saturating_add(staged_frame_count);
        *backlog
    }

    pub fn take_batches_for_flush(
        &self,
        contexts: &[TransactionFrameBatchContext],
    ) -> Option<HashMap<u64, ParallelWalLaneBatch<T>>> {
        let mut lane_batches = self
            .lane_batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut expected_offsets = HashMap::<u16, usize>::new();
        for context in contexts {
            let offset = expected_offsets.entry(context.lane_id).or_insert(0);
            let candidate = lane_batches
                .get(&context.lane_id)
                .and_then(|queue| queue.get(*offset))
                .filter(|candidate| candidate.batch_id == context.batch_id)?;
            let _ = candidate;
            *offset = offset.saturating_add(1);
        }

        let mut by_batch_id = HashMap::with_capacity(contexts.len());
        let mut lane_backlog = self
            .lane_backlog_frames
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for context in contexts {
            let candidate = lane_batches
                .get_mut(&context.lane_id)
                .and_then(VecDeque::pop_front)
                .expect("verified lane-local batch must still exist");
            let backlog = lane_backlog.entry(context.lane_id).or_insert(0);
            *backlog = backlog.saturating_sub(
                usize::try_from(candidate.staged_frame_count).unwrap_or(usize::MAX),
            );
            if *backlog == 0 {
                lane_backlog.remove(&context.lane_id);
            }
            if lane_batches
                .get(&context.lane_id)
                .is_some_and(VecDeque::is_empty)
            {
                lane_batches.remove(&context.lane_id);
            }
            by_batch_id.insert(candidate.batch_id, candidate);
        }

        Some(by_batch_id)
    }

    /// Discard prepared payloads for batches that already fell back to the
    /// raw ordered WAL path.
    ///
    /// Lane-local preparation is an optimization, not the durability record.
    /// If a flusher cannot consume the prepared payloads for a group-commit
    /// epoch, those payloads become stale as soon as the same batches are
    /// appended via borrowed frame refs. Leaving them queued would make later
    /// epochs see an old batch id at the front of the lane and permanently
    /// fall back.
    pub fn discard_batches_for_flush(&self, contexts: &[TransactionFrameBatchContext]) -> usize {
        if contexts.is_empty() {
            return 0;
        }

        let discard_ids = contexts
            .iter()
            .map(|context| context.batch_id)
            .collect::<HashSet<_>>();
        let mut removed_batches = 0_usize;
        let mut removed_frames_by_lane = HashMap::<u16, usize>::new();

        let mut lane_batches = self
            .lane_batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (lane_id, queue) in lane_batches.iter_mut() {
            let mut retained = VecDeque::with_capacity(queue.len());
            while let Some(batch) = queue.pop_front() {
                if discard_ids.contains(&batch.batch_id) {
                    removed_batches = removed_batches.saturating_add(1);
                    let removed_frames =
                        usize::try_from(batch.staged_frame_count).unwrap_or(usize::MAX);
                    let entry = removed_frames_by_lane.entry(*lane_id).or_insert(0);
                    *entry = entry.saturating_add(removed_frames);
                } else {
                    retained.push_back(batch);
                }
            }
            *queue = retained;
        }
        lane_batches.retain(|_, queue| !queue.is_empty());

        if !removed_frames_by_lane.is_empty() {
            let mut lane_backlog = self
                .lane_backlog_frames
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (lane_id, removed_frames) in removed_frames_by_lane {
                let backlog = lane_backlog.entry(lane_id).or_insert(0);
                *backlog = backlog.saturating_sub(removed_frames);
                if *backlog == 0 {
                    lane_backlog.remove(&lane_id);
                }
            }
        }

        removed_batches
    }
}

#[must_use]
pub fn parallel_wal_mode_name(mode: ParallelWalOperatingMode) -> &'static str {
    match mode {
        ParallelWalOperatingMode::Auto => "auto",
        ParallelWalOperatingMode::Conservative => "conservative",
        ParallelWalOperatingMode::ShadowCompare => "shadow_compare",
    }
}

#[must_use]
pub fn parallel_wal_fallback_reason_name(
    reason: Option<ParallelWalFallbackReason>,
) -> &'static str {
    match reason {
        None => "none",
        Some(ParallelWalFallbackReason::OperatorForced) => "operator_forced",
        Some(ParallelWalFallbackReason::LaneOverflow) => "lane_overflow",
        Some(ParallelWalFallbackReason::CertificateGap) => "certificate_gap",
        Some(ParallelWalFallbackReason::CertificateChecksumMismatch) => {
            "certificate_checksum_mismatch"
        }
        Some(ParallelWalFallbackReason::PublicationMismatch) => "publication_mismatch",
        Some(ParallelWalFallbackReason::RecoveryGap) => "recovery_gap",
        Some(ParallelWalFallbackReason::CheckpointConflict) => "checkpoint_conflict",
        Some(ParallelWalFallbackReason::ControllerEvidenceLost) => "controller_evidence_lost",
    }
}

#[must_use]
pub fn parallel_wal_shadow_verdict_name(verdict: ParallelWalShadowVerdict) -> &'static str {
    match verdict {
        ParallelWalShadowVerdict::NotRun => "not_run",
        ParallelWalShadowVerdict::Clean => "clean",
        ParallelWalShadowVerdict::Diverged => "diverged",
    }
}

/// Decide whether a batch should run the shadow-compare proof path.
///
/// `ShadowCompare` mode always compares. In `Auto`, operators can enable a
/// deterministic sample using `shadow_compare_sampling_per_mille`; the first
/// `N` batch ids in each 1000-batch window take the compare path.
#[must_use]
pub fn parallel_wal_should_shadow_compare(
    control: &ParallelWalControlSurface,
    batch_id: u64,
) -> bool {
    match control.mode {
        ParallelWalOperatingMode::Conservative => false,
        ParallelWalOperatingMode::ShadowCompare => true,
        ParallelWalOperatingMode::Auto => {
            control
                .shadow_compare_sampling_per_mille
                .is_some_and(|rate| {
                    let rate = u64::from(rate.min(1_000));
                    rate > 0 && batch_id.saturating_sub(1) % 1_000 < rate
                })
        }
    }
}

#[must_use]
pub fn default_parallel_wal_lane_count() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        .max(1)
}

#[must_use]
pub fn resolve_parallel_wal_control_surface_from_env() -> ParallelWalControlSurface {
    let mut control = ParallelWalControlSurface {
        // Default runtime policy favors the raw ordered append path; operators
        // can still opt into lane-local staging with FSQLITE_PARALLEL_WAL_MODE=auto.
        mode: ParallelWalOperatingMode::Conservative,
        ..ParallelWalControlSurface::default()
    };

    if let Ok(mode) = env::var("FSQLITE_PARALLEL_WAL_MODE") {
        control.mode = match mode.trim().to_ascii_lowercase().as_str() {
            "auto" => ParallelWalOperatingMode::Auto,
            "conservative" | "serialized" | "single_lane" => ParallelWalOperatingMode::Conservative,
            "shadow" | "shadow_compare" => ParallelWalOperatingMode::ShadowCompare,
            _ => control.mode,
        };
    }
    if let Ok(raw) = env::var("FSQLITE_PARALLEL_WAL_LANES") {
        if let Ok(value) = raw.trim().parse::<usize>() {
            control.lane_count_override = Some(value.max(1));
        }
    }
    if let Ok(raw) = env::var("FSQLITE_PARALLEL_WAL_MAX_BATCH_BYTES") {
        if let Ok(value) = raw.trim().parse::<u64>() {
            control.max_parallel_commit_bytes = Some(value.max(1));
        }
    }
    if let Ok(raw) = env::var("FSQLITE_PARALLEL_WAL_MAX_FLUSH_DELAY_MS") {
        if let Ok(value) = raw.trim().parse::<u64>() {
            control.max_flush_delay_ms = Some(value);
        }
    }
    if let Ok(raw) = env::var("FSQLITE_PARALLEL_WAL_SHADOW_COMPARE_PER_MILLE") {
        if let Ok(value) = raw.trim().parse::<u16>() {
            control.shadow_compare_sampling_per_mille = Some(value);
        }
    }

    control
}

/// Commit-certificate proof object for the parallel WAL data plane.
///
/// A commit becomes externally publishable only after the certificate is
/// durably written. The certificate covers a contiguous commit-sequence range,
/// the lanes that contributed to that range, and the pager-visible metadata
/// that must be published once the ordered residue completes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelWalCommitCertificate {
    pub format_version: u16,
    pub residue: ParallelWalOrderedResidue,
    pub certificate_epoch: u64,
    pub commit_seq_lo: CommitSeq,
    pub commit_seq_hi: CommitSeq,
    pub durable_segment_epoch: u64,
    pub lane_count: u16,
    pub lane_record_counts: Vec<u32>,
    pub db_size_pages: u32,
    pub page_set_size: u32,
    pub certificate_crc32c: u32,
    pub fallback_active: bool,
}

/// Trace schema shared by lane, combiner, checkpoint, recovery, and
/// control-plane events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelWalTraceRecord {
    pub component: String,
    pub trace_id: u64,
    pub decision_id: Option<u64>,
    pub mode: ParallelWalOperatingMode,
    pub lane_id: Option<usize>,
    pub epoch: Option<u64>,
    pub commit_seq_lo: Option<CommitSeq>,
    pub commit_seq_hi: Option<CommitSeq>,
    pub checkpoint_epoch: Option<u64>,
    pub recovery_epoch: Option<u64>,
    pub fallback_active: bool,
    pub fallback_reason: Option<ParallelWalFallbackReason>,
    pub policy_id: Option<String>,
    pub policy_version: Option<String>,
}

/// Optional controller action for the D1 decision plane.
///
/// The deterministic data plane stays correct even when the controller is
/// disabled. These actions only tune batching and lane budgets within the
/// declared control surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelWalDecisionAction {
    KeepCurrent,
    SealEpochNow,
    IncreaseLaneBudget,
    DecreaseLaneBudget,
    ForceConservative,
}

/// Decision-record schema for the optional D1 controller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelWalDecisionRecord {
    pub policy_id: String,
    pub policy_version: String,
    pub decision_id: u64,
    pub action: ParallelWalDecisionAction,
    pub confidence_bps: u16,
    pub expected_loss_micros: u64,
    pub top_evidence_terms: Vec<String>,
    pub counterfactual_action: ParallelWalDecisionAction,
    pub counterfactual_regret_micros: i64,
    pub fallback_active: bool,
}

// ---------------------------------------------------------------------------
// Segment File I/O (D1.6)
// ---------------------------------------------------------------------------

/// Magic number for parallel WAL segment files.
const SEGMENT_MAGIC: u32 = 0x5057_414C; // "PWAL"

/// Version of the segment file format.
const SEGMENT_VERSION: u16 = 1;

/// Segment file header size in bytes.
const SEGMENT_HEADER_SIZE: usize = 24;

/// Fixed record bytes for a record without `end_seq` and without page images.
const SEGMENT_RECORD_MIN_SIZE: usize = 8 + 4 + 8 + 4 + 8 + 1 + 4 + 4;

/// Largest supported page image in a segment record.
const MAX_SEGMENT_RECORD_IMAGE_BYTES: usize = limits::MAX_PAGE_SIZE as usize;

/// Largest record payload the segment reader will allocate from an on-disk length.
const MAX_SEGMENT_RECORD_SIZE: usize =
    SEGMENT_RECORD_MIN_SIZE + 8 + 2 * MAX_SEGMENT_RECORD_IMAGE_BYTES;

/// fsync policy for segment files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FsyncPolicy {
    /// Full fsync after every write (safest, slowest).
    #[default]
    Full,
    /// Fsync at epoch boundaries only.
    Normal,
    /// No fsync (fastest, least safe).
    Off,
}

/// Segment file header.
///
/// Layout (24 bytes):
/// ```text
/// [0..4]   magic: u32 (0x5057414C = "PWAL")
/// [4..6]   version: u16
/// [6..8]   reserved: u16 (for alignment)
/// [8..16]  epoch: u64
/// [16..20] record_count: u32
/// [20..24] checksum: u32 (CRC32C of header fields 0..20)
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SegmentHeader {
    /// Epoch number for this segment.
    pub epoch: u64,
    /// Number of records in this segment.
    pub record_count: u32,
}

impl SegmentHeader {
    /// Create a new segment header.
    #[must_use]
    pub const fn new(epoch: u64, record_count: u32) -> Self {
        Self {
            epoch,
            record_count,
        }
    }

    /// Serialize the header to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; SEGMENT_HEADER_SIZE] {
        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&SEGMENT_MAGIC.to_le_bytes());
        buf[4..6].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
        // buf[6..8] reserved
        buf[8..16].copy_from_slice(&self.epoch.to_le_bytes());
        buf[16..20].copy_from_slice(&self.record_count.to_le_bytes());
        // Compute CRC32C of bytes 0..20
        let checksum = crc32c::crc32c(&buf[0..20]);
        buf[20..24].copy_from_slice(&checksum.to_le_bytes());
        buf
    }

    /// Parse a header from bytes.
    pub fn from_bytes(buf: &[u8; SEGMENT_HEADER_SIZE]) -> Result<Self, String> {
        let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != SEGMENT_MAGIC {
            return Err(format!("invalid segment magic: {magic:#x}"));
        }
        let version = u16::from_le_bytes([buf[4], buf[5]]);
        if version != SEGMENT_VERSION {
            return Err(format!("unsupported segment version: {version}"));
        }
        let epoch = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        let record_count = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]);
        let stored_checksum = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]);
        let computed_checksum = crc32c::crc32c(&buf[0..20]);
        if stored_checksum != computed_checksum {
            return Err(format!(
                "segment header checksum mismatch: stored={stored_checksum:#x}, computed={computed_checksum:#x}"
            ));
        }
        Ok(Self {
            epoch,
            record_count,
        })
    }
}

/// Generate the segment file path for a given database and epoch.
#[must_use]
pub fn segment_path(db_path: &Path, epoch: u64) -> PathBuf {
    let mut path = db_path.to_path_buf();
    let file_name = path
        .file_name()
        .map_or_else(|| "db".to_string(), |n| n.to_string_lossy().to_string());
    path.set_file_name(format!("{file_name}-wal-seg-{epoch:016x}"));
    path
}

/// List all segment files for a database, sorted by epoch.
pub fn list_segments(db_path: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let db_name = db_path
        .file_name()
        .map_or_else(|| "db".to_string(), |n| n.to_string_lossy().to_string());
    let prefix = format!("{db_name}-wal-seg-");

    let mut segments = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(epoch_hex) = name_str.strip_prefix(&prefix) {
            if let Ok(epoch) = u64::from_str_radix(epoch_hex, 16) {
                segments.push((epoch, entry.path()));
            }
        }
    }
    segments.sort_by_key(|(epoch, _)| *epoch);
    Ok(segments)
}

/// Write a segment file for the given epoch batch.
///
/// The segment file contains:
/// 1. Header with epoch and record count
/// 2. Serialized records (length-prefixed bincode)
///
/// Returns the number of bytes written.
pub fn write_segment(
    db_path: &Path,
    batch: &EpochFlushBatch,
    fsync_policy: FsyncPolicy,
) -> io::Result<usize> {
    let path = segment_path(db_path, batch.epoch);

    let ordered_records = ordered_segment_records(batch.epoch, &batch.records)?;
    for record in &ordered_records {
        validate_segment_record_images(record)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    }
    let record_count = u32::try_from(ordered_records.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "segment record count {} exceeds u32 header field",
                ordered_records.len()
            ),
        )
    })?;

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    let mut writer = BufWriter::new(file);

    // Write header
    let header = SegmentHeader::new(batch.epoch, record_count);
    let header_bytes = header.to_bytes();
    writer.write_all(&header_bytes)?;
    let mut total_bytes = SEGMENT_HEADER_SIZE;

    // Write records in canonical replay order so crash recovery is deterministic.
    for record in &ordered_records {
        let record_bytes =
            serialize_record(record).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let len = u32::try_from(record_bytes.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "segment record length {} exceeds u32 length prefix",
                    record_bytes.len()
                ),
            )
        })?;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(&record_bytes)?;
        total_bytes += 4 + record_bytes.len();
    }

    writer.flush()?;

    // Apply fsync policy
    if fsync_policy == FsyncPolicy::Full || fsync_policy == FsyncPolicy::Normal {
        writer.get_ref().sync_all()?;
    }

    Ok(total_bytes)
}

/// Read a segment file and return the records.
pub fn read_segment(path: &Path) -> io::Result<(SegmentHeader, Vec<WalRecord>)> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Read header
    let mut header_buf = [0u8; SEGMENT_HEADER_SIZE];
    reader.read_exact(&mut header_buf)?;
    let header = SegmentHeader::from_bytes(&header_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let file_len = reader.get_ref().metadata()?.len();
    let body_len = file_len.saturating_sub(SEGMENT_HEADER_SIZE as u64);
    let min_record_on_disk_len = u64::try_from(4 + SEGMENT_RECORD_MIN_SIZE).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "minimum segment record length exceeds u64",
        )
    })?;
    let max_possible_records = body_len / min_record_on_disk_len;
    if u64::from(header.record_count) > max_possible_records {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment record count {} exceeds maximum possible {} for file length {}",
                header.record_count, max_possible_records, file_len
            ),
        ));
    }

    // Read records
    let record_capacity = usize::try_from(header.record_count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment record count {} exceeds addressable size",
                header.record_count
            ),
        )
    })?;
    let mut records = Vec::with_capacity(record_capacity);
    for _ in 0..header.record_count {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > MAX_SEGMENT_RECORD_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("segment record length {len} exceeds maximum {MAX_SEGMENT_RECORD_SIZE}"),
            ));
        }

        let mut record_buf = vec![0u8; len];
        reader.read_exact(&mut record_buf)?;
        let record = deserialize_record(&record_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        records.push(record);
    }
    let consumed_len = reader.stream_position()?;
    if consumed_len != file_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment has {} trailing bytes after declared records",
                file_len.saturating_sub(consumed_len)
            ),
        ));
    }

    Ok((header, ordered_segment_records(header.epoch, &records)?))
}

/// Delete a segment file.
pub fn delete_segment(path: &Path) -> io::Result<()> {
    fs::remove_file(path)
}

// ---------------------------------------------------------------------------
// Segment Recovery (D1.7)
// ---------------------------------------------------------------------------

/// Result of recovering segments for a database.
#[derive(Debug, Clone)]
pub struct SegmentRecoveryResult {
    /// Number of segments recovered.
    pub segments_recovered: usize,
    /// Number of records applied.
    pub records_applied: usize,
    /// Total bytes read from segment files.
    pub bytes_read: u64,
    /// Epochs recovered, in order.
    pub epochs: Vec<u64>,
    /// Any partial segments that were skipped (truncated/corrupt).
    pub partial_segments: Vec<PathBuf>,
    /// Successfully parsed segment files that are eligible for deletion once
    /// their records have been durably applied (GH #192). Populated only when
    /// `delete_after_recovery` was requested; the caller (or
    /// [`delete_recovered_segments`]) removes them **after** apply.
    pub deletable_segments: Vec<PathBuf>,
}

/// Options for segment recovery.
#[derive(Debug, Clone, Copy, Default)]
pub struct SegmentRecoveryOptions {
    /// Mark successfully parsed segment files for deletion once their records
    /// have been applied. Segments are **never** removed by
    /// [`recover_segments`] itself (GH #192): parsed-but-unapplied records
    /// exist only in memory, so deleting the durable segment before apply
    /// creates a crash window that silently loses committed data. The
    /// eligible paths are returned in
    /// [`SegmentRecoveryResult::deletable_segments`], and the caller invokes
    /// [`delete_recovered_segments`] once the applied state is durable.
    pub delete_after_recovery: bool,
    /// Stop at the first corrupt segment and return the durable prefix instead
    /// of failing the whole recovery.
    pub skip_corrupt: bool,
}

/// Recover all segments for a database.
///
/// This function:
/// 1. Finds all segment files for the database.
/// 2. Sorts them by epoch (ascending).
/// 3. Reads and returns records from each segment.
/// 4. Optionally deletes segments after recovery.
///
/// The caller is responsible for applying records to the database
/// (updating page contents based on after_images).
pub fn recover_segments(
    db_path: &Path,
    options: SegmentRecoveryOptions,
) -> io::Result<(SegmentRecoveryResult, Vec<WalRecord>)> {
    let segments = list_segments(db_path)?;

    let mut result = SegmentRecoveryResult {
        segments_recovered: 0,
        records_applied: 0,
        bytes_read: 0,
        epochs: Vec::with_capacity(segments.len()),
        partial_segments: Vec::new(),
        deletable_segments: Vec::new(),
    };

    let mut all_records = Vec::new();

    for (segment_index, (epoch, path)) in segments.iter().enumerate() {
        // Get file size for byte tracking
        let metadata = fs::metadata(path)?;
        let file_size = metadata.len();

        // Try to read the segment
        match read_segment(path) {
            Ok((header, records)) => {
                if header.epoch != *epoch {
                    let error = io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "segment {} has mismatched epoch: header={}, filename={}",
                            path.display(),
                            header.epoch,
                            epoch
                        ),
                    );
                    if options.skip_corrupt {
                        eprintln!(
                            "warning: stopping recovery at corrupt segment {}: {error}",
                            path.display()
                        );
                        result.partial_segments.extend(
                            segments[segment_index..]
                                .iter()
                                .map(|(_, path)| path.clone()),
                        );
                        break;
                    }
                    return Err(error);
                }

                result.segments_recovered += 1;
                result.records_applied += records.len();
                result.bytes_read += file_size;
                result.epochs.push(*epoch);

                all_records.extend(records);
            }
            Err(e) => {
                if options.skip_corrupt {
                    eprintln!(
                        "warning: stopping recovery at corrupt segment {}: {e}",
                        path.display()
                    );
                    result.partial_segments.extend(
                        segments[segment_index..]
                            .iter()
                            .map(|(_, path)| path.clone()),
                    );
                    break;
                }
                return Err(e);
            }
        }
    }

    // GH #192: segments must stay durable until their records are applied.
    // Deleting here would leave parsed records only in memory — a crash
    // between this point and apply would silently discard committed data.
    // Instead, expose the deletable set for the caller's post-apply cleanup.
    if options.delete_after_recovery {
        result.deletable_segments = segments
            .iter()
            .filter(|(_, path)| !result.partial_segments.contains(path))
            .map(|(_, path)| path.clone())
            .collect();
    }

    Ok((result, EpochOrderCoordinator::recovery_order(&all_records)))
}

/// Delete segment files that a completed recovery marked as deletable.
///
/// Call this only **after** every recovered record has been durably applied
/// to page state (GH #192). Deletion failures are non-fatal: an undeleted
/// segment is re-parsed on the next recovery, which is idempotent.
pub fn delete_recovered_segments(result: &SegmentRecoveryResult) {
    for path in &result.deletable_segments {
        if let Err(e) = delete_segment(path) {
            tracing::warn!(
                segment = %path.display(),
                error = %e,
                "failed to delete recovered segment; it will be re-parsed on the next recovery"
            );
        }
    }
}

fn ordered_segment_records(epoch: u64, records: &[WalRecord]) -> io::Result<Vec<WalRecord>> {
    let ordered = EpochOrderCoordinator::recovery_order(records);
    if let Some(record) = ordered.iter().find(|record| record.epoch != epoch) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment epoch {epoch} contains record from epoch {}",
                record.epoch
            ),
        ));
    }
    Ok(ordered)
}

/// Recover segments and apply records to a page cache.
///
/// This is a higher-level recovery function that takes a mutable page
/// map and applies after_images from recovered records. It returns
/// the recovery result and the final page contents.
///
/// The page_contents map is keyed by page number and contains the
/// current contents of each page. Records are applied in epoch order.
///
/// With `delete_after_recovery`, eligible segment paths are returned in
/// [`SegmentRecoveryResult::deletable_segments`] but are **not** removed:
/// the map filled here is in-memory state, so the caller must first make
/// the applied pages durable and then call [`delete_recovered_segments`]
/// (GH #192).
pub fn recover_and_apply_segments(
    db_path: &Path,
    page_contents: &mut HashMap<u32, Vec<u8>, impl BuildHasher>,
    options: SegmentRecoveryOptions,
) -> io::Result<SegmentRecoveryResult> {
    let (result, records) = recover_segments(db_path, options)?;

    // Apply records in order (they're already sorted by epoch)
    for record in records {
        let page_id = record.page_id.get();
        if !record.after_image.is_empty() {
            page_contents.insert(page_id, record.after_image);
        }
    }

    // GH #192: segments are NOT deleted here. `page_contents` is an
    // in-memory map — deleting the segments now would make it the sole copy
    // of committed data, recreating the crash window this fix closes. The
    // caller must persist the applied state durably and only then call
    // [`delete_recovered_segments`] with this result.
    Ok(result)
}

/// Get the maximum durable epoch from existing segment files.
///
/// This can be used to determine the recovery point after a crash.
/// Returns None if no segment files exist.
pub fn max_durable_epoch(db_path: &Path) -> io::Result<Option<u64>> {
    let segments = list_segments(db_path)?;
    Ok(segments.last().map(|(epoch, _)| *epoch))
}

/// Clean up all segment files for a database.
///
/// This should be called after checkpoint when segments are no longer needed.
pub fn cleanup_segments(db_path: &Path) -> io::Result<usize> {
    let segments = list_segments(db_path)?;
    let count = segments.len();
    for (_, path) in segments {
        delete_segment(&path)?;
    }
    Ok(count)
}

/// Serialize a WalRecord to bytes.
fn serialize_record(record: &WalRecord) -> Result<Vec<u8>, String> {
    // Simple binary format:
    // [8] txn_id
    // [4] txn_epoch
    // [8] record_epoch
    // [4] page_id
    // [8] begin_seq
    // [1] has_end_seq
    // [8] end_seq (if has_end_seq)
    // [4] before_image_len
    // [N] before_image
    // [4] after_image_len
    // [N] after_image
    validate_segment_record_images(record)?;
    let before_len = u32::try_from(record.before_image.len())
        .map_err(|_| "before_image length exceeds u32 length prefix".to_string())?;
    let after_len = u32::try_from(record.after_image.len())
        .map_err(|_| "after_image length exceeds u32 length prefix".to_string())?;

    let mut buf = Vec::with_capacity(64 + record.before_image.len() + record.after_image.len());

    buf.extend_from_slice(&record.txn_token.id.get().to_le_bytes());
    buf.extend_from_slice(&record.txn_token.epoch.get().to_le_bytes());
    buf.extend_from_slice(&record.epoch.to_le_bytes());
    buf.extend_from_slice(&record.page_id.get().to_le_bytes());
    buf.extend_from_slice(&record.begin_seq.get().to_le_bytes());
    if let Some(end_seq) = record.end_seq {
        buf.push(1);
        buf.extend_from_slice(&end_seq.get().to_le_bytes());
    } else {
        buf.push(0);
    }
    buf.extend_from_slice(&before_len.to_le_bytes());
    buf.extend_from_slice(&record.before_image);
    buf.extend_from_slice(&after_len.to_le_bytes());
    buf.extend_from_slice(&record.after_image);

    Ok(buf)
}

fn validate_segment_record_images(record: &WalRecord) -> Result<(), String> {
    validate_segment_image_len("before_image", record.before_image.len())?;
    validate_segment_image_len("after_image", record.after_image.len())
}

fn validate_segment_image_len(field: &'static str, len: usize) -> Result<(), String> {
    if len > MAX_SEGMENT_RECORD_IMAGE_BYTES {
        return Err(format!(
            "{field} length {len} exceeds maximum {MAX_SEGMENT_RECORD_IMAGE_BYTES}"
        ));
    }
    Ok(())
}

fn read_record_bytes<'a>(
    buf: &'a [u8],
    offset: &mut usize,
    len: usize,
    field: &'static str,
) -> Result<&'a [u8], String> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| format!("{field} offset overflow"))?;
    let bytes = buf
        .get(*offset..end)
        .ok_or_else(|| format!("{field} truncated"))?;
    *offset = end;
    Ok(bytes)
}

fn read_record_u32(buf: &[u8], offset: &mut usize, field: &'static str) -> Result<u32, String> {
    let bytes = read_record_bytes(buf, offset, 4, field)?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_record_u64(buf: &[u8], offset: &mut usize, field: &'static str) -> Result<u64, String> {
    let bytes = read_record_bytes(buf, offset, 8, field)?;
    Ok(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

/// Deserialize a WalRecord from bytes.
fn deserialize_record(buf: &[u8]) -> Result<WalRecord, String> {
    if buf.len() < SEGMENT_RECORD_MIN_SIZE {
        return Err("record too short".to_string());
    }

    let mut offset = 0;

    let txn_id = read_record_u64(buf, &mut offset, "txn_id")?;
    let txn_epoch = read_record_u32(buf, &mut offset, "txn_epoch")?;
    let record_epoch = read_record_u64(buf, &mut offset, "record_epoch")?;
    let page_id = read_record_u32(buf, &mut offset, "page_id")?;
    let begin_seq = read_record_u64(buf, &mut offset, "begin_seq")?;
    let has_end_seq = *read_record_bytes(buf, &mut offset, 1, "end_seq flag")?
        .first()
        .ok_or_else(|| "end_seq flag truncated".to_string())?;
    let end_seq = if has_end_seq == 1 {
        let seq = read_record_u64(buf, &mut offset, "end_seq")?;
        Some(CommitSeq::new(seq))
    } else if has_end_seq == 0 {
        None
    } else {
        return Err(format!("invalid end_seq flag: {has_end_seq}"));
    };
    let before_len = read_record_u32(buf, &mut offset, "before_image length")? as usize;
    validate_segment_image_len("before_image", before_len)?;
    let before_image = read_record_bytes(buf, &mut offset, before_len, "before_image")?.to_vec();
    let after_len = read_record_u32(buf, &mut offset, "after_image length")? as usize;
    validate_segment_image_len("after_image", after_len)?;
    let after_image = read_record_bytes(buf, &mut offset, after_len, "after_image")?.to_vec();
    if offset != buf.len() {
        return Err(format!(
            "trailing bytes after WAL record: {}",
            buf.len().saturating_sub(offset)
        ));
    }

    let txn_id = fsqlite_types::TxnId::new(txn_id).ok_or("invalid txn_id (zero)")?;
    let page_id = PageNumber::new(page_id).ok_or("invalid page_id (zero)")?;

    Ok(WalRecord {
        txn_token: TxnToken::new(txn_id, fsqlite_types::TxnEpoch::new(txn_epoch)),
        epoch: record_epoch,
        page_id,
        begin_seq: CommitSeq::new(begin_seq),
        end_seq,
        before_image,
        after_image,
    })
}

// ---------------------------------------------------------------------------
// WAL Frame for Parallel Submission
// ---------------------------------------------------------------------------

/// A WAL frame submitted for parallel writing.
#[derive(Debug, Clone)]
pub struct ParallelWalFrame {
    /// Page number.
    pub page_number: PageNumber,
    /// Page data (owned copy for buffering).
    pub page_data: Vec<u8>,
    /// Database size in pages for commit frames, or 0 for non-commit frames.
    pub db_size_if_commit: u32,
}

/// A batch of WAL frames from a single transaction.
#[derive(Debug, Clone)]
pub struct ParallelWalBatch {
    /// Transaction token identifying this batch.
    pub txn_token: TxnToken,
    /// Commit sequence assigned to this batch.
    pub commit_seq: CommitSeq,
    /// Frames in write order.
    pub frames: Vec<ParallelWalFrame>,
}

impl ParallelWalBatch {
    /// Create a new batch from the given frames.
    #[must_use]
    pub fn new(txn_token: TxnToken, commit_seq: CommitSeq, frames: Vec<ParallelWalFrame>) -> Self {
        Self {
            txn_token,
            commit_seq,
            frames,
        }
    }
}

// ---------------------------------------------------------------------------
// Parallel WAL Coordinator
// ---------------------------------------------------------------------------

/// Per-database parallel WAL coordinator.
///
/// This coordinator manages per-thread WAL buffers and epoch-based flushing.
/// It replaces the global WAL append mutex with lock-free per-thread appends.
pub struct ParallelWalCoordinator {
    /// The epoch-based buffer coordinator (Arc for ticker thread sharing).
    inner: Arc<EpochOrderCoordinator>,
    /// Path to the database (for segment file naming).
    db_path: PathBuf,
    /// Configuration.
    config: ParallelWalConfig,
    /// Whether the coordinator is running (Arc for ticker thread sharing).
    running: Arc<AtomicBool>,
    /// Epoch batches drained from memory but not yet durably written.
    pending_batches: Arc<Mutex<VecDeque<EpochFlushBatch>>>,
    /// Child cancellation scope for the background ticker task.
    ticker_cx: Mutex<Option<Cx>>,
    /// Epoch ticker handle (spawned on an asupersync runtime).
    #[cfg(not(target_arch = "wasm32"))]
    ticker_handle: Mutex<Option<BlockingTaskHandle>>,
}

impl std::fmt::Debug for ParallelWalCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelWalCoordinator")
            .field("db_path", &self.db_path)
            .field("config", &self.config)
            .field("running", &self.running.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ParallelWalCoordinator {
    /// Create a new parallel WAL coordinator for the given database path.
    #[must_use]
    pub fn new(db_path: &Path, config: ParallelWalConfig) -> Self {
        let buffer_config = BufferConfig {
            capacity_bytes: config.buffer_capacity_bytes,
            ..BufferConfig::default()
        };
        let epoch_config = EpochConfig {
            advance_interval_ms: config.epoch_interval_ms,
        };

        Self {
            inner: Arc::new(EpochOrderCoordinator::new(
                config.slot_count,
                buffer_config,
                epoch_config,
            )),
            db_path: db_path.to_path_buf(),
            config,
            running: Arc::new(AtomicBool::new(false)),
            pending_batches: Arc::new(Mutex::new(VecDeque::new())),
            ticker_cx: Mutex::new(None),
            #[cfg(not(target_arch = "wasm32"))]
            ticker_handle: Mutex::new(None),
        }
    }

    /// Get the current epoch.
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.inner.current_epoch()
    }

    /// Get the durable epoch (all epochs <= this are guaranteed durable).
    #[must_use]
    pub fn durable_epoch(&self) -> Option<u64> {
        self.inner.durable_epoch()
    }

    /// Get the buffer slot index for the current thread.
    #[must_use]
    pub fn thread_slot(&self) -> usize {
        thread_buffer_slot(self.config.slot_count)
    }

    /// Submit a WAL frame batch for the current thread.
    ///
    /// This method appends the batch's frames to the current thread's buffer
    /// with NO global lock. The batch will be flushed when the epoch advances.
    ///
    /// Returns the epoch in which the batch was submitted.
    pub fn submit_batch(&self, batch: ParallelWalBatch) -> Result<u64, String> {
        let slot = self.thread_slot();
        let epoch = self.inner.current_append_epoch();
        let records = batch
            .frames
            .into_iter()
            .map(|frame| WalRecord {
                txn_token: batch.txn_token,
                epoch,
                page_id: frame.page_number,
                begin_seq: batch.commit_seq,
                end_seq: Some(batch.commit_seq),
                before_image: Vec::new(), // WAL frames don't have before images
                after_image: frame.page_data,
            })
            .collect();

        let outcome = self.inner.append_records_to_core(slot, records)?;
        if matches!(outcome, AppendOutcome::Blocked) {
            return Err("buffer blocked, fallback to serialized path".to_string());
        }

        Ok(epoch)
    }

    /// Wait until the given epoch is durable.
    ///
    /// This method blocks until all frames submitted in or before `epoch`
    /// have been flushed to disk.
    pub fn wait_for_epoch_durable(&self, epoch: u64, timeout: Duration) -> Result<(), String> {
        self.inner.wait_until_epoch_durable(epoch, timeout)
    }

    /// Start the background epoch ticker on a caller-owned asupersync runtime.
    ///
    /// The ticker task advances the epoch at the configured interval (default 10ms),
    /// sealing and flushing all per-thread buffers. This implements the Silo/Aether
    /// group commit pattern where transactions wait for their epoch to become durable.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_on_runtime(&self, runtime: &RuntimeHandle, parent_cx: &Cx) -> Result<(), String> {
        self.start_on_runtime_with_fsync(runtime, parent_cx, FsyncPolicy::default())
    }

    /// Start the background epoch ticker with a specific fsync policy.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_on_runtime_with_fsync(
        &self,
        runtime: &RuntimeHandle,
        parent_cx: &Cx,
        fsync_policy: FsyncPolicy,
    ) -> Result<(), String> {
        if self.running.load(Ordering::Acquire) {
            return Err("coordinator already running".to_string());
        }

        let prior_ticker_cx = self
            .ticker_cx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(ticker_cx) = prior_ticker_cx {
            ticker_cx.cancel();
        }
        let prior_handle = self
            .ticker_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(handle) = prior_handle {
            handle.wait();
        }
        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err("coordinator already running".to_string());
        }

        let ticker_cx = parent_cx.create_child();

        // Clone Arc handles for the ticker task.
        let running = Arc::clone(&self.running);
        let inner = Arc::clone(&self.inner);
        let db_path = self.db_path.clone();
        let pending_batches = Arc::clone(&self.pending_batches);
        let interval = Duration::from_millis(self.config.epoch_interval_ms);
        let flush_timeout = Duration::from_millis(self.config.epoch_interval_ms * 10);
        let loop_cx = ticker_cx.clone();

        let Some(handle) = runtime.spawn_blocking(move || {
            epoch_ticker_loop(
                running,
                inner,
                db_path,
                pending_batches,
                interval,
                flush_timeout,
                fsync_policy,
                loop_cx,
            );
        }) else {
            self.running.store(false, Ordering::Release);
            return Err(
                "failed to spawn epoch ticker task: runtime has no blocking pool".to_string(),
            );
        };

        *self
            .ticker_cx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ticker_cx);

        let mut ticker_handle = self
            .ticker_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *ticker_handle = Some(handle);

        Ok(())
    }

    /// Stop the background epoch ticker task.
    ///
    /// Signals the ticker to stop and waits for it to complete its current
    /// flush cycle before returning.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
        let prior_ticker_cx = self
            .ticker_cx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(ticker_cx) = prior_ticker_cx {
            ticker_cx.cancel();
        }

        #[cfg(not(target_arch = "wasm32"))]
        let mut handle = self
            .ticker_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(h) = handle.take() {
            h.wait();
        }
    }

    /// Check if the background epoch ticker is running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Manually advance the epoch and flush all buffers.
    ///
    /// This is used for testing or when no background ticker is running.
    pub fn advance_and_flush(&self, timeout: Duration) -> Result<u64, String> {
        flush_pending_batches(
            &self.pending_batches,
            &self.inner,
            &self.db_path,
            FsyncPolicy::default(),
        )?;

        // Slot-level buffer locks serialize a batch append against sealing, so
        // the top-level coordinator can advance without waiting on inactive slots.
        let new_epoch = self.inner.advance_epoch_and_wait(&[], timeout)?;

        let prev_epoch = new_epoch.saturating_sub(1);
        let batch = self.inner.flush_epoch(prev_epoch)?;
        if batch.records.is_empty() {
            self.inner.mark_epoch_durable(prev_epoch);
        } else {
            enqueue_flush_batch(&self.pending_batches, batch);
            flush_pending_batches(
                &self.pending_batches,
                &self.inner,
                &self.db_path,
                FsyncPolicy::default(),
            )?;
        }

        Ok(new_epoch)
    }
}

impl Drop for ParallelWalCoordinator {
    fn drop(&mut self) {
        self.stop();
    }
}

fn enqueue_flush_batch(
    pending_batches: &Arc<Mutex<VecDeque<EpochFlushBatch>>>,
    batch: EpochFlushBatch,
) {
    let mut pending = pending_batches
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    pending.push_back(batch);
}

fn flush_pending_batches(
    pending_batches: &Arc<Mutex<VecDeque<EpochFlushBatch>>>,
    inner: &EpochOrderCoordinator,
    db_path: &Path,
    fsync_policy: FsyncPolicy,
) -> Result<(), String> {
    loop {
        let next_batch = {
            let mut pending = pending_batches
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pending.pop_front()
        };

        let Some(batch) = next_batch else {
            return Ok(());
        };

        if let Err(error) = write_segment(db_path, &batch, fsync_policy) {
            let epoch = batch.epoch;
            let mut pending = pending_batches
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pending.push_front(batch);
            return Err(format!("write_segment({epoch}) failed: {error}"));
        }

        inner.mark_epoch_durable(batch.epoch);
    }
}

// ---------------------------------------------------------------------------
// Epoch Ticker Loop
// ---------------------------------------------------------------------------

/// Background task loop that advances epochs and flushes WAL buffers.
///
/// This implements an epoch-based group commit pattern:
/// 1. Sleep for the configured interval (default 10ms).
/// 2. Advance the global epoch.
/// 3. Flush any prior pending segment writes.
/// 4. Seal and drain the previous epoch's buffers.
/// 5. Write the batch to a segment file.
/// 6. Mark the epoch as durable.
///
/// The loop exits when `running` is cleared or the task `Cx` is cancelled.
#[allow(clippy::too_many_arguments)]
fn epoch_ticker_loop(
    running: Arc<AtomicBool>,
    inner: Arc<EpochOrderCoordinator>,
    db_path: PathBuf,
    pending_batches: Arc<Mutex<VecDeque<EpochFlushBatch>>>,
    interval: Duration,
    flush_timeout: Duration,
    fsync_policy: FsyncPolicy,
    ticker_cx: Cx,
) {
    while running.load(Ordering::Acquire) {
        if ticker_cx.checkpoint().is_err() {
            break;
        }

        // Sleep for the epoch interval.
        std::thread::sleep(interval);

        // Check if we should stop before doing work.
        if !running.load(Ordering::Acquire) || ticker_cx.is_cancel_requested() {
            break;
        }

        if let Err(error) = flush_pending_batches(&pending_batches, &inner, &db_path, fsync_policy)
        {
            eprintln!("epoch ticker: {error}");
            continue;
        }

        // Slot-level buffer locking makes batch submission atomic relative to sealing,
        // so we can advance without stalling on globally inactive slots.
        match inner.advance_epoch_and_wait(&[], flush_timeout) {
            Ok(new_epoch) => {
                let prev_epoch = new_epoch.saturating_sub(1);
                match inner.flush_epoch(prev_epoch) {
                    Ok(batch) => {
                        if batch.records.is_empty() {
                            inner.mark_epoch_durable(prev_epoch);
                        } else {
                            enqueue_flush_batch(&pending_batches, batch);
                            if let Err(error) = flush_pending_batches(
                                &pending_batches,
                                &inner,
                                &db_path,
                                fsync_policy,
                            ) {
                                eprintln!("epoch ticker: {error}");
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!("epoch ticker: flush_epoch({prev_epoch}) failed: {error}");
                    }
                }
            }
            Err(error) => {
                eprintln!("epoch ticker: advance_epoch_and_wait failed: {error}");
            }
        }
    }

    running.store(false, Ordering::Release);
}

// ---------------------------------------------------------------------------
// Global Coordinators Registry
// ---------------------------------------------------------------------------

type CoordinatorRef = Arc<ParallelWalCoordinator>;

static PARALLEL_WAL_COORDINATORS: OnceLock<Mutex<HashMap<PathBuf, CoordinatorRef>>> =
    OnceLock::new();

/// Get or create a parallel WAL coordinator for the given database path.
pub fn parallel_wal_coordinator_for_path(db_path: &Path) -> CoordinatorRef {
    let coordinators = PARALLEL_WAL_COORDINATORS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut coordinators = coordinators
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    Arc::clone(
        coordinators
            .entry(db_path.to_path_buf())
            .or_insert_with(|| {
                Arc::new(ParallelWalCoordinator::new(
                    db_path,
                    ParallelWalConfig::default(),
                ))
            }),
    )
}

/// Remove a parallel WAL coordinator for the given database path.
pub fn remove_parallel_wal_coordinator(db_path: &Path) {
    if let Some(coordinators) = PARALLEL_WAL_COORDINATORS.get() {
        let mut coordinators = coordinators
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(coordinator) = coordinators.remove(db_path) {
            coordinator.stop();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::runtime::RuntimeBuilder;
    use std::path::PathBuf;
    use std::sync::{LazyLock, Mutex, MutexGuard};

    use crate::per_core_buffer::reset_slot_counter;

    static PARALLEL_WAL_LANE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn lane_test_guard() -> MutexGuard<'static, ()> {
        PARALLEL_WAL_LANE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn test_runtime() -> asupersync::runtime::Runtime {
        RuntimeBuilder::current_thread()
            .blocking_threads(1, 1)
            .build()
            .expect("runtime should build")
    }

    fn test_cx() -> Cx {
        Cx::default()
    }

    fn sample_batch(txn_id: u64, commit_seq: u64) -> ParallelWalBatch {
        ParallelWalBatch::new(
            TxnToken::new(
                fsqlite_types::TxnId::new(txn_id).expect("txn id should be non-zero"),
                fsqlite_types::TxnEpoch::new(0),
            ),
            CommitSeq::new(commit_seq),
            vec![
                ParallelWalFrame {
                    page_number: PageNumber::new(7).expect("page should be non-zero"),
                    page_data: vec![0xAA; 16],
                    db_size_if_commit: 0,
                },
                ParallelWalFrame {
                    page_number: PageNumber::new(9).expect("page should be non-zero"),
                    page_data: vec![0xBB; 24],
                    db_size_if_commit: 12,
                },
            ],
        )
    }

    fn sample_lane_batch(
        batch_id: u64,
        lane_id: u16,
        staged_frame_count: u32,
        payload: u32,
    ) -> ParallelWalLaneBatch<u32> {
        ParallelWalLaneBatch {
            batch_id,
            lane_id,
            staged_frame_count,
            staging_elapsed_ns: u64::from(staged_frame_count) * 10,
            shadow_verdict: ParallelWalShadowVerdict::NotRun,
            payload,
        }
    }

    fn sample_lane_context(batch_id: u64, lane_id: u16) -> TransactionFrameBatchContext {
        TransactionFrameBatchContext {
            batch_id,
            lane_id,
            staged_frame_count: 1,
            staging_elapsed_ns: 10,
        }
    }

    #[test]
    fn test_parallel_wal_coordinator_creation() {
        let path = PathBuf::from("/tmp/test.db");
        let coordinator = ParallelWalCoordinator::new(&path, ParallelWalConfig::default());

        assert_eq!(coordinator.current_epoch(), 0);
        assert_eq!(coordinator.durable_epoch(), None);
    }

    #[test]
    fn test_thread_slot_assignment() {
        let _guard = lane_test_guard();
        let path = PathBuf::from("/tmp/test.db");
        let config = ParallelWalConfig {
            slot_count: 4,
            ..ParallelWalConfig::default()
        };
        let coordinator = ParallelWalCoordinator::new(&path, config);

        // Thread slot should be consistent for the same thread.
        let slot1 = coordinator.thread_slot();
        let slot2 = coordinator.thread_slot();
        assert_eq!(slot1, slot2);
        assert!(slot1 < 4);
    }

    #[test]
    fn test_lane_stager_identity_is_stable_within_thread() {
        let _guard = lane_test_guard();
        let stager = ParallelWalLaneStager::<u32>::new(ParallelWalControlSurface {
            mode: ParallelWalOperatingMode::Auto,
            lane_count_override: Some(4),
            ..ParallelWalControlSurface::default()
        });

        let first = stager.current_lane_id();
        let second = stager.current_lane_id();
        assert_eq!(first, second);
        assert!(usize::from(first) < 4);
    }

    #[test]
    fn test_lane_stager_reuses_lanes_after_worker_churn() {
        let _guard = lane_test_guard();
        reset_slot_counter();

        let stager = Arc::new(ParallelWalLaneStager::<u32>::new(
            ParallelWalControlSurface {
                mode: ParallelWalOperatingMode::Auto,
                lane_count_override: Some(2),
                ..ParallelWalControlSurface::default()
            },
        ));

        let spawn_wave = || {
            let mut lanes = Vec::new();
            for _ in 0..2 {
                let stager = Arc::clone(&stager);
                lanes.push(std::thread::spawn(move || stager.current_lane_id()));
            }
            let mut observed = lanes
                .into_iter()
                .map(|handle| handle.join().expect("lane thread should join"))
                .collect::<Vec<_>>();
            observed.sort_unstable();
            observed
        };

        assert_eq!(spawn_wave(), vec![0, 1]);
        assert_eq!(spawn_wave(), vec![0, 1]);
    }

    #[test]
    fn test_lane_stager_conservative_mode_collapses_to_single_lane() {
        let _guard = lane_test_guard();
        let stager = Arc::new(ParallelWalLaneStager::<u32>::new(
            ParallelWalControlSurface {
                mode: ParallelWalOperatingMode::Conservative,
                lane_count_override: Some(8),
                ..ParallelWalControlSurface::default()
            },
        ));

        assert_eq!(stager.lane_count(), 1);

        let mut lanes = Vec::new();
        for _ in 0..2 {
            let stager = Arc::clone(&stager);
            lanes.push(std::thread::spawn(move || stager.current_lane_id()));
        }

        let observed = lanes
            .into_iter()
            .map(|handle| handle.join().expect("lane thread should join"))
            .collect::<Vec<_>>();
        assert_eq!(observed, vec![0, 0]);
    }

    #[test]
    fn test_lane_stager_clamps_lane_count_to_lane_id_range() {
        let _guard = lane_test_guard();
        let stager = ParallelWalLaneStager::<u32>::new(ParallelWalControlSurface {
            mode: ParallelWalOperatingMode::Auto,
            lane_count_override: Some(MAX_PARALLEL_WAL_LANE_COUNT + 1),
            ..ParallelWalControlSurface::default()
        });

        assert_eq!(stager.lane_count(), MAX_PARALLEL_WAL_LANE_COUNT);
        assert_eq!(stager.lane_count(), usize::from(u16::MAX));
        assert!(usize::from(stager.current_lane_id()) < stager.lane_count());
    }

    #[test]
    fn test_lane_stager_same_lane_order_mismatch_returns_none_without_drain() {
        let stager = ParallelWalLaneStager::<u32>::new(ParallelWalControlSurface {
            mode: ParallelWalOperatingMode::Auto,
            lane_count_override: Some(2),
            ..ParallelWalControlSurface::default()
        });

        assert_eq!(stager.record_batch(sample_lane_batch(10, 0, 1, 10)), 1);
        assert_eq!(stager.record_batch(sample_lane_batch(11, 0, 1, 11)), 2);

        let out_of_order = [sample_lane_context(11, 0), sample_lane_context(10, 0)];
        assert!(stager.take_batches_for_flush(&out_of_order).is_none());
        assert_eq!(stager.current_lane_backlog(0), 2);

        let in_order = [sample_lane_context(10, 0), sample_lane_context(11, 0)];
        let drained = stager
            .take_batches_for_flush(&in_order)
            .expect("verified in-order batches should drain");
        assert_eq!(drained.len(), 2);
        assert_eq!(drained.get(&10).map(|batch| batch.payload), Some(10));
        assert_eq!(drained.get(&11).map(|batch| batch.payload), Some(11));
        assert_eq!(stager.current_lane_backlog(0), 0);
    }

    #[test]
    fn test_lane_stager_discard_batches_for_flush_removes_stale_payloads() {
        let stager = ParallelWalLaneStager::<u32>::new(ParallelWalControlSurface {
            mode: ParallelWalOperatingMode::Auto,
            lane_count_override: Some(2),
            ..ParallelWalControlSurface::default()
        });

        assert_eq!(stager.record_batch(sample_lane_batch(10, 0, 2, 10)), 2);
        assert_eq!(stager.record_batch(sample_lane_batch(11, 0, 3, 11)), 5);
        assert_eq!(stager.record_batch(sample_lane_batch(12, 0, 5, 12)), 10);

        assert_eq!(
            stager.discard_batches_for_flush(&[sample_lane_context(11, 0)]),
            1
        );
        assert_eq!(
            stager.current_lane_backlog(0),
            7,
            "discarding a stale middle batch must subtract its staged frames without disturbing retained payloads"
        );

        let retained = [sample_lane_context(10, 0), sample_lane_context(12, 0)];
        let drained = stager
            .take_batches_for_flush(&retained)
            .expect("discarded stale payload should not block later retained batches");
        assert_eq!(drained.len(), 2);
        assert_eq!(drained.get(&10).map(|batch| batch.payload), Some(10));
        assert_eq!(drained.get(&12).map(|batch| batch.payload), Some(12));
        assert_eq!(stager.current_lane_backlog(0), 0);
    }

    #[test]
    fn test_lane_stager_discard_batches_for_flush_is_idempotent() {
        let stager = ParallelWalLaneStager::<u32>::new(ParallelWalControlSurface {
            mode: ParallelWalOperatingMode::Auto,
            lane_count_override: Some(2),
            ..ParallelWalControlSurface::default()
        });

        assert_eq!(stager.record_batch(sample_lane_batch(20, 1, 4, 20)), 4);
        let context = [sample_lane_context(20, 1)];
        assert_eq!(stager.discard_batches_for_flush(&context), 1);
        assert_eq!(stager.current_lane_backlog(1), 0);
        assert_eq!(
            stager.discard_batches_for_flush(&context),
            0,
            "discarding an already-flushed raw fallback batch should be a no-op"
        );
        assert_eq!(stager.current_lane_backlog(1), 0);
    }

    #[test]
    fn test_lane_stager_discard_batches_for_flush_ignores_unknown_ids() {
        let stager = ParallelWalLaneStager::<u32>::new(ParallelWalControlSurface {
            mode: ParallelWalOperatingMode::Auto,
            lane_count_override: Some(2),
            ..ParallelWalControlSurface::default()
        });

        assert_eq!(stager.record_batch(sample_lane_batch(30, 0, 2, 30)), 2);
        assert_eq!(
            stager.discard_batches_for_flush(&[sample_lane_context(99, 0)]),
            0
        );
        assert_eq!(stager.current_lane_backlog(0), 2);

        let drained = stager
            .take_batches_for_flush(&[sample_lane_context(30, 0)])
            .expect("unknown discard must not perturb queued batches");
        assert_eq!(drained.get(&30).map(|batch| batch.payload), Some(30));
        assert_eq!(stager.current_lane_backlog(0), 0);
    }

    #[test]
    fn test_auto_shadow_compare_sampling_is_deterministic_by_batch_window() {
        let control = ParallelWalControlSurface {
            mode: ParallelWalOperatingMode::Auto,
            shadow_compare_sampling_per_mille: Some(2),
            ..ParallelWalControlSurface::default()
        };

        assert!(parallel_wal_should_shadow_compare(&control, 1));
        assert!(parallel_wal_should_shadow_compare(&control, 2));
        assert!(!parallel_wal_should_shadow_compare(&control, 3));
        assert!(parallel_wal_should_shadow_compare(&control, 1_001));
        assert!(parallel_wal_should_shadow_compare(&control, 1_002));
        assert!(!parallel_wal_should_shadow_compare(&control, 1_003));
    }

    #[test]
    fn test_shadow_compare_mode_ignores_sampling_gate() {
        let control = ParallelWalControlSurface {
            mode: ParallelWalOperatingMode::ShadowCompare,
            shadow_compare_sampling_per_mille: Some(0),
            ..ParallelWalControlSurface::default()
        };

        assert!(parallel_wal_should_shadow_compare(&control, 1));
        assert!(parallel_wal_should_shadow_compare(&control, 7));
    }

    #[test]
    fn test_conservative_mode_never_runs_shadow_compare_sampling() {
        let control = ParallelWalControlSurface {
            mode: ParallelWalOperatingMode::Conservative,
            shadow_compare_sampling_per_mille: Some(1_000),
            ..ParallelWalControlSurface::default()
        };

        assert!(!parallel_wal_should_shadow_compare(&control, 1));
        assert!(!parallel_wal_should_shadow_compare(&control, 1_000));
    }

    #[test]
    fn test_global_coordinator_registry() {
        let path = PathBuf::from("/tmp/test_registry.db");
        let coord1 = parallel_wal_coordinator_for_path(&path);
        let coord2 = parallel_wal_coordinator_for_path(&path);

        // Should return the same coordinator.
        assert!(Arc::ptr_eq(&coord1, &coord2));

        // Cleanup.
        remove_parallel_wal_coordinator(&path);
    }

    #[test]
    fn test_epoch_ticker_start_stop() {
        let path = PathBuf::from("/tmp/test_ticker.db");
        let config = ParallelWalConfig {
            slot_count: 4,
            epoch_interval_ms: 5, // Fast interval for testing
            ..ParallelWalConfig::default()
        };
        let coordinator = ParallelWalCoordinator::new(&path, config);
        let runtime = test_runtime();
        let cx = test_cx();

        // Initially not running.
        assert!(!coordinator.is_running());

        // Start the ticker.
        coordinator
            .start_on_runtime(&runtime.handle(), &cx)
            .expect("start should succeed");
        assert!(coordinator.is_running());

        // Starting again should fail.
        assert!(
            coordinator
                .start_on_runtime(&runtime.handle(), &cx)
                .is_err()
        );

        // Let the ticker run for a few epochs.
        std::thread::sleep(Duration::from_millis(25));

        // Epoch should be accessible (exact count depends on timing).
        let _epoch = coordinator.current_epoch();

        // Stop the ticker.
        coordinator.stop();
        assert!(!coordinator.is_running());

        // Stopping again should be a no-op (idempotent).
        coordinator.stop();
        assert!(!coordinator.is_running());
    }

    #[test]
    fn test_epoch_ticker_advances_epochs() {
        let path = PathBuf::from("/tmp/test_ticker_advance.db");
        let config = ParallelWalConfig {
            slot_count: 2,        // Small slot count for testing
            epoch_interval_ms: 5, // Fast interval for testing
            ..ParallelWalConfig::default()
        };
        let coordinator = ParallelWalCoordinator::new(&path, config);
        let runtime = test_runtime();
        let cx = test_cx();

        let initial_epoch = coordinator.current_epoch();

        // Start the ticker and wait for several epochs.
        coordinator
            .start_on_runtime(&runtime.handle(), &cx)
            .expect("start should succeed");
        std::thread::sleep(Duration::from_millis(50));
        coordinator.stop();

        let final_epoch = coordinator.current_epoch();

        assert!(
            final_epoch > initial_epoch,
            "epoch ticker should advance without stalling on inactive slots: initial={initial_epoch}, final={final_epoch}"
        );
    }

    #[test]
    fn test_epoch_ticker_restart_after_parent_cancellation() {
        let path = PathBuf::from("/tmp/test_ticker_restart.db");
        let config = ParallelWalConfig {
            slot_count: 2,
            epoch_interval_ms: 5,
            ..ParallelWalConfig::default()
        };
        let coordinator = ParallelWalCoordinator::new(&path, config);
        let runtime = test_runtime();
        let parent_cx = test_cx();

        coordinator
            .start_on_runtime(&runtime.handle(), &parent_cx)
            .expect("initial start should succeed");
        parent_cx.cancel();
        std::thread::sleep(Duration::from_millis(15));

        let replacement_cx = test_cx();
        coordinator
            .start_on_runtime(&runtime.handle(), &replacement_cx)
            .expect("restart after parent cancellation should drain prior task");
        assert!(coordinator.is_running());

        coordinator.stop();
        assert!(!coordinator.is_running());
    }

    #[test]
    fn test_submit_batch_persists_actual_frame_payloads() {
        use tempfile::tempdir;

        let _guard = lane_test_guard();
        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("submit_batch.db");
        let config = ParallelWalConfig {
            slot_count: 1,
            ..ParallelWalConfig::default()
        };
        let coordinator = ParallelWalCoordinator::new(&db_path, config);

        let epoch = coordinator
            .submit_batch(sample_batch(11, 77))
            .expect("submit should succeed");
        assert_eq!(epoch, 0);

        coordinator
            .advance_and_flush(Duration::from_millis(50))
            .expect("flush should succeed");
        assert_eq!(coordinator.durable_epoch(), Some(0));

        let seg_path = segment_path(&db_path, 0);
        let (_, records) = read_segment(&seg_path).expect("segment should read back");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].txn_token.id.get(), 11);
        assert_eq!(records[0].begin_seq, CommitSeq::new(77));
        assert_eq!(records[0].page_id.get(), 7);
        assert_eq!(records[0].after_image, vec![0xAA; 16]);
        assert_eq!(records[1].page_id.get(), 9);
        assert_eq!(records[1].after_image, vec![0xBB; 24]);
    }

    #[test]
    fn test_advance_and_flush_does_not_mark_epoch_durable_on_segment_write_failure() {
        use tempfile::tempdir;

        let _guard = lane_test_guard();
        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("missing").join("write_failure.db");
        let config = ParallelWalConfig {
            slot_count: 1,
            ..ParallelWalConfig::default()
        };
        let coordinator = ParallelWalCoordinator::new(&db_path, config);

        coordinator
            .submit_batch(sample_batch(21, 99))
            .expect("submit should succeed");

        let error = coordinator
            .advance_and_flush(Duration::from_millis(50))
            .expect_err("flush should fail when the segment directory is missing");
        assert!(
            error.contains("write_segment(0) failed"),
            "error should preserve the failing epoch: {error}"
        );
        assert_eq!(
            coordinator.durable_epoch(),
            None,
            "failed segment writes must not be reported as durable"
        );
        assert!(
            coordinator
                .wait_for_epoch_durable(0, Duration::from_millis(10))
                .is_err(),
            "durability wait must keep blocking after a failed segment write"
        );
    }

    // -------------------------------------------------------------------------
    // Segment File I/O Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_segment_header_roundtrip() {
        let header = SegmentHeader::new(42, 100);
        let bytes = header.to_bytes();
        let parsed = SegmentHeader::from_bytes(&bytes).expect("should parse");
        assert_eq!(parsed.epoch, 42);
        assert_eq!(parsed.record_count, 100);
    }

    #[test]
    fn test_segment_header_invalid_magic() {
        let mut bytes = [0u8; SEGMENT_HEADER_SIZE];
        bytes[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        let result = SegmentHeader::from_bytes(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid segment magic"));
    }

    #[test]
    fn test_segment_header_checksum_mismatch() {
        let header = SegmentHeader::new(42, 100);
        let mut bytes = header.to_bytes();
        // Corrupt the epoch field
        bytes[8] ^= 0xFF;
        let result = SegmentHeader::from_bytes(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("checksum mismatch"));
    }

    #[test]
    fn test_segment_path_generation() {
        let db_path = PathBuf::from("/tmp/mydb.sqlite");
        let path = segment_path(&db_path, 0x1234_5678_9ABC_DEF0);
        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            "mydb.sqlite-wal-seg-123456789abcdef0"
        );
    }

    #[test]
    fn test_segment_write_and_read() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("test.db");

        // Create a batch with some records
        let records = vec![
            WalRecord {
                txn_token: TxnToken::new(
                    fsqlite_types::TxnId::new(1).unwrap(),
                    fsqlite_types::TxnEpoch::new(0),
                ),
                epoch: 5,
                page_id: PageNumber::new(1).unwrap(),
                begin_seq: CommitSeq::new(100),
                end_seq: Some(CommitSeq::new(100)),
                before_image: vec![0u8; 32],
                after_image: vec![1u8; 32],
            },
            WalRecord {
                txn_token: TxnToken::new(
                    fsqlite_types::TxnId::new(2).unwrap(),
                    fsqlite_types::TxnEpoch::new(1),
                ),
                epoch: 5,
                page_id: PageNumber::new(2).unwrap(),
                begin_seq: CommitSeq::new(101),
                end_seq: None,
                before_image: Vec::new(),
                after_image: vec![2u8; 64],
            },
        ];

        let batch = EpochFlushBatch {
            epoch: 5,
            records,
            records_per_core: vec![1, 1],
        };

        // Write the segment
        let bytes_written =
            write_segment(&db_path, &batch, FsyncPolicy::Off).expect("write should succeed");
        assert!(bytes_written > SEGMENT_HEADER_SIZE);

        // Read it back
        let seg_path = segment_path(&db_path, 5);
        let (header, records) = read_segment(&seg_path).expect("read should succeed");

        assert_eq!(header.epoch, 5);
        assert_eq!(header.record_count, 2);
        assert_eq!(records.len(), 2);

        // Verify first record
        assert_eq!(records[0].txn_token.id.get(), 1);
        assert_eq!(records[0].page_id.get(), 1);
        assert_eq!(records[0].before_image.len(), 32);
        assert_eq!(records[0].after_image.len(), 32);
        assert_eq!(records[0].end_seq, Some(CommitSeq::new(100)));

        // Verify second record
        assert_eq!(records[1].txn_token.id.get(), 2);
        assert_eq!(records[1].page_id.get(), 2);
        assert_eq!(records[1].before_image.len(), 0);
        assert_eq!(records[1].after_image.len(), 64);
        assert_eq!(records[1].end_seq, None);

        // Cleanup
        delete_segment(&seg_path).expect("delete should succeed");
    }

    #[test]
    fn test_deserialize_record_rejects_invalid_end_seq_flag() {
        let record = WalRecord {
            txn_token: TxnToken::new(
                fsqlite_types::TxnId::new(1).expect("txn id should be non-zero"),
                fsqlite_types::TxnEpoch::new(0),
            ),
            epoch: 5,
            page_id: PageNumber::new(1).expect("page should be non-zero"),
            begin_seq: CommitSeq::new(100),
            end_seq: None,
            before_image: Vec::new(),
            after_image: vec![0xAA; 8],
        };
        let mut bytes = serialize_record(&record).expect("sample record should serialize");
        let end_seq_flag_offset = 8 + 4 + 8 + 4 + 8;
        bytes[end_seq_flag_offset] = 2;

        let error = deserialize_record(&bytes)
            .expect_err("invalid end_seq flag must reject corrupt record bytes");
        assert!(
            error.contains("invalid end_seq flag"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_deserialize_record_rejects_trailing_bytes() {
        let record = WalRecord {
            txn_token: TxnToken::new(
                fsqlite_types::TxnId::new(1).expect("txn id should be non-zero"),
                fsqlite_types::TxnEpoch::new(0),
            ),
            epoch: 5,
            page_id: PageNumber::new(1).expect("page should be non-zero"),
            begin_seq: CommitSeq::new(100),
            end_seq: None,
            before_image: Vec::new(),
            after_image: vec![0xAA; 8],
        };
        let mut bytes = serialize_record(&record).expect("sample record should serialize");
        bytes.extend_from_slice(b"junk");

        let error =
            deserialize_record(&bytes).expect_err("record decoder must reject trailing bytes");
        assert!(
            error.contains("trailing bytes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_read_segment_rejects_impossible_record_count_before_allocation() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("impossible-count.db");
        let seg_path = segment_path(&db_path, 1);
        std::fs::write(&seg_path, SegmentHeader::new(1, u32::MAX).to_bytes())
            .expect("write corrupt segment header");

        let error =
            read_segment(&seg_path).expect_err("impossible record count must fail before alloc");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("exceeds maximum possible"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_read_segment_rejects_record_count_without_min_payload_space() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("short-records.db");
        let seg_path = segment_path(&db_path, 1);
        let mut bytes = SegmentHeader::new(1, 2).to_bytes().to_vec();
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        std::fs::write(&seg_path, bytes).expect("write corrupt segment");

        let error = read_segment(&seg_path)
            .expect_err("record count must account for minimum record payload bytes");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("exceeds maximum possible"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_read_segment_rejects_trailing_bytes_after_declared_records() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("trailing-bytes.db");
        let batch = EpochFlushBatch {
            epoch: 1,
            records: vec![WalRecord {
                txn_token: TxnToken::new(
                    fsqlite_types::TxnId::new(1).expect("txn id should be non-zero"),
                    fsqlite_types::TxnEpoch::new(0),
                ),
                epoch: 1,
                page_id: PageNumber::new(1).expect("page should be non-zero"),
                begin_seq: CommitSeq::new(1),
                end_seq: Some(CommitSeq::new(1)),
                before_image: Vec::new(),
                after_image: vec![0xCC; 16],
            }],
            records_per_core: vec![1],
        };
        write_segment(&db_path, &batch, FsyncPolicy::Off).expect("write should succeed");

        let seg_path = segment_path(&db_path, 1);
        {
            use std::io::Write as _;
            let mut file = OpenOptions::new()
                .append(true)
                .open(&seg_path)
                .expect("open segment for append");
            file.write_all(b"junk").expect("append trailing bytes");
        }

        let error =
            read_segment(&seg_path).expect_err("segment decoder must reject trailing bytes");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("trailing bytes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_read_segment_rejects_oversized_record_length_before_allocation() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("oversized.db");
        let seg_path = segment_path(&db_path, 1);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&SegmentHeader::new(1, 1).to_bytes());
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(&seg_path, bytes).expect("write corrupt segment");

        let error =
            read_segment(&seg_path).expect_err("oversized record length must fail before alloc");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("exceeds maximum"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_segment_write_and_recovery_canonicalize_intra_epoch_order() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("ordered.db");
        let page_id = PageNumber::new(1).unwrap();

        let later = WalRecord {
            txn_token: TxnToken::new(
                fsqlite_types::TxnId::new(2).unwrap(),
                fsqlite_types::TxnEpoch::new(0),
            ),
            epoch: 7,
            page_id,
            begin_seq: CommitSeq::new(200),
            end_seq: Some(CommitSeq::new(200)),
            before_image: Vec::new(),
            after_image: vec![0x22; 8],
        };
        let earlier = WalRecord {
            txn_token: TxnToken::new(
                fsqlite_types::TxnId::new(1).unwrap(),
                fsqlite_types::TxnEpoch::new(0),
            ),
            epoch: 7,
            page_id,
            begin_seq: CommitSeq::new(100),
            end_seq: Some(CommitSeq::new(100)),
            before_image: Vec::new(),
            after_image: vec![0x11; 8],
        };
        let batch = EpochFlushBatch {
            epoch: 7,
            records: vec![later, earlier],
            records_per_core: vec![1, 1],
        };

        write_segment(&db_path, &batch, FsyncPolicy::Off).expect("write should succeed");

        let seg_path = segment_path(&db_path, 7);
        let (_, records) = read_segment(&seg_path).expect("read should succeed");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].begin_seq, CommitSeq::new(100));
        assert_eq!(records[1].begin_seq, CommitSeq::new(200));

        let mut page_contents = HashMap::new();
        recover_and_apply_segments(
            &db_path,
            &mut page_contents,
            SegmentRecoveryOptions::default(),
        )
        .expect("recovery should succeed");
        assert_eq!(
            page_contents.get(&page_id.get()),
            Some(&vec![0x22; 8]),
            "recovery must replay the later commit last even if the flushed batch arrived out of order"
        );
    }

    #[test]
    fn test_write_segment_rejects_record_epoch_mismatch() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("mismatch.db");

        let batch = EpochFlushBatch {
            epoch: 5,
            records: vec![WalRecord {
                txn_token: TxnToken::new(
                    fsqlite_types::TxnId::new(1).unwrap(),
                    fsqlite_types::TxnEpoch::new(0),
                ),
                epoch: 4,
                page_id: PageNumber::new(1).unwrap(),
                begin_seq: CommitSeq::new(100),
                end_seq: Some(CommitSeq::new(100)),
                before_image: Vec::new(),
                after_image: vec![0xAB; 8],
            }],
            records_per_core: vec![1],
        };

        let error = write_segment(&db_path, &batch, FsyncPolicy::Off)
            .expect_err("segment write must reject mixed-epoch records");
        assert!(
            error
                .to_string()
                .contains("segment epoch 5 contains record from epoch 4"),
            "unexpected error: {error}"
        );
        assert!(
            !segment_path(&db_path, 5).exists(),
            "failed validation must not create or truncate a segment file"
        );
    }

    #[test]
    fn test_write_segment_rejects_oversized_page_image_before_create() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("oversized-write.db");
        let batch = EpochFlushBatch {
            epoch: 5,
            records: vec![WalRecord {
                txn_token: TxnToken::new(
                    fsqlite_types::TxnId::new(1).expect("txn id should be non-zero"),
                    fsqlite_types::TxnEpoch::new(0),
                ),
                epoch: 5,
                page_id: PageNumber::new(1).expect("page should be non-zero"),
                begin_seq: CommitSeq::new(100),
                end_seq: Some(CommitSeq::new(100)),
                before_image: Vec::new(),
                after_image: vec![0xAB; MAX_SEGMENT_RECORD_IMAGE_BYTES + 1],
            }],
            records_per_core: vec![1],
        };

        let error = write_segment(&db_path, &batch, FsyncPolicy::Off)
            .expect_err("segment write must reject oversized page images");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("after_image length"),
            "unexpected error: {error}"
        );
        assert!(
            !segment_path(&db_path, 5).exists(),
            "failed validation must not create or truncate a segment file"
        );
    }

    #[test]
    fn test_list_segments() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("test.db");

        // Create a few empty segment files
        for epoch in [1u64, 5, 10, 2] {
            let batch = EpochFlushBatch {
                epoch,
                records: Vec::new(),
                records_per_core: Vec::new(),
            };
            write_segment(&db_path, &batch, FsyncPolicy::Off).expect("write should succeed");
        }

        // List segments
        let segments = list_segments(&db_path).expect("list should succeed");
        assert_eq!(segments.len(), 4);

        // Should be sorted by epoch
        assert_eq!(segments[0].0, 1);
        assert_eq!(segments[1].0, 2);
        assert_eq!(segments[2].0, 5);
        assert_eq!(segments[3].0, 10);

        // Cleanup
        for (_, path) in segments {
            delete_segment(&path).expect("delete should succeed");
        }
    }

    // -------------------------------------------------------------------------
    // Segment Recovery Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_recover_segments_basic() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("test.db");

        // Create segments for epochs 1, 2, 3
        for epoch in 1..=3u64 {
            let records = vec![WalRecord {
                txn_token: TxnToken::new(
                    fsqlite_types::TxnId::new(epoch).unwrap(),
                    fsqlite_types::TxnEpoch::new(0),
                ),
                epoch,
                page_id: PageNumber::new(epoch as u32).unwrap(),
                begin_seq: CommitSeq::new(epoch * 100),
                end_seq: Some(CommitSeq::new(epoch * 100)),
                before_image: Vec::new(),
                after_image: vec![epoch as u8; 32],
            }];
            let batch = EpochFlushBatch {
                epoch,
                records,
                records_per_core: vec![1],
            };
            write_segment(&db_path, &batch, FsyncPolicy::Off).expect("write should succeed");
        }

        // Recover segments
        let options = SegmentRecoveryOptions::default();
        let (result, records) =
            recover_segments(&db_path, options).expect("recovery should succeed");

        assert_eq!(result.segments_recovered, 3);
        assert_eq!(result.records_applied, 3);
        assert_eq!(result.epochs, vec![1, 2, 3]);
        assert!(result.partial_segments.is_empty());

        // Verify records are in epoch order
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].epoch, 1);
        assert_eq!(records[1].epoch, 2);
        assert_eq!(records[2].epoch, 3);

        // Cleanup
        cleanup_segments(&db_path).expect("cleanup should succeed");
    }

    #[test]
    fn test_recover_segments_rejects_header_filename_epoch_mismatch() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("rename.db");
        let batch = EpochFlushBatch {
            epoch: 5,
            records: vec![WalRecord {
                txn_token: TxnToken::new(
                    fsqlite_types::TxnId::new(1).unwrap(),
                    fsqlite_types::TxnEpoch::new(0),
                ),
                epoch: 5,
                page_id: PageNumber::new(1).unwrap(),
                begin_seq: CommitSeq::new(100),
                end_seq: Some(CommitSeq::new(100)),
                before_image: Vec::new(),
                after_image: vec![0xAA; 8],
            }],
            records_per_core: vec![1],
        };
        write_segment(&db_path, &batch, FsyncPolicy::Off).expect("write should succeed");

        let original = segment_path(&db_path, 5);
        let renamed = segment_path(&db_path, 3);
        std::fs::rename(&original, &renamed).expect("rename should succeed");

        let error = recover_segments(&db_path, SegmentRecoveryOptions::default())
            .expect_err("recovery must fail closed on mismatched epoch metadata");
        assert!(
            error.to_string().contains("mismatched epoch"),
            "unexpected error: {error}"
        );

        let (result, records) = recover_segments(
            &db_path,
            SegmentRecoveryOptions {
                skip_corrupt: true,
                ..Default::default()
            },
        )
        .expect("skip_corrupt should ignore the bad segment");
        assert_eq!(result.segments_recovered, 0);
        assert_eq!(result.partial_segments, vec![renamed]);
        assert!(records.is_empty());
    }

    #[test]
    fn test_recover_and_apply_segments_skip_corrupt_stops_at_first_bad_epoch() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("prefix.db");

        for epoch in 1..=3u64 {
            let batch = EpochFlushBatch {
                epoch,
                records: vec![WalRecord {
                    txn_token: TxnToken::new(
                        fsqlite_types::TxnId::new(epoch).unwrap(),
                        fsqlite_types::TxnEpoch::new(0),
                    ),
                    epoch,
                    page_id: PageNumber::new(1).unwrap(),
                    begin_seq: CommitSeq::new(epoch * 100),
                    end_seq: Some(CommitSeq::new(epoch * 100)),
                    before_image: Vec::new(),
                    after_image: vec![epoch as u8; 16],
                }],
                records_per_core: vec![1],
            };
            write_segment(&db_path, &batch, FsyncPolicy::Off).expect("write should succeed");
        }

        let corrupt_epoch_path = segment_path(&db_path, 2);
        std::fs::write(&corrupt_epoch_path, [0xFF_u8; 8]).expect("corrupt write should succeed");

        let mut page_contents = HashMap::new();
        let result = recover_and_apply_segments(
            &db_path,
            &mut page_contents,
            SegmentRecoveryOptions {
                skip_corrupt: true,
                ..Default::default()
            },
        )
        .expect("skip_corrupt should return the durable prefix");

        assert_eq!(result.segments_recovered, 1);
        assert_eq!(result.records_applied, 1);
        assert_eq!(result.epochs, vec![1]);
        assert_eq!(
            result.partial_segments,
            vec![segment_path(&db_path, 2), segment_path(&db_path, 3)]
        );

        let page = page_contents
            .get(&1)
            .expect("prefix recovery should apply the last durable epoch only");
        assert!(
            page.iter().all(|&byte| byte == 1),
            "recovery must stop before epoch 3 once epoch 2 is corrupt"
        );
    }

    #[test]
    fn test_recover_and_apply_segments() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("test.db");

        // Create segments that update the same page multiple times
        let page_id = 1u32;
        for epoch in 1..=3u64 {
            let records = vec![WalRecord {
                txn_token: TxnToken::new(
                    fsqlite_types::TxnId::new(epoch).unwrap(),
                    fsqlite_types::TxnEpoch::new(0),
                ),
                epoch,
                page_id: PageNumber::new(page_id).unwrap(),
                begin_seq: CommitSeq::new(epoch * 100),
                end_seq: Some(CommitSeq::new(epoch * 100)),
                before_image: Vec::new(),
                after_image: vec![epoch as u8; 32], // Different content each epoch
            }];
            let batch = EpochFlushBatch {
                epoch,
                records,
                records_per_core: vec![1],
            };
            write_segment(&db_path, &batch, FsyncPolicy::Off).expect("write should succeed");
        }

        // Recover and apply to page cache
        let mut page_contents = HashMap::new();
        let options = SegmentRecoveryOptions {
            delete_after_recovery: true,
            ..Default::default()
        };
        let result = recover_and_apply_segments(&db_path, &mut page_contents, options)
            .expect("should succeed");

        assert_eq!(result.segments_recovered, 3);

        // Page should have the final epoch's contents (epoch 3)
        let page = page_contents.get(&page_id).expect("page should exist");
        assert_eq!(page.len(), 32);
        assert!(page.iter().all(|&b| b == 3), "should have epoch 3 content");

        // GH #192: the in-memory apply must NOT delete the segments — they
        // are the sole durable copy until the caller persists the pages.
        let remaining = list_segments(&db_path).expect("list should succeed");
        assert_eq!(
            remaining.len(),
            3,
            "segments must survive until the applied state is durable"
        );
        assert_eq!(result.deletable_segments.len(), 3);

        // Once the caller has made the applied pages durable, explicit
        // cleanup removes exactly the recovered segments.
        delete_recovered_segments(&result);
        let remaining = list_segments(&db_path).expect("list should succeed");
        assert!(
            remaining.is_empty(),
            "post-durability cleanup deletes the recovered segments"
        );
    }

    /// GH #192: `recover_segments` must never delete segment files itself.
    /// Between parse and apply the returned records exist only in memory, so
    /// the durable segments have to survive until the caller's apply boundary.
    #[test]
    fn test_recover_segments_keeps_segments_until_apply() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("test.db");

        let batch = EpochFlushBatch {
            epoch: 1,
            records: vec![WalRecord {
                txn_token: TxnToken::new(
                    fsqlite_types::TxnId::new(1).unwrap(),
                    fsqlite_types::TxnEpoch::new(0),
                ),
                epoch: 1,
                page_id: PageNumber::new(1).unwrap(),
                begin_seq: CommitSeq::new(100),
                end_seq: Some(CommitSeq::new(100)),
                before_image: Vec::new(),
                after_image: vec![0xAA; 32],
            }],
            records_per_core: vec![1],
        };
        write_segment(&db_path, &batch, FsyncPolicy::Off).expect("write should succeed");

        // This is the crash-window boundary from the issue: records parsed
        // and returned, nothing applied yet.
        let (result, records) = recover_segments(
            &db_path,
            SegmentRecoveryOptions {
                delete_after_recovery: true,
                ..Default::default()
            },
        )
        .expect("recovery should succeed");

        assert_eq!(result.segments_recovered, 1);
        assert_eq!(records.len(), 1, "record retained in memory");
        let remaining = list_segments(&db_path).expect("list should succeed");
        assert_eq!(
            remaining.len(),
            1,
            "segment must remain durable while its records are unapplied"
        );
        assert_eq!(result.deletable_segments, vec![segment_path(&db_path, 1)]);

        // After the caller applies the records, explicit cleanup removes them.
        delete_recovered_segments(&result);
        let remaining = list_segments(&db_path).expect("list should succeed");
        assert!(
            remaining.is_empty(),
            "post-apply cleanup deletes the recovered segments"
        );
    }

    #[test]
    fn test_max_durable_epoch() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("test.db");

        // Initially no segments
        let max = max_durable_epoch(&db_path).expect("should succeed");
        assert_eq!(max, None);

        // Create segments
        for epoch in [5u64, 10, 3] {
            let batch = EpochFlushBatch {
                epoch,
                records: Vec::new(),
                records_per_core: Vec::new(),
            };
            write_segment(&db_path, &batch, FsyncPolicy::Off).expect("write should succeed");
        }

        // Max should be 10
        let max = max_durable_epoch(&db_path).expect("should succeed");
        assert_eq!(max, Some(10));

        // Cleanup
        cleanup_segments(&db_path).expect("cleanup should succeed");

        // Now max should be None again
        let max = max_durable_epoch(&db_path).expect("should succeed");
        assert_eq!(max, None);
    }

    #[test]
    fn test_cleanup_segments() {
        use tempfile::tempdir;

        let dir = tempdir().expect("create temp dir");
        let db_path = dir.path().join("test.db");

        // Create segments
        for epoch in 1..=5u64 {
            let batch = EpochFlushBatch {
                epoch,
                records: Vec::new(),
                records_per_core: Vec::new(),
            };
            write_segment(&db_path, &batch, FsyncPolicy::Off).expect("write should succeed");
        }

        // Verify segments exist
        let segments = list_segments(&db_path).expect("list should succeed");
        assert_eq!(segments.len(), 5);

        // Cleanup
        let count = cleanup_segments(&db_path).expect("cleanup should succeed");
        assert_eq!(count, 5);

        // Verify segments are gone
        let segments = list_segments(&db_path).expect("list should succeed");
        assert!(segments.is_empty());
    }

    #[test]
    fn parallel_wal_config_default_values() {
        let cfg = ParallelWalConfig::default();
        assert_eq!(cfg.slot_count, DEFAULT_BUFFER_SLOT_COUNT);
        assert_eq!(cfg.epoch_interval_ms, 10);
        assert_eq!(cfg.buffer_capacity_bytes, 4 * 1024 * 1024);
        let copied = cfg;
        assert_eq!(copied.epoch_interval_ms, cfg.epoch_interval_ms);
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("ParallelWalConfig"));
    }

    #[test]
    fn parallel_wal_fallback_reason_all_variants_debug_copy_eq() {
        let variants = [
            ParallelWalFallbackReason::OperatorForced,
            ParallelWalFallbackReason::LaneOverflow,
            ParallelWalFallbackReason::CertificateGap,
            ParallelWalFallbackReason::CertificateChecksumMismatch,
            ParallelWalFallbackReason::PublicationMismatch,
            ParallelWalFallbackReason::RecoveryGap,
            ParallelWalFallbackReason::CheckpointConflict,
            ParallelWalFallbackReason::ControllerEvidenceLost,
        ];
        for (i, v) in variants.iter().enumerate() {
            let copied = *v;
            assert_eq!(copied, *v);
            for (j, w) in variants.iter().enumerate() {
                assert_eq!(i == j, v == w);
            }
        }
        let dbg = format!("{:?}", ParallelWalFallbackReason::CertificateGap);
        assert!(dbg.contains("CertificateGap"));
    }

    #[test]
    fn parallel_wal_control_surface_default_and_eq() {
        let def = ParallelWalControlSurface::default();
        assert_eq!(def.mode, ParallelWalOperatingMode::Auto);
        assert!(def.lane_count_override.is_none());
        assert!(def.helper_lane_budget.is_none());
        assert!(def.max_parallel_commit_bytes.is_none());
        assert!(def.max_flush_delay_ms.is_none());
        assert!(def.shadow_compare_sampling_per_mille.is_none());
        let other = ParallelWalControlSurface {
            mode: ParallelWalOperatingMode::Conservative,
            ..ParallelWalControlSurface::default()
        };
        assert_ne!(def, other);
        let dbg = format!("{def:?}");
        assert!(dbg.contains("ParallelWalControlSurface"));
    }

    #[test]
    fn parallel_wal_ordered_residue_and_shadow_verdict_defaults() {
        let residue = ParallelWalOrderedResidue::default();
        assert_eq!(
            residue,
            ParallelWalOrderedResidue::CommitCertificateThenPublish
        );
        let copied = residue;
        assert_eq!(copied, residue);

        let verdict = ParallelWalShadowVerdict::default();
        assert_eq!(verdict, ParallelWalShadowVerdict::NotRun);
        assert_ne!(verdict, ParallelWalShadowVerdict::Clean);
        assert_ne!(
            ParallelWalShadowVerdict::Clean,
            ParallelWalShadowVerdict::Diverged
        );
        let dbg = format!("{verdict:?}");
        assert!(dbg.contains("NotRun"));
    }

    #[test]
    fn decision_action_all_variants_copy_eq() {
        let variants = [
            ParallelWalDecisionAction::KeepCurrent,
            ParallelWalDecisionAction::SealEpochNow,
            ParallelWalDecisionAction::IncreaseLaneBudget,
            ParallelWalDecisionAction::DecreaseLaneBudget,
            ParallelWalDecisionAction::ForceConservative,
        ];
        for (i, a) in variants.iter().enumerate() {
            let copied = *a;
            assert_eq!(copied, *a);
            for (j, b) in variants.iter().enumerate() {
                assert_eq!(i == j, a == b);
            }
        }
    }

    #[test]
    fn fsync_policy_all_variants_and_default() {
        assert_eq!(FsyncPolicy::default(), FsyncPolicy::Full);
        let variants = [FsyncPolicy::Full, FsyncPolicy::Normal, FsyncPolicy::Off];
        for (i, a) in variants.iter().enumerate() {
            let copied = *a;
            assert_eq!(copied, *a);
            for (j, b) in variants.iter().enumerate() {
                assert_eq!(i == j, a == b);
            }
        }
        let dbg = format!("{:?}", FsyncPolicy::Off);
        assert!(dbg.contains("Off"));
    }

    #[test]
    fn trace_record_clone_eq_debug() {
        let tr = ParallelWalTraceRecord {
            component: "test".into(),
            trace_id: 1,
            decision_id: None,
            mode: ParallelWalOperatingMode::Auto,
            lane_id: Some(0),
            epoch: Some(5),
            commit_seq_lo: None,
            commit_seq_hi: None,
            checkpoint_epoch: None,
            recovery_epoch: None,
            fallback_active: false,
            fallback_reason: None,
            policy_id: None,
            policy_version: None,
        };
        let cloned = tr.clone();
        assert_eq!(cloned, tr);
        let dbg = format!("{tr:?}");
        assert!(dbg.contains("ParallelWalTraceRecord"));
    }

    #[test]
    fn commit_certificate_clone_eq_debug() {
        let cert = ParallelWalCommitCertificate {
            format_version: 1,
            residue: ParallelWalOrderedResidue::default(),
            certificate_epoch: 10,
            commit_seq_lo: CommitSeq::new(1),
            commit_seq_hi: CommitSeq::new(5),
            durable_segment_epoch: 9,
            lane_count: 4,
            lane_record_counts: vec![2, 3, 0, 1],
            db_size_pages: 100,
            page_set_size: 6,
            certificate_crc32c: 0,
            fallback_active: false,
        };
        let cloned = cert.clone();
        assert_eq!(cloned, cert);
        assert_eq!(cert.lane_count, 4);
        assert_eq!(cert.lane_record_counts.len(), 4);
        let dbg = format!("{cert:?}");
        assert!(dbg.contains("ParallelWalCommitCertificate"));
    }
}
