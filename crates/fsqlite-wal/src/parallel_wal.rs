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
use std::future::Future;
use std::hash::BuildHasher;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use asupersync::runtime::{BlockingTaskHandle, RuntimeHandle};
use fsqlite_types::{CommitSeq, PageNumber, TxnToken, cx::Cx, limits, sync_primitives::Instant};

use crate::group_commit::TransactionFrameBatchContext;
use crate::per_core_buffer::{
    AppendOutcome, BufferConfig, DEFAULT_BUFFER_SLOT_COUNT, EpochConfig, EpochFlushBatch,
    EpochOrderCoordinator, WalRecord, thread_buffer_slot,
};
use crate::wal::WalGenerationIdentity;

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
    /// bd-o81ov: the batch commits freshly allocated EOF pages (pages beyond
    /// the transaction's begin-time committed database size). Growth commits
    /// must take the serialized append path so the peer-claimed-range guard
    /// can observe a prior grower's durable `db_size` before this batch
    /// appends: in parallel lanes two connections' growth commits ride
    /// concurrent single-batch epochs, neither durable when the other
    /// validates, and both can publish the SAME physical page into different
    /// B-tree positions ("page N referenced multiple times").
    EofGrowth,
    /// bd-gh302 / bd-0shxy: the batch publishes durable freelist metadata
    /// (page-1 head/count + trunk pages) derived from the transaction's
    /// begin-time freelist view. Such commits must take the serialized append
    /// path so the resurrection guard can compare the publication against the
    /// CURRENT durable freelist before it lands: concurrently validating
    /// freelist publications could each re-publish a page the other just
    /// consumed, granting one physical page to multiple connections.
    FreelistPublication,
    /// bd-dw8oe: the batch carries a page-1 frame. Page-1 header fields
    /// (page_count, freelist head/count) are promoted at FLUSH time from the
    /// current durable state; lane staging serializes frames BEFORE the
    /// flusher runs, so a lane-staged page-1 frame appends its stale
    /// begin-time header bytes verbatim — republishing a consumed freelist
    /// head (trunk-chain garbage) or an erased freelist (never-used orphans)
    /// or a regressed page_count. Every page-1-carrying batch takes the
    /// serialized path where promotion and the append-gate header checks
    /// actually apply to the appended bytes.
    PageOneHeader,
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
/// Structured-log scenario id for the ordered durability/publication residue.
pub const PARALLEL_WAL_PUBLICATION_SCENARIO_ID: &str = "parallel_wal_publication";
/// Stable on-disk schema version for commit-certificate canonical bytes.
pub const PARALLEL_WAL_COMMIT_CERTIFICATE_VERSION: u16 = 2;
/// Stable envelope version for append-only durable certificate records.
///
/// Bumped 3 -> 4 for bd-85x9y / GH#364: the record envelope now carries the
/// creating database's 16-byte creation-stable identity (`db_file_id`). A v3
/// (identity-less) record decodes to an error, which recovery treats as an
/// absent certificate rather than a fatal failure.
pub const PARALLEL_WAL_DURABLE_CERTIFICATE_RECORD_VERSION: u16 = 4;
/// First durable-certificate envelope version that ever reached a `-wal-cert`
/// sidecar. The parallel-WAL durability work shipped its on-disk record at v2;
/// no v1 record was ever written.
///
/// Every version in `FIRST..RECORD_VERSION` is a *legacy* envelope: a durable
/// proof written by an older release that this build no longer decodes (v3
/// predates the `db_file_id` identity; v2 additionally predates the
/// ordered-frame payload digest). Recovery treats such a record as an ABSENT
/// certificate — conservative WAL recovery — rather than as corruption, so a
/// store written by an older build still opens after an upgrade (GH#372).
pub const PARALLEL_WAL_DURABLE_CERTIFICATE_FIRST_RECORD_VERSION: u16 = 2;

/// True when `version` names a durable-certificate envelope that an older
/// release wrote and this build recognizes but cannot honor (every version in
/// [`PARALLEL_WAL_DURABLE_CERTIFICATE_FIRST_RECORD_VERSION`]`..`
/// [`PARALLEL_WAL_DURABLE_CERTIFICATE_RECORD_VERSION`]).
///
/// Strictly newer versions and never-shipped values (0, 1) are NOT legacy:
/// they still fail strict decoding, so a corrupted version field keeps
/// reading as corruption rather than being silently ignored.
#[must_use]
pub const fn durable_certificate_record_version_is_legacy(version: u16) -> bool {
    version >= PARALLEL_WAL_DURABLE_CERTIFICATE_FIRST_RECORD_VERSION
        && version < PARALLEL_WAL_DURABLE_CERTIFICATE_RECORD_VERSION
}
/// Magic prefix for one record in the `-wal-cert` sidecar.
pub const PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC: [u8; 8] = *b"FSQLCERT";
const PARALLEL_WAL_FRAME_PAYLOAD_DIGEST_DOMAIN: &str =
    "FrankenSQLite parallel WAL ordered frame payload digest v1";
const PARALLEL_WAL_FRAME_PAYLOAD_DIGEST_SIZE: usize = 32;
/// Lane ids and commit-certificate lane counts are stored as `u16`, so keep
/// both the count and every generated id representable.
const MAX_PARALLEL_WAL_LANE_COUNT: usize = 65_535;
/// Largest valid encoded commit-certificate sidecar record.
///
/// The fixed envelope is 152 bytes (136 pre-bd-85x9y plus the 16-byte
/// `db_file_id`) and each representable lane contributes one four-byte record
/// count. Readers and writers share this bound so no successfully persisted
/// record can become unreadable during recovery.
pub const PARALLEL_WAL_MAX_DURABLE_CERTIFICATE_RECORD_SIZE: usize =
    ParallelWalDurableCertificateRecord::MIN_ENCODED_SIZE + MAX_PARALLEL_WAL_LANE_COUNT * 4;

/// Incrementally binds a certificate to its ordered WAL frame contents.
///
/// Each [`Self::update`] hashes a zero-based `u64` frame ordinal, the `u32`
/// page number, the `u32` commit database size, the `u64` payload length, and
/// the payload bytes, all in little-endian field order. BLAKE3 derive-key mode
/// domain-separates this digest from every other hash protocol.
#[derive(Debug, Clone)]
pub struct ParallelWalFramePayloadDigestBuilder {
    hasher: blake3::Hasher,
    next_ordinal: u64,
}

impl ParallelWalFramePayloadDigestBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new_derive_key(PARALLEL_WAL_FRAME_PAYLOAD_DIGEST_DOMAIN),
            next_ordinal: 0,
        }
    }

    /// Add the next frame in its exact ordered-WAL position.
    pub fn update(&mut self, page_number: PageNumber, db_size_if_commit: u32, payload: &[u8]) {
        let ordinal = self.next_ordinal;
        self.next_ordinal = ordinal
            .checked_add(1)
            .expect("parallel WAL frame ordinal overflow");
        let payload_len =
            u64::try_from(payload.len()).expect("parallel WAL frame payload length exceeds u64");

        self.hasher.update(&ordinal.to_le_bytes());
        self.hasher.update(&page_number.get().to_le_bytes());
        self.hasher.update(&db_size_if_commit.to_le_bytes());
        self.hasher.update(&payload_len.to_le_bytes());
        self.hasher.update(payload);
    }

    /// Finish the ordered-frame digest, consuming the builder so a completed
    /// certificate cannot accidentally be extended with later frames.
    #[must_use]
    pub fn finalize(self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }
}

impl Default for ParallelWalFramePayloadDigestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

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
        Some(ParallelWalFallbackReason::EofGrowth) => "eof_growth",
        Some(ParallelWalFallbackReason::FreelistPublication) => "freelist_publication",
        Some(ParallelWalFallbackReason::PageOneHeader) => "page_one_header",
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
    if let Ok(raw) = env::var("FSQLITE_PARALLEL_WAL_LANES")
        && let Ok(value) = raw.trim().parse::<usize>()
    {
        control.lane_count_override = Some(value.max(1));
    }
    if let Ok(raw) = env::var("FSQLITE_PARALLEL_WAL_MAX_BATCH_BYTES")
        && let Ok(value) = raw.trim().parse::<u64>()
    {
        control.max_parallel_commit_bytes = Some(value.max(1));
    }
    if let Ok(raw) = env::var("FSQLITE_PARALLEL_WAL_MAX_FLUSH_DELAY_MS")
        && let Ok(value) = raw.trim().parse::<u64>()
    {
        control.max_flush_delay_ms = Some(value);
    }
    if let Ok(raw) = env::var("FSQLITE_PARALLEL_WAL_SHADOW_COMPARE_PER_MILLE")
        && let Ok(value) = raw.trim().parse::<u16>()
    {
        control.shadow_compare_sampling_per_mille = Some(value);
    }

    control
}

/// Commit-certificate proof object for the parallel WAL data plane.
///
/// A commit becomes externally publishable only after the certificate is
/// durably written. The certificate covers a contiguous commit-sequence range,
/// the lanes that contributed to that range, the exact ordered WAL frame
/// contents, and the pager-visible metadata that must be published once the
/// ordered residue completes.
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
    /// BLAKE3 digest of the exact ordered WAL frame headers and payloads.
    pub wal_frame_payload_digest: [u8; 32],
    pub certificate_crc32c: u32,
    pub fallback_active: bool,
}

impl ParallelWalCommitCertificate {
    /// Canonical semantics-bearing bytes covered by `certificate_crc32c`.
    ///
    /// The checksum field itself is deliberately excluded. Integer fields use
    /// little-endian encoding, and the lane cardinalities remain in lane-id
    /// order, so the same certificate has identical bytes on every target.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            2 + 1
                + 8 * 4
                + 2
                + 4
                + self.lane_record_counts.len() * 4
                + 4 * 2
                + PARALLEL_WAL_FRAME_PAYLOAD_DIGEST_SIZE
                + 1,
        );
        bytes.extend_from_slice(&self.format_version.to_le_bytes());
        bytes.push(match self.residue {
            ParallelWalOrderedResidue::CommitCertificateThenPublish => 1,
        });
        bytes.extend_from_slice(&self.certificate_epoch.to_le_bytes());
        bytes.extend_from_slice(&self.commit_seq_lo.get().to_le_bytes());
        bytes.extend_from_slice(&self.commit_seq_hi.get().to_le_bytes());
        bytes.extend_from_slice(&self.durable_segment_epoch.to_le_bytes());
        bytes.extend_from_slice(&self.lane_count.to_le_bytes());
        bytes.extend_from_slice(
            &u32::try_from(self.lane_record_counts.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        for record_count in &self.lane_record_counts {
            bytes.extend_from_slice(&record_count.to_le_bytes());
        }
        bytes.extend_from_slice(&self.db_size_pages.to_le_bytes());
        bytes.extend_from_slice(&self.page_set_size.to_le_bytes());
        bytes.extend_from_slice(&self.wal_frame_payload_digest);
        bytes.push(u8::from(self.fallback_active));
        bytes
    }

    #[must_use]
    pub fn computed_crc32c(&self) -> u32 {
        crc32c::crc32c(&self.canonical_bytes())
    }

    #[must_use]
    pub fn checksum_is_valid(&self) -> bool {
        self.certificate_crc32c == self.computed_crc32c()
    }
}

/// Append-only proof record binding a certificate to one stock-WAL generation
/// and closed frame interval.
///
/// A durable record is authorization, not sufficient evidence by itself.
/// Recovery must also find the matching generation, complete frame interval,
/// and valid commit marker before treating the certificate as committed. This
/// makes a certificate written just before a crash a harmless orphan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelWalDurableCertificateRecord {
    pub wal_generation: WalGenerationIdentity,
    /// One-based first WAL frame covered by this certificate.
    pub wal_frame_start: u64,
    /// One-based final (commit-marker) WAL frame covered by this certificate.
    pub wal_frame_end: u64,
    /// Creation-stable identity of the database file this certificate was
    /// written for (page-1 header bytes 76..92; bd-85x9y / GH#364).
    ///
    /// Recovery binds the certificate to this exact physical database. A cert
    /// whose identity does not match the database currently being opened — the
    /// signature of a stale sidecar left behind across a database-*file*
    /// replacement — is treated as absent so it cannot re-extend a fresh,
    /// smaller database to the replaced file's committed page count. An all-zero
    /// value marks a legacy/pre-identity (unstamped) database and is compared
    /// leniently by the reader. This lives on the record *envelope* only; the
    /// [`ParallelWalCommitCertificate`] payload and its checksum are unchanged.
    pub db_file_id: [u8; 16],
    pub certificate: ParallelWalCommitCertificate,
}

impl ParallelWalDurableCertificateRecord {
    // magic(8) + version(2) + total_len(4) + checkpoint_seq(4) + salt1(4)
    // + salt2(4) + wal_frame_start(8) + wal_frame_end(8) + db_file_id(16).
    // The trailing 16 bytes are the bd-85x9y / GH#364 envelope identity.
    const FIXED_PREFIX_SIZE: usize = 8 + 2 + 4 + 4 + 4 + 4 + 8 + 8 + 16;
    const MIN_CERTIFICATE_SIZE: usize =
        2 + 1 + 8 * 4 + 2 + 4 + 4 + 4 + PARALLEL_WAL_FRAME_PAYLOAD_DIGEST_SIZE + 1 + 4;
    const ENVELOPE_CRC_SIZE: usize = 4;
    /// Smallest valid encoded sidecar record, with zero contributing lanes.
    ///
    /// Recovery readers use this shared bound rather than duplicating the
    /// version-sensitive envelope calculation.
    pub const MIN_ENCODED_SIZE: usize = Self::FIXED_PREFIX_SIZE
        + Self::MIN_CERTIFICATE_SIZE
        + Self::ENVELOPE_CRC_SIZE
        + Self::LENGTH_FOOTER_SIZE;
    /// Duplicate record length stored at the tail for bounded latest-record
    /// lookup without scanning the append-only sidecar.
    pub const LENGTH_FOOTER_SIZE: usize = 4;

    pub fn new(
        wal_generation: WalGenerationIdentity,
        wal_frame_start: u64,
        wal_frame_end: u64,
        db_file_id: [u8; 16],
        certificate: ParallelWalCommitCertificate,
    ) -> Result<Self, String> {
        if wal_frame_start == 0 || wal_frame_end < wal_frame_start {
            return Err(format!(
                "invalid durable certificate WAL frame interval {wal_frame_start}..={wal_frame_end}"
            ));
        }
        if !certificate.checksum_is_valid() {
            return Err("durable certificate has invalid certificate checksum".to_owned());
        }
        Ok(Self {
            wal_generation,
            wal_frame_start,
            wal_frame_end,
            db_file_id,
            certificate,
        })
    }

    /// Decide whether this proof record may authorize a reconstructed WAL
    /// boundary. The caller supplies the already-validated WAL generation,
    /// valid frame count, one-based final commit-marker frame, and digest
    /// independently reconstructed from those ordered frames.
    #[must_use]
    pub fn authorizes_wal_boundary(
        &self,
        wal_generation: WalGenerationIdentity,
        valid_frame_count: u64,
        commit_marker_frame: u64,
        actual_wal_frame_payload_digest: [u8; 32],
    ) -> bool {
        self.wal_generation == wal_generation
            && self.wal_frame_end <= valid_frame_count
            && self.wal_frame_end == commit_marker_frame
            && self.certificate.wal_frame_payload_digest == actual_wal_frame_payload_digest
    }

    /// Encode one self-delimiting sidecar record.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let certificate_bytes = self.certificate.canonical_bytes();
        let total_len = Self::FIXED_PREFIX_SIZE
            .saturating_add(certificate_bytes.len())
            .saturating_add(4)
            .saturating_add(Self::ENVELOPE_CRC_SIZE)
            .saturating_add(Self::LENGTH_FOOTER_SIZE);
        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC);
        bytes.extend_from_slice(&PARALLEL_WAL_DURABLE_CERTIFICATE_RECORD_VERSION.to_le_bytes());
        bytes.extend_from_slice(&u32::try_from(total_len).unwrap_or(u32::MAX).to_le_bytes());
        bytes.extend_from_slice(&self.wal_generation.checkpoint_seq.to_le_bytes());
        bytes.extend_from_slice(&self.wal_generation.salts.salt1.to_le_bytes());
        bytes.extend_from_slice(&self.wal_generation.salts.salt2.to_le_bytes());
        bytes.extend_from_slice(&self.wal_frame_start.to_le_bytes());
        bytes.extend_from_slice(&self.wal_frame_end.to_le_bytes());
        // bd-85x9y / GH#364: creation-stable db-file identity, fixed 16-byte
        // envelope field between the frame interval and the certificate payload.
        bytes.extend_from_slice(&self.db_file_id);
        bytes.extend_from_slice(&certificate_bytes);
        bytes.extend_from_slice(&self.certificate.certificate_crc32c.to_le_bytes());
        let envelope_crc32c = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&envelope_crc32c.to_le_bytes());
        bytes.extend_from_slice(&u32::try_from(total_len).unwrap_or(u32::MAX).to_le_bytes());
        bytes
    }

    /// Strictly decode exactly one sidecar record.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let min_size = Self::MIN_ENCODED_SIZE;
        if bytes.len() < min_size {
            return Err(format!(
                "durable certificate record too short: expected at least {min_size}, got {}",
                bytes.len()
            ));
        }
        let mut offset = 0_usize;
        let magic = read_record_bytes(
            bytes,
            &mut offset,
            PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC.len(),
            "durable certificate magic",
        )?;
        if magic != PARALLEL_WAL_DURABLE_CERTIFICATE_MAGIC {
            return Err("durable certificate record magic mismatch".to_owned());
        }
        let version_bytes = read_record_bytes(bytes, &mut offset, 2, "record version")?;
        let version = u16::from_le_bytes([version_bytes[0], version_bytes[1]]);
        if version != PARALLEL_WAL_DURABLE_CERTIFICATE_RECORD_VERSION {
            return Err(format!(
                "unsupported durable certificate record version {version}"
            ));
        }
        let declared_len =
            usize::try_from(read_record_u32(bytes, &mut offset, "record length")?)
                .map_err(|_| "durable certificate record length exceeds usize".to_owned())?;
        if declared_len != bytes.len() {
            return Err(format!(
                "durable certificate record length mismatch: declared {declared_len}, actual {}",
                bytes.len()
            ));
        }

        let length_footer_offset = bytes.len() - Self::LENGTH_FOOTER_SIZE;
        let footer_len = usize::try_from(u32::from_le_bytes(
            bytes[length_footer_offset..]
                .try_into()
                .map_err(|_| "durable certificate length footer truncated".to_owned())?,
        ))
        .map_err(|_| "durable certificate footer length exceeds usize".to_owned())?;
        if footer_len != declared_len {
            return Err(format!(
                "durable certificate length footer mismatch: header={declared_len}, footer={footer_len}"
            ));
        }
        let envelope_crc_offset = length_footer_offset - Self::ENVELOPE_CRC_SIZE;
        let expected_envelope_crc = u32::from_le_bytes(
            bytes[envelope_crc_offset..length_footer_offset]
                .try_into()
                .map_err(|_| "durable certificate envelope checksum truncated".to_owned())?,
        );
        let actual_envelope_crc = crc32c::crc32c(&bytes[..envelope_crc_offset]);
        if expected_envelope_crc != actual_envelope_crc {
            return Err("durable certificate envelope checksum mismatch".to_owned());
        }

        let checkpoint_seq = read_record_u32(bytes, &mut offset, "WAL checkpoint sequence")?;
        let salt1 = read_record_u32(bytes, &mut offset, "WAL salt1")?;
        let salt2 = read_record_u32(bytes, &mut offset, "WAL salt2")?;
        let wal_frame_start = read_record_u64(bytes, &mut offset, "WAL frame start")?;
        let wal_frame_end = read_record_u64(bytes, &mut offset, "WAL frame end")?;
        if wal_frame_start == 0 || wal_frame_end < wal_frame_start {
            return Err(format!(
                "invalid durable certificate WAL frame interval {wal_frame_start}..={wal_frame_end}"
            ));
        }

        // bd-85x9y / GH#364: creation-stable db-file identity (16 raw bytes).
        let mut db_file_id = [0u8; 16];
        db_file_id.copy_from_slice(read_record_bytes(
            bytes,
            &mut offset,
            16,
            "db-file identity",
        )?);

        let format_bytes = read_record_bytes(bytes, &mut offset, 2, "certificate format")?;
        let format_version = u16::from_le_bytes([format_bytes[0], format_bytes[1]]);
        if format_version != PARALLEL_WAL_COMMIT_CERTIFICATE_VERSION {
            return Err(format!(
                "unsupported commit certificate version {format_version}"
            ));
        }
        let residue = match read_record_bytes(bytes, &mut offset, 1, "ordered residue")?[0] {
            1 => ParallelWalOrderedResidue::CommitCertificateThenPublish,
            value => return Err(format!("invalid ordered residue tag {value}")),
        };
        let certificate_epoch = read_record_u64(bytes, &mut offset, "certificate epoch")?;
        let commit_seq_lo = CommitSeq::new(read_record_u64(
            bytes,
            &mut offset,
            "certificate commit sequence low",
        )?);
        let commit_seq_hi = CommitSeq::new(read_record_u64(
            bytes,
            &mut offset,
            "certificate commit sequence high",
        )?);
        if commit_seq_hi < commit_seq_lo {
            return Err(format!(
                "invalid certificate commit interval {commit_seq_lo}..={commit_seq_hi}"
            ));
        }
        let durable_segment_epoch = read_record_u64(bytes, &mut offset, "durable segment epoch")?;
        let lane_count_bytes = read_record_bytes(bytes, &mut offset, 2, "lane count")?;
        let lane_count = u16::from_le_bytes([lane_count_bytes[0], lane_count_bytes[1]]);
        let lane_record_count = usize::try_from(read_record_u32(
            bytes,
            &mut offset,
            "lane record count length",
        )?)
        .map_err(|_| "lane record count length exceeds usize".to_owned())?;
        if lane_record_count != usize::from(lane_count)
            || lane_record_count > MAX_PARALLEL_WAL_LANE_COUNT
        {
            return Err(format!(
                "durable certificate lane count mismatch: header={lane_count}, entries={lane_record_count}"
            ));
        }
        let mut lane_record_counts = Vec::with_capacity(lane_record_count);
        for _ in 0..lane_record_count {
            lane_record_counts.push(read_record_u32(
                bytes,
                &mut offset,
                "lane record cardinality",
            )?);
        }
        let db_size_pages = read_record_u32(bytes, &mut offset, "database size pages")?;
        let page_set_size = read_record_u32(bytes, &mut offset, "page set size")?;
        let mut wal_frame_payload_digest = [0_u8; 32];
        wal_frame_payload_digest.copy_from_slice(read_record_bytes(
            bytes,
            &mut offset,
            PARALLEL_WAL_FRAME_PAYLOAD_DIGEST_SIZE,
            "WAL frame payload digest",
        )?);
        let fallback_active = match read_record_bytes(bytes, &mut offset, 1, "fallback flag")?[0] {
            0 => false,
            1 => true,
            value => return Err(format!("invalid certificate fallback flag {value}")),
        };
        let certificate_crc32c = read_record_u32(bytes, &mut offset, "certificate checksum")?;
        if offset != envelope_crc_offset {
            return Err(format!(
                "durable certificate record has {} trailing payload bytes",
                envelope_crc_offset.saturating_sub(offset)
            ));
        }
        let certificate = ParallelWalCommitCertificate {
            format_version,
            residue,
            certificate_epoch,
            commit_seq_lo,
            commit_seq_hi,
            durable_segment_epoch,
            lane_count,
            lane_record_counts,
            db_size_pages,
            page_set_size,
            wal_frame_payload_digest,
            certificate_crc32c,
            fallback_active,
        };
        if !certificate.checksum_is_valid() {
            return Err("durable certificate checksum mismatch".to_owned());
        }

        Self::new(
            WalGenerationIdentity {
                checkpoint_seq,
                salts: crate::checksum::WalSalts { salt1, salt2 },
            },
            wal_frame_start,
            wal_frame_end,
            db_file_id,
            certificate,
        )
    }
}

/// Strictly decode a complete append-only certificate sidecar.
///
/// This helper intentionally rejects torn/trailing bytes. D1.d owns the
/// recovery policy that selects a valid prefix or conservative fallback after
/// a crash; D1.c only exposes deterministic record reconstruction.
pub fn decode_parallel_wal_durable_certificate_records(
    bytes: &[u8],
) -> Result<Vec<ParallelWalDurableCertificateRecord>, String> {
    let mut records = Vec::new();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let length_offset = offset
            .checked_add(10)
            .ok_or_else(|| "durable certificate sidecar offset overflow".to_owned())?;
        let length_end = length_offset
            .checked_add(4)
            .ok_or_else(|| "durable certificate sidecar length overflow".to_owned())?;
        let length_bytes = bytes.get(length_offset..length_end).ok_or_else(|| {
            format!("torn durable certificate record header at byte offset {offset}")
        })?;
        let record_len = usize::try_from(u32::from_le_bytes([
            length_bytes[0],
            length_bytes[1],
            length_bytes[2],
            length_bytes[3],
        ]))
        .map_err(|_| "durable certificate record length exceeds usize".to_owned())?;
        let record_end = offset
            .checked_add(record_len)
            .ok_or_else(|| "durable certificate record end overflow".to_owned())?;
        let record_bytes = bytes.get(offset..record_end).ok_or_else(|| {
            format!(
                "torn durable certificate record at byte offset {offset}: declared {record_len} bytes"
            )
        })?;
        records.push(ParallelWalDurableCertificateRecord::from_bytes(
            record_bytes,
        )?);
        offset = record_end;
    }
    Ok(records)
}

/// Bounded reader lookup selected by a certificate-backed publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelWalLookupMode {
    /// The commit-published visibility map is complete for this generation.
    AuthoritativeIndex,
    /// The conservative writer path produced the same bounded publication.
    ConservativeIndex,
}

/// Input to the irreducible ordered durability/publication residue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelWalDurabilityRequest {
    pub trace_id: u64,
    pub scenario_id: String,
    /// Caller-provided epoch, or zero to allocate the next combiner epoch.
    pub certificate_epoch: u64,
    /// Durable segment generation, or zero to bind it to the certificate epoch.
    pub durable_segment_epoch: u64,
    /// Number of commits certified by this group, not the number of frames.
    pub batch_size: u32,
    /// Deterministic group-commit order used for per-waiter publication handoff.
    pub batch_ids: Vec<u64>,
    /// Frame/record cardinality by lane id, including zero-count lanes.
    pub lane_record_counts: Vec<u32>,
    pub db_size_pages: u32,
    pub page_set_size: u32,
    /// Digest of the exact ordered WAL frame headers and payloads.
    pub wal_frame_payload_digest: [u8; 32],
    pub control_mode: ParallelWalOperatingMode,
    pub fallback_reason: Option<ParallelWalFallbackReason>,
    pub checkpoint_active: bool,
}

/// Conservative shadow candidate derived independently from the raw group
/// membership and live WAL interval.
///
/// The durability combiner uses this only in shadow-compare mode. Keeping the
/// evidence separate from [`ParallelWalDurabilityRequest`] prevents a clean
/// verdict from being manufactured by cloning or rebuilding the authoritative
/// candidate from its own summarized inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelWalConservativeShadowEvidence {
    pub certificate_epoch: u64,
    pub durable_segment_epoch: u64,
    pub batch_ids: Vec<u64>,
    pub lane_record_counts: Vec<u32>,
    pub db_size_pages: u32,
    pub page_set_size: u32,
    pub wal_frame_payload_digest: [u8; 32],
    pub control_mode: ParallelWalOperatingMode,
    pub fallback_reason: Option<ParallelWalFallbackReason>,
    pub checkpoint_active: bool,
    pub wal_frame_start: u64,
    pub wal_frame_end: u64,
}

/// Authoritative reader/checkpoint boundary published by the combiner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParallelWalVisibilitySnapshot {
    pub certificate_epoch: u64,
    pub visible_commit_seq: CommitSeq,
    pub durability_seq: u64,
    pub publication_generation: u64,
    pub db_size_pages: u32,
    pub page_set_size: u32,
    pub lookup_mode: ParallelWalLookupMode,
}

impl Default for ParallelWalVisibilitySnapshot {
    fn default() -> Self {
        Self {
            certificate_epoch: 0,
            visible_commit_seq: CommitSeq::ZERO,
            durability_seq: 0,
            publication_generation: 0,
            db_size_pages: 0,
            page_set_size: 0,
            lookup_mode: ParallelWalLookupMode::AuthoritativeIndex,
        }
    }
}

/// Receipt proving that one certificate crossed the configured WAL durability
/// boundary and was already published.
///
/// Under a synchronous policy that disables fsync, "durability" is deliberately
/// policy-relative: certificate and WAL writes retain their required ordering,
/// but neither is represented as power-loss-stable media.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelWalDurabilityReceipt {
    pub certificate: ParallelWalCommitCertificate,
    pub durability_seq: u64,
    pub publication_generation: u64,
    pub ordered_region_ns: u64,
    pub batch_size: u32,
    pub member_commit_seqs: Vec<(u64, CommitSeq)>,
    pub lookup_mode: ParallelWalLookupMode,
    pub control_mode: ParallelWalOperatingMode,
    pub shadow_certificate_verdict: ParallelWalShadowVerdict,
    pub fallback_reason: Option<ParallelWalFallbackReason>,
}

/// Opaque identity for a prepared parallel-WAL publication whose durability
/// outcome has not yet been reconciled.
///
/// Preparing a publication retains the combiner's ordered residue. The exact
/// handle must subsequently be passed to
/// [`ParallelWalDurabilityCombiner::finalize_pending_publication`] after the
/// durable certificate and WAL interval are proven committed, or to
/// [`ParallelWalDurabilityCombiner::abort_pending_publication`] after they are
/// proven not committed. Merely receiving an I/O error is not proof of either
/// outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelWalPendingPublication {
    combiner_id: u64,
    pending_id: u64,
    certificate: ParallelWalCommitCertificate,
}

impl ParallelWalPendingPublication {
    /// Monotonic identity within the originating combiner.
    ///
    /// Exact reconciliation also validates the handle's opaque combiner
    /// identity; this value alone does not authorize finalize or abort.
    #[must_use]
    pub const fn pending_id(&self) -> u64 {
        self.pending_id
    }

    /// Certificate bytes that must precede the matching WAL commit interval.
    #[must_use]
    pub const fn certificate(&self) -> &ParallelWalCommitCertificate {
        &self.certificate
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParallelWalCombinerError {
    OrderedResidueBusy,
    Cancelled,
    EmptyBatch,
    BatchIdentityCountMismatch {
        batch_size: u32,
        identity_count: usize,
    },
    DuplicateBatchIdentity {
        batch_id: u64,
    },
    TooManyLanes {
        lane_count: usize,
    },
    EmptyLaneEvidence,
    StaleCertificateEpoch {
        current: u64,
        proposed: u64,
    },
    StaleSegmentEpoch {
        current: u64,
        proposed: u64,
    },
    CommitSequenceOverflow,
    DurabilitySequenceOverflow,
    PublicationGenerationOverflow,
    CertificateChecksumMismatch,
    CertificateGap {
        expected: CommitSeq,
        actual: CommitSeq,
    },
    DuplicateOrStalePublication {
        published: CommitSeq,
        proposed: CommitSeq,
    },
    ShadowCertificateMismatch,
    MissingConservativeShadowEvidence,
    DurabilityWriteFailed(String),
    PendingPublicationIdOverflow,
    PendingPublicationMissing {
        pending_id: u64,
    },
    PendingPublicationOwnerMismatch {
        expected_combiner_id: u64,
        actual_combiner_id: u64,
    },
    PendingPublicationMismatch {
        expected_pending_id: u64,
        actual_pending_id: u64,
    },
}

impl std::fmt::Display for ParallelWalCombinerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OrderedResidueBusy => {
                f.write_str("parallel WAL ordered durability residue is already active")
            }
            Self::Cancelled => f.write_str("parallel WAL ordered durability residue was cancelled"),
            Self::EmptyBatch => f.write_str("parallel WAL certificate cannot cover zero commits"),
            Self::BatchIdentityCountMismatch {
                batch_size,
                identity_count,
            } => write!(
                f,
                "parallel WAL certificate covers {batch_size} commits but has {identity_count} batch identities"
            ),
            Self::DuplicateBatchIdentity { batch_id } => write!(
                f,
                "parallel WAL certificate repeats batch identity {batch_id}"
            ),
            Self::TooManyLanes { lane_count } => {
                write!(
                    f,
                    "parallel WAL certificate has {lane_count} lanes; maximum is {MAX_PARALLEL_WAL_LANE_COUNT}"
                )
            }
            Self::EmptyLaneEvidence => {
                f.write_str("parallel WAL certificate requires lane record evidence")
            }
            Self::StaleCertificateEpoch { current, proposed } => write!(
                f,
                "parallel WAL certificate epoch {proposed} is not newer than {current}"
            ),
            Self::StaleSegmentEpoch { current, proposed } => write!(
                f,
                "parallel WAL durable segment epoch {proposed} precedes {current}"
            ),
            Self::CommitSequenceOverflow => f.write_str("parallel WAL commit sequence overflow"),
            Self::DurabilitySequenceOverflow => {
                f.write_str("parallel WAL durability sequence overflow")
            }
            Self::PublicationGenerationOverflow => {
                f.write_str("parallel WAL publication generation overflow")
            }
            Self::CertificateChecksumMismatch => {
                f.write_str("parallel WAL certificate checksum mismatch")
            }
            Self::CertificateGap { expected, actual } => write!(
                f,
                "parallel WAL certificate gap: expected {expected}, got {actual}"
            ),
            Self::DuplicateOrStalePublication {
                published,
                proposed,
            } => write!(
                f,
                "parallel WAL publication {proposed} is not newer than {published}"
            ),
            Self::ShadowCertificateMismatch => {
                f.write_str("parallel WAL shadow certificate mismatch")
            }
            Self::MissingConservativeShadowEvidence => f.write_str(
                "parallel WAL shadow mode requires independently-derived conservative evidence",
            ),
            Self::DurabilityWriteFailed(detail) => {
                write!(
                    f,
                    "parallel WAL certificate durability write failed: {detail}"
                )
            }
            Self::PendingPublicationIdOverflow => {
                f.write_str("parallel WAL pending-publication identity overflow")
            }
            Self::PendingPublicationMissing { pending_id } => write!(
                f,
                "parallel WAL pending publication {pending_id} no longer exists"
            ),
            Self::PendingPublicationOwnerMismatch {
                expected_combiner_id,
                actual_combiner_id,
            } => write!(
                f,
                "parallel WAL pending publication belongs to combiner {actual_combiner_id}, expected combiner {expected_combiner_id}"
            ),
            Self::PendingPublicationMismatch {
                expected_pending_id,
                actual_pending_id,
            } => write!(
                f,
                "parallel WAL pending publication mismatch: expected {expected_pending_id}, got {actual_pending_id}"
            ),
        }
    }
}

impl std::error::Error for ParallelWalCombinerError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ParallelWalCombinerMetricsSnapshot {
    pub certificates_published: u64,
    pub commits_published: u64,
    pub ordered_region_ns_total: u64,
    pub ordered_region_ns_max: u64,
    pub fallback_entries: u64,
    pub shadow_comparisons: u64,
    pub shadow_mismatches: u64,
}

#[derive(Debug, Clone, Default)]
struct ParallelWalCombinerState {
    visibility: ParallelWalVisibilitySnapshot,
    durable_segment_epoch: u64,
    last_certificate_crc32c: u32,
    metrics: ParallelWalCombinerMetricsSnapshot,
}

static NEXT_PARALLEL_WAL_COMBINER_ID: AtomicU64 = AtomicU64::new(0);

fn atomic_checked_increment(
    counter: &AtomicU64,
    set_order: Ordering,
    fetch_order: Ordering,
) -> Result<u64, u64> {
    let mut current = counter.load(fetch_order);
    loop {
        let Some(next) = current.checked_add(1) else {
            return Err(current);
        };
        match counter.compare_exchange_weak(current, next, set_order, fetch_order) {
            Ok(previous) => return Ok(previous),
            Err(observed) => current = observed,
        }
    }
}

fn next_parallel_wal_combiner_id() -> u64 {
    atomic_checked_increment(
        &NEXT_PARALLEL_WAL_COMBINER_ID,
        Ordering::Relaxed,
        Ordering::Relaxed,
    )
    .map(|previous| previous + 1)
    .expect("parallel WAL combiner identity space exhausted")
}

/// Tiny serialized residue joining already-parallel lane staging to durable,
/// authoritative visibility publication.
///
/// The supplied callback is the durability boundary. State is advanced only
/// after it returns success. Callers must write or prove the certificate's
/// durable equivalent in that callback; page-plane work and structured logging
/// stay outside this lock.
#[derive(Debug)]
pub struct ParallelWalDurabilityCombiner {
    combiner_id: u64,
    ordered_residue_claimed: AtomicBool,
    ordered_residue_wait_lock: Mutex<()>,
    ordered_residue_wait: Condvar,
    #[cfg(test)]
    ordered_residue_blocking_waiters: std::sync::atomic::AtomicUsize,
    next_pending_publication_id: AtomicU64,
    pending_publication: Mutex<Option<PendingParallelWalPublicationState>>,
    state: Mutex<ParallelWalCombinerState>,
}

struct ParallelWalOrderedResidueGuard<'a> {
    claimed: &'a AtomicBool,
    wait_lock: &'a Mutex<()>,
    wait: &'a Condvar,
    release_claim_on_drop: bool,
}

#[cfg(test)]
struct ParallelWalBlockingWaiterGuard<'a> {
    waiters: &'a std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl Drop for ParallelWalBlockingWaiterGuard<'_> {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for ParallelWalOrderedResidueGuard<'_> {
    fn drop(&mut self) {
        if !self.release_claim_on_drop {
            return;
        }
        let _wait_guard = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.claimed.store(false, Ordering::Release);
        self.wait.notify_all();
    }
}

impl ParallelWalOrderedResidueGuard<'_> {
    fn retain_claim_on_drop(&mut self) {
        self.release_claim_on_drop = false;
    }
}

#[derive(Debug)]
struct PreparedParallelWalPublication {
    certificate: ParallelWalCommitCertificate,
    fallback_reason: Option<ParallelWalFallbackReason>,
    shadow_certificate_verdict: ParallelWalShadowVerdict,
    durability_seq: u64,
    publication_generation: u64,
    lookup_mode: ParallelWalLookupMode,
}

#[derive(Debug)]
struct PendingParallelWalPublicationState {
    combiner_id: u64,
    pending_id: u64,
    request: ParallelWalDurabilityRequest,
    staged_state: ParallelWalCombinerState,
    prepared: PreparedParallelWalPublication,
    ordered_start: Instant,
}

impl ParallelWalDurabilityCombiner {
    #[must_use]
    pub fn new(initial_visibility: ParallelWalVisibilitySnapshot) -> Self {
        Self {
            combiner_id: next_parallel_wal_combiner_id(),
            ordered_residue_claimed: AtomicBool::new(false),
            ordered_residue_wait_lock: Mutex::new(()),
            ordered_residue_wait: Condvar::new(),
            #[cfg(test)]
            ordered_residue_blocking_waiters: std::sync::atomic::AtomicUsize::new(0),
            next_pending_publication_id: AtomicU64::new(0),
            pending_publication: Mutex::new(None),
            state: Mutex::new(ParallelWalCombinerState {
                durable_segment_epoch: initial_visibility.certificate_epoch,
                visibility: initial_visibility,
                ..ParallelWalCombinerState::default()
            }),
        }
    }

    /// Raise the certificate allocator to an independently verified durable
    /// visibility floor before assigning another interval.
    ///
    /// A long-lived process-local combiner can outlive a pager refresh or a
    /// checkpoint handoff. In that case the pager's durable commit identity may
    /// be newer than the combiner's last locally published certificate even
    /// when no newer certificate record exists in the current WAL generation.
    /// The caller must derive this floor from durable pager state while holding
    /// the external writer gate.
    pub fn reconcile_durable_visibility_floor(&self, durable_visible_commit_seq: CommitSeq) {
        let _ordered_residue = self.claim_ordered_residue_blocking();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if durable_visible_commit_seq > state.visibility.visible_commit_seq {
            state.visibility.visible_commit_seq = durable_visible_commit_seq;
            // The previous checksum identifies the certificate that established
            // the old horizon. Once an independently verified pager floor moves
            // past it, an equal authorized seed must be allowed to bind the new
            // horizon's certificate identity.
            state.last_certificate_crc32c = 0;
        }
    }

    /// Advance this process-local combiner from an already-authorized durable
    /// sidecar tail before assigning the next interval.
    ///
    /// The caller must validate the record against the live WAL generation,
    /// complete frame boundary, and commit marker while holding the external
    /// writer gate. This method only reconciles monotonic certificate clocks;
    /// it does not publish the imported certificate to readers.
    pub fn reconcile_authorized_seed(
        &self,
        certificate: &ParallelWalCommitCertificate,
    ) -> Result<(), ParallelWalCombinerError> {
        let _ordered_residue = self.claim_ordered_residue_blocking();
        if !certificate.checksum_is_valid() {
            return Err(ParallelWalCombinerError::CertificateChecksumMismatch);
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if certificate.commit_seq_hi <= state.visibility.visible_commit_seq {
            if certificate.commit_seq_hi < state.visibility.visible_commit_seq
                && state.last_certificate_crc32c != 0
            {
                return Ok(());
            }
            if state.last_certificate_crc32c != 0
                && certificate.commit_seq_hi == state.visibility.visible_commit_seq
                && state.last_certificate_crc32c != certificate.certificate_crc32c
            {
                return Err(ParallelWalCombinerError::DuplicateOrStalePublication {
                    published: state.visibility.visible_commit_seq,
                    proposed: certificate.commit_seq_hi,
                });
            }
            // A fresh process may already have reconstructed the same or a
            // newer logical commit clock from the WAL. Seed the independent
            // certificate clocks as well so its first emitted epoch cannot
            // reuse the durable tail's identity.
            state.visibility.certificate_epoch = state
                .visibility
                .certificate_epoch
                .max(certificate.certificate_epoch);
            state.visibility.durability_seq = state
                .visibility
                .durability_seq
                .max(certificate.certificate_epoch);
            state.visibility.publication_generation = state
                .visibility
                .publication_generation
                .max(certificate.certificate_epoch);
            state.durable_segment_epoch = state
                .durable_segment_epoch
                .max(certificate.durable_segment_epoch);
            state.last_certificate_crc32c = certificate.certificate_crc32c;
            return Ok(());
        }
        if certificate.certificate_epoch <= state.visibility.certificate_epoch {
            return Err(ParallelWalCombinerError::StaleCertificateEpoch {
                current: state.visibility.certificate_epoch,
                proposed: certificate.certificate_epoch,
            });
        }
        if certificate.durable_segment_epoch < state.durable_segment_epoch {
            return Err(ParallelWalCombinerError::StaleSegmentEpoch {
                current: state.durable_segment_epoch,
                proposed: certificate.durable_segment_epoch,
            });
        }

        let durability_seq = state
            .visibility
            .durability_seq
            .max(certificate.certificate_epoch);
        let publication_generation = state
            .visibility
            .publication_generation
            .max(certificate.certificate_epoch);
        state.visibility = ParallelWalVisibilitySnapshot {
            certificate_epoch: certificate.certificate_epoch,
            visible_commit_seq: certificate.commit_seq_hi,
            durability_seq,
            publication_generation,
            db_size_pages: certificate.db_size_pages,
            page_set_size: certificate.page_set_size,
            lookup_mode: if certificate.fallback_active {
                ParallelWalLookupMode::ConservativeIndex
            } else {
                ParallelWalLookupMode::AuthoritativeIndex
            },
        };
        state.durable_segment_epoch = certificate.durable_segment_epoch;
        state.last_certificate_crc32c = certificate.certificate_crc32c;
        Ok(())
    }

    fn try_claim_ordered_residue(
        &self,
    ) -> Result<ParallelWalOrderedResidueGuard<'_>, ParallelWalCombinerError> {
        self.ordered_residue_claimed
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .map_err(|_| ParallelWalCombinerError::OrderedResidueBusy)?;
        Ok(ParallelWalOrderedResidueGuard {
            claimed: &self.ordered_residue_claimed,
            wait_lock: &self.ordered_residue_wait_lock,
            wait: &self.ordered_residue_wait,
            release_claim_on_drop: true,
        })
    }

    fn claim_ordered_residue_blocking(&self) -> ParallelWalOrderedResidueGuard<'_> {
        if let Ok(guard) = self.try_claim_ordered_residue() {
            return guard;
        }

        #[cfg(test)]
        let _waiter_guard = {
            self.ordered_residue_blocking_waiters
                .fetch_add(1, Ordering::AcqRel);
            ParallelWalBlockingWaiterGuard {
                waiters: &self.ordered_residue_blocking_waiters,
            }
        };
        let mut wait_guard = self
            .ordered_residue_wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Ok(guard) = self.try_claim_ordered_residue() {
                drop(wait_guard);
                return guard;
            }
            wait_guard = self
                .ordered_residue_wait
                .wait(wait_guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    async fn claim_ordered_residue(
        &self,
        cx: &Cx,
    ) -> Result<ParallelWalOrderedResidueGuard<'_>, ParallelWalCombinerError> {
        loop {
            cx.checkpoint()
                .map_err(|_| ParallelWalCombinerError::Cancelled)?;
            match self.try_claim_ordered_residue() {
                Ok(guard) => return Ok(guard),
                Err(ParallelWalCombinerError::OrderedResidueBusy) => {
                    asupersync::runtime::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn next_pending_publication_id(&self) -> Result<u64, ParallelWalCombinerError> {
        atomic_checked_increment(
            &self.next_pending_publication_id,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map(|previous| previous + 1)
        .map_err(|_| ParallelWalCombinerError::PendingPublicationIdOverflow)
    }

    fn prepare_pending_publication_from_claim<S>(
        &self,
        mut ordered_residue: ParallelWalOrderedResidueGuard<'_>,
        request: ParallelWalDurabilityRequest,
        shadow_certificate: Option<S>,
        conservative_shadow_evidence: Option<ParallelWalConservativeShadowEvidence>,
    ) -> Result<ParallelWalPendingPublication, ParallelWalCombinerError>
    where
        S: FnOnce(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate,
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let ordered_start = Instant::now();
        let mut staged_state = state.clone();
        let prepared = match prepare_parallel_wal_publication(
            &mut staged_state,
            &request,
            shadow_certificate,
            conservative_shadow_evidence.as_ref(),
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                preserve_shadow_failure_metrics(&mut state, &staged_state);
                return Err(error);
            }
        };
        drop(state);

        let pending_id = self.next_pending_publication_id()?;
        let pending = ParallelWalPendingPublication {
            combiner_id: self.combiner_id,
            pending_id,
            certificate: prepared.certificate.clone(),
        };
        let mut slot = self
            .pending_publication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(
            slot.is_none(),
            "an ordered-residue claim cannot coexist with another pending publication"
        );
        if slot.is_some() {
            return Err(ParallelWalCombinerError::OrderedResidueBusy);
        }
        *slot = Some(PendingParallelWalPublicationState {
            combiner_id: self.combiner_id,
            pending_id,
            request,
            staged_state,
            prepared,
            ordered_start,
        });
        ordered_residue.retain_claim_on_drop();
        drop(slot);
        Ok(pending)
    }

    fn prepare_pending_publication_blocking<S>(
        &self,
        request: ParallelWalDurabilityRequest,
        shadow_certificate: Option<S>,
        conservative_shadow_evidence: Option<ParallelWalConservativeShadowEvidence>,
    ) -> Result<ParallelWalPendingPublication, ParallelWalCombinerError>
    where
        S: FnOnce(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate,
    {
        let ordered_residue = self.claim_ordered_residue_blocking();
        self.prepare_pending_publication_from_claim(
            ordered_residue,
            request,
            shadow_certificate,
            conservative_shadow_evidence,
        )
    }

    async fn prepare_pending_publication_async_inner<S>(
        &self,
        cx: &Cx,
        request: ParallelWalDurabilityRequest,
        shadow_certificate: Option<S>,
        conservative_shadow_evidence: Option<ParallelWalConservativeShadowEvidence>,
    ) -> Result<ParallelWalPendingPublication, ParallelWalCombinerError>
    where
        S: FnOnce(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate,
    {
        let ordered_residue = self.claim_ordered_residue(cx).await?;
        // No durable side effect exists yet, so cancellation can release the
        // claim and leave the interval available for ordinary retry.
        cx.checkpoint()
            .map_err(|_| ParallelWalCombinerError::Cancelled)?;
        self.prepare_pending_publication_from_claim(
            ordered_residue,
            request,
            shadow_certificate,
            conservative_shadow_evidence,
        )
    }

    /// Prepare and retain the exact ordered publication that a caller will
    /// make durable.
    ///
    /// The returned handle owns no resources itself; the combiner retains the
    /// ordered residue even if the handle or calling future is dropped.
    /// Recovery must prove the physical outcome before finalizing or aborting
    /// the handle.
    pub async fn prepare_pending_publication(
        &self,
        cx: &Cx,
        request: ParallelWalDurabilityRequest,
    ) -> Result<ParallelWalPendingPublication, ParallelWalCombinerError> {
        self.prepare_pending_publication_async_inner(
            cx,
            request,
            Option::<fn(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate>::None,
            None,
        )
        .await
    }

    /// Prepare an ordered publication with independently reconstructed
    /// conservative shadow evidence.
    pub async fn prepare_pending_publication_with_conservative_shadow(
        &self,
        cx: &Cx,
        request: ParallelWalDurabilityRequest,
        shadow_evidence: ParallelWalConservativeShadowEvidence,
    ) -> Result<ParallelWalPendingPublication, ParallelWalCombinerError> {
        self.prepare_pending_publication_async_inner(
            cx,
            request,
            Option::<fn(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate>::None,
            Some(shadow_evidence),
        )
        .await
    }

    /// Return the currently retained publication, if any.
    ///
    /// This is a recovery lookup, not an authorization verdict. The caller
    /// must still inspect lower-layer completion and durable WAL evidence.
    #[must_use]
    pub fn pending_publication(&self) -> Option<ParallelWalPendingPublication> {
        self.pending_publication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|pending| ParallelWalPendingPublication {
                combiner_id: pending.combiner_id,
                pending_id: pending.pending_id,
                certificate: pending.prepared.certificate.clone(),
            })
    }

    fn take_exact_pending_publication(
        &self,
        pending: &ParallelWalPendingPublication,
    ) -> Result<PendingParallelWalPublicationState, ParallelWalCombinerError> {
        let mut slot = self
            .pending_publication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(current) = slot.as_ref() else {
            return Err(ParallelWalCombinerError::PendingPublicationMissing {
                pending_id: pending.pending_id,
            });
        };
        if pending.combiner_id != self.combiner_id || current.combiner_id != pending.combiner_id {
            return Err(ParallelWalCombinerError::PendingPublicationOwnerMismatch {
                expected_combiner_id: self.combiner_id,
                actual_combiner_id: pending.combiner_id,
            });
        }
        if current.pending_id != pending.pending_id
            || current.prepared.certificate != pending.certificate
        {
            return Err(ParallelWalCombinerError::PendingPublicationMismatch {
                expected_pending_id: current.pending_id,
                actual_pending_id: pending.pending_id,
            });
        }
        Ok(slot
            .take()
            .expect("pending publication was present after exact validation"))
    }

    fn release_retained_ordered_residue(&self) {
        let _wait_guard = self
            .ordered_residue_wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let was_claimed = self.ordered_residue_claimed.swap(false, Ordering::Release);
        debug_assert!(
            was_claimed,
            "a retained publication must own the ordered residue"
        );
        self.ordered_residue_wait.notify_all();
    }

    /// Publish an exact pending interval after durable recovery evidence
    /// authorizes its certificate and matching commit marker.
    pub fn finalize_pending_publication(
        &self,
        pending: &ParallelWalPendingPublication,
    ) -> Result<ParallelWalDurabilityReceipt, ParallelWalCombinerError> {
        let PendingParallelWalPublicationState {
            request,
            mut staged_state,
            prepared,
            ordered_start,
            ..
        } = self.take_exact_pending_publication(pending)?;
        let receipt =
            finalize_parallel_wal_publication(&mut staged_state, &request, prepared, ordered_start);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = staged_state;
        drop(state);
        self.release_retained_ordered_residue();
        trace_parallel_wal_publication(&request, &receipt);
        Ok(receipt)
    }

    /// Release an exact pending interval after recovery proves that no matching
    /// commit marker was authorized.
    ///
    /// An I/O error alone is insufficient. The caller must also prove all
    /// started lower-layer writes terminal before invoking this method.
    pub fn abort_pending_publication(
        &self,
        pending: &ParallelWalPendingPublication,
    ) -> Result<(), ParallelWalCombinerError> {
        let _discarded = self.take_exact_pending_publication(pending)?;
        self.release_retained_ordered_residue();
        Ok(())
    }

    pub fn certify_and_publish<F>(
        &self,
        request: ParallelWalDurabilityRequest,
        durable_write: F,
    ) -> Result<ParallelWalDurabilityReceipt, ParallelWalCombinerError>
    where
        F: FnOnce(&ParallelWalCommitCertificate) -> Result<(), String>,
    {
        self.certify_and_publish_inner(
            request,
            durable_write,
            Option::<fn(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate>::None,
            None,
        )
    }

    /// Asynchronously persist a commit certificate and publish it atomically.
    ///
    /// The combiner's visibility, clocks, checksum, and metrics remain exactly
    /// unchanged until `durable_write` resolves successfully. Cancellation
    /// before preparation leaves the interval available for ordinary retry.
    /// Once a pending publication exists, success advances it and every other
    /// outcome retains it for explicit physical reconciliation; an I/O error
    /// does not prove whether the commit marker reached its destination.
    pub async fn certify_and_publish_async<F, Fut>(
        &self,
        cx: &Cx,
        request: ParallelWalDurabilityRequest,
        durable_write: F,
    ) -> Result<ParallelWalDurabilityReceipt, ParallelWalCombinerError>
    where
        F: FnOnce(ParallelWalCommitCertificate) -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        self.certify_and_publish_inner_async(
            cx,
            request,
            durable_write,
            Option::<fn(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate>::None,
            None,
        )
        .await
    }

    pub fn certify_and_publish_with_shadow<F, S>(
        &self,
        request: ParallelWalDurabilityRequest,
        durable_write: F,
        shadow_certificate: S,
    ) -> Result<ParallelWalDurabilityReceipt, ParallelWalCombinerError>
    where
        F: FnOnce(&ParallelWalCommitCertificate) -> Result<(), String>,
        S: FnOnce(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate,
    {
        self.certify_and_publish_inner(request, durable_write, Some(shadow_certificate), None)
    }

    /// Certify one group while comparing against evidence independently
    /// reconstructed from raw group membership and live WAL boundaries.
    pub fn certify_and_publish_with_conservative_shadow<F>(
        &self,
        request: ParallelWalDurabilityRequest,
        shadow_evidence: ParallelWalConservativeShadowEvidence,
        durable_write: F,
    ) -> Result<ParallelWalDurabilityReceipt, ParallelWalCombinerError>
    where
        F: FnOnce(&ParallelWalCommitCertificate) -> Result<(), String>,
    {
        self.certify_and_publish_inner(
            request,
            durable_write,
            Option::<fn(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate>::None,
            Some(shadow_evidence),
        )
    }

    /// Asynchronously certify and publish while independently reconstructing
    /// the conservative shadow certificate from raw group/WAL evidence.
    ///
    /// As with [`Self::certify_and_publish_async`], no combiner state becomes
    /// visible until the awaited durability callback succeeds.
    pub async fn certify_and_publish_with_conservative_shadow_async<F, Fut>(
        &self,
        cx: &Cx,
        request: ParallelWalDurabilityRequest,
        shadow_evidence: ParallelWalConservativeShadowEvidence,
        durable_write: F,
    ) -> Result<ParallelWalDurabilityReceipt, ParallelWalCombinerError>
    where
        F: FnOnce(ParallelWalCommitCertificate) -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        self.certify_and_publish_inner_async(
            cx,
            request,
            durable_write,
            Option::<fn(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate>::None,
            Some(shadow_evidence),
        )
        .await
    }

    fn certify_and_publish_inner<F, S>(
        &self,
        request: ParallelWalDurabilityRequest,
        durable_write: F,
        shadow_certificate: Option<S>,
        conservative_shadow_evidence: Option<ParallelWalConservativeShadowEvidence>,
    ) -> Result<ParallelWalDurabilityReceipt, ParallelWalCombinerError>
    where
        F: FnOnce(&ParallelWalCommitCertificate) -> Result<(), String>,
        S: FnOnce(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate,
    {
        let pending = self.prepare_pending_publication_blocking(
            request,
            shadow_certificate,
            conservative_shadow_evidence,
        )?;
        durable_write(pending.certificate())
            .map_err(ParallelWalCombinerError::DurabilityWriteFailed)?;
        self.finalize_pending_publication(&pending)
    }

    // The retained pending state spans the durability await, but the state
    // mutex does not. Contenders cooperatively yield, while synchronous
    // visibility and metrics readers can still inspect the last published
    // snapshot without blocking an executor thread on disk I/O.
    async fn certify_and_publish_inner_async<F, Fut, S>(
        &self,
        cx: &Cx,
        request: ParallelWalDurabilityRequest,
        durable_write: F,
        shadow_certificate: Option<S>,
        conservative_shadow_evidence: Option<ParallelWalConservativeShadowEvidence>,
    ) -> Result<ParallelWalDurabilityReceipt, ParallelWalCombinerError>
    where
        F: FnOnce(ParallelWalCommitCertificate) -> Fut,
        Fut: Future<Output = Result<(), String>>,
        S: FnOnce(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate,
    {
        let pending = self
            .prepare_pending_publication_async_inner(
                cx,
                request,
                shadow_certificate,
                conservative_shadow_evidence,
            )
            .await?;

        // Once a lower-layer write begins, neither cancellation nor an I/O
        // error proves whether bytes reached the OS or stable media. The
        // pending publication therefore remains retained unless this callback
        // succeeds. Callers that receive an error must reconcile the exact
        // handle exposed by `pending_publication()`.
        let durability_result = {
            let _durability_mask = cx.masked();
            durable_write(pending.certificate.clone()).await
        };
        durability_result.map_err(ParallelWalCombinerError::DurabilityWriteFailed)?;
        self.finalize_pending_publication(&pending)
    }

    /// Validate a receipt proposed for publication by an external transport.
    /// This rejects damaged, duplicate, stale, and non-contiguous handoffs.
    pub fn validate_external_publication(
        &self,
        receipt: &ParallelWalDurabilityReceipt,
    ) -> Result<(), ParallelWalCombinerError> {
        if !receipt.certificate.checksum_is_valid() {
            return Err(ParallelWalCombinerError::CertificateChecksumMismatch);
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if receipt.certificate.commit_seq_hi <= state.visibility.visible_commit_seq {
            return Err(ParallelWalCombinerError::DuplicateOrStalePublication {
                published: state.visibility.visible_commit_seq,
                proposed: receipt.certificate.commit_seq_hi,
            });
        }
        let expected = checked_next_commit_seq(state.visibility.visible_commit_seq)?;
        if receipt.certificate.commit_seq_lo != expected {
            return Err(ParallelWalCombinerError::CertificateGap {
                expected,
                actual: receipt.certificate.commit_seq_lo,
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn visibility_snapshot(&self) -> ParallelWalVisibilitySnapshot {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .visibility
    }

    #[must_use]
    pub fn metrics_snapshot(&self) -> ParallelWalCombinerMetricsSnapshot {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .metrics
    }
}

impl Default for ParallelWalDurabilityCombiner {
    fn default() -> Self {
        Self::new(ParallelWalVisibilitySnapshot::default())
    }
}

fn prepare_parallel_wal_publication<S>(
    state: &mut ParallelWalCombinerState,
    request: &ParallelWalDurabilityRequest,
    shadow_certificate: Option<S>,
    conservative_shadow_evidence: Option<&ParallelWalConservativeShadowEvidence>,
) -> Result<PreparedParallelWalPublication, ParallelWalCombinerError>
where
    S: FnOnce(&ParallelWalCommitCertificate) -> ParallelWalCommitCertificate,
{
    let (mut certificate, fallback_reason) = build_commit_certificate(state, request)?;
    certificate.certificate_crc32c = certificate.computed_crc32c();

    let shadow_certificate_verdict = if let Some(shadow_certificate) = shadow_certificate {
        state.metrics.shadow_comparisons = state.metrics.shadow_comparisons.saturating_add(1);
        let shadow = shadow_certificate(&certificate);
        if shadow == certificate {
            ParallelWalShadowVerdict::Clean
        } else {
            state.metrics.shadow_mismatches = state.metrics.shadow_mismatches.saturating_add(1);
            return Err(ParallelWalCombinerError::ShadowCertificateMismatch);
        }
    } else if matches!(
        request.control_mode,
        ParallelWalOperatingMode::ShadowCompare
    ) {
        state.metrics.shadow_comparisons = state.metrics.shadow_comparisons.saturating_add(1);
        let evidence = conservative_shadow_evidence
            .ok_or(ParallelWalCombinerError::MissingConservativeShadowEvidence)?;
        // Reconstruct the conservative candidate from raw group/WAL evidence
        // through a separate implementation. Every certificate field and the
        // fallback decision participates in the comparison before durability.
        let (mut shadow, shadow_fallback_reason) =
            build_conservative_shadow_certificate(state, evidence)?;
        shadow.certificate_crc32c = shadow.computed_crc32c();
        if shadow == certificate && shadow_fallback_reason == fallback_reason {
            ParallelWalShadowVerdict::Clean
        } else {
            state.metrics.shadow_mismatches = state.metrics.shadow_mismatches.saturating_add(1);
            return Err(ParallelWalCombinerError::ShadowCertificateMismatch);
        }
    } else {
        ParallelWalShadowVerdict::NotRun
    };

    // Validate every fallible publication clock before crossing the durable
    // boundary. Once the callback succeeds, finalization must be infallible.
    let durability_seq = state
        .visibility
        .durability_seq
        .checked_add(1)
        .ok_or(ParallelWalCombinerError::DurabilitySequenceOverflow)?;
    let publication_generation = state
        .visibility
        .publication_generation
        .checked_add(1)
        .ok_or(ParallelWalCombinerError::PublicationGenerationOverflow)?;
    let lookup_mode = if certificate.fallback_active {
        ParallelWalLookupMode::ConservativeIndex
    } else {
        ParallelWalLookupMode::AuthoritativeIndex
    };

    Ok(PreparedParallelWalPublication {
        certificate,
        fallback_reason,
        shadow_certificate_verdict,
        durability_seq,
        publication_generation,
        lookup_mode,
    })
}

fn preserve_shadow_failure_metrics(
    state: &mut ParallelWalCombinerState,
    staged_state: &ParallelWalCombinerState,
) {
    // Preparation failures historically remain observable even though no
    // publication occurs. Copy only those diagnostic counters; durability
    // failures never call this helper and therefore leave all state unchanged.
    state.metrics.shadow_comparisons = staged_state.metrics.shadow_comparisons;
    state.metrics.shadow_mismatches = staged_state.metrics.shadow_mismatches;
}

fn finalize_parallel_wal_publication(
    state: &mut ParallelWalCombinerState,
    request: &ParallelWalDurabilityRequest,
    prepared: PreparedParallelWalPublication,
    ordered_start: Instant,
) -> ParallelWalDurabilityReceipt {
    let PreparedParallelWalPublication {
        certificate,
        fallback_reason,
        shadow_certificate_verdict,
        durability_seq,
        publication_generation,
        lookup_mode,
    } = prepared;

    state.visibility = ParallelWalVisibilitySnapshot {
        certificate_epoch: certificate.certificate_epoch,
        visible_commit_seq: certificate.commit_seq_hi,
        durability_seq,
        publication_generation,
        db_size_pages: certificate.db_size_pages,
        page_set_size: certificate.page_set_size,
        lookup_mode,
    };
    state.durable_segment_epoch = certificate.durable_segment_epoch;
    state.last_certificate_crc32c = certificate.certificate_crc32c;
    state.metrics.certificates_published = state.metrics.certificates_published.saturating_add(1);
    state.metrics.commits_published = state
        .metrics
        .commits_published
        .saturating_add(u64::from(request.batch_size));
    if certificate.fallback_active {
        state.metrics.fallback_entries = state.metrics.fallback_entries.saturating_add(1);
    }
    let ordered_region_ns = u64::try_from(ordered_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    state.metrics.ordered_region_ns_total = state
        .metrics
        .ordered_region_ns_total
        .saturating_add(ordered_region_ns);
    state.metrics.ordered_region_ns_max =
        state.metrics.ordered_region_ns_max.max(ordered_region_ns);

    ParallelWalDurabilityReceipt {
        member_commit_seqs: request
            .batch_ids
            .iter()
            .enumerate()
            .map(|(index, batch_id)| {
                (
                    *batch_id,
                    CommitSeq::new(
                        certificate
                            .commit_seq_lo
                            .get()
                            .saturating_add(u64::try_from(index).unwrap_or(u64::MAX)),
                    ),
                )
            })
            .collect(),
        certificate,
        durability_seq,
        publication_generation,
        ordered_region_ns,
        batch_size: request.batch_size,
        lookup_mode,
        control_mode: request.control_mode,
        shadow_certificate_verdict,
        fallback_reason,
    }
}

fn trace_parallel_wal_publication(
    request: &ParallelWalDurabilityRequest,
    receipt: &ParallelWalDurabilityReceipt,
) {
    tracing::debug!(
        target: "fsqlite::wal::durability_combiner",
        trace_id = request.trace_id,
        scenario_id = request.scenario_id.as_str(),
        commit_certificate = receipt.certificate.certificate_crc32c,
        certificate_epoch = receipt.certificate.certificate_epoch,
        commit_seq_lo = receipt.certificate.commit_seq_lo.get(),
        commit_seq_hi = receipt.certificate.commit_seq_hi.get(),
        durability_seq = receipt.durability_seq,
        publication_generation = receipt.publication_generation,
        ordered_region_ns = receipt.ordered_region_ns,
        batch_size = receipt.batch_size,
        lookup_mode = parallel_wal_lookup_mode_name(receipt.lookup_mode),
        control_mode = parallel_wal_mode_name(receipt.control_mode),
        shadow_certificate_verdict =
            parallel_wal_shadow_verdict_name(receipt.shadow_certificate_verdict),
        compatibility_selector = PARALLEL_WAL_COMPATIBILITY_SELECTOR,
        fallback_reason = parallel_wal_fallback_reason_name(receipt.fallback_reason),
        "published durable parallel WAL commit certificate"
    );
}

impl ParallelWalDurabilityReceipt {
    #[must_use]
    pub fn commit_seq_for_batch(&self, batch_id: u64) -> Option<CommitSeq> {
        self.member_commit_seqs
            .iter()
            .find_map(|(member_id, commit_seq)| (*member_id == batch_id).then_some(*commit_seq))
    }
}

fn checked_next_commit_seq(seq: CommitSeq) -> Result<CommitSeq, ParallelWalCombinerError> {
    seq.get()
        .checked_add(1)
        .map(CommitSeq::new)
        .ok_or(ParallelWalCombinerError::CommitSequenceOverflow)
}

fn build_commit_certificate(
    state: &ParallelWalCombinerState,
    request: &ParallelWalDurabilityRequest,
) -> Result<
    (
        ParallelWalCommitCertificate,
        Option<ParallelWalFallbackReason>,
    ),
    ParallelWalCombinerError,
> {
    if request.batch_size == 0 {
        return Err(ParallelWalCombinerError::EmptyBatch);
    }
    if usize::try_from(request.batch_size).ok() != Some(request.batch_ids.len()) {
        return Err(ParallelWalCombinerError::BatchIdentityCountMismatch {
            batch_size: request.batch_size,
            identity_count: request.batch_ids.len(),
        });
    }
    let mut batch_ids = HashSet::with_capacity(request.batch_ids.len());
    for batch_id in &request.batch_ids {
        if !batch_ids.insert(*batch_id) {
            return Err(ParallelWalCombinerError::DuplicateBatchIdentity {
                batch_id: *batch_id,
            });
        }
    }
    if request.lane_record_counts.is_empty() {
        return Err(ParallelWalCombinerError::EmptyLaneEvidence);
    }
    if request.lane_record_counts.len() > MAX_PARALLEL_WAL_LANE_COUNT {
        return Err(ParallelWalCombinerError::TooManyLanes {
            lane_count: request.lane_record_counts.len(),
        });
    }
    let certificate_epoch = if request.certificate_epoch == 0 {
        state
            .visibility
            .certificate_epoch
            .checked_add(1)
            .ok_or(ParallelWalCombinerError::PublicationGenerationOverflow)?
    } else {
        request.certificate_epoch
    };
    if certificate_epoch <= state.visibility.certificate_epoch {
        return Err(ParallelWalCombinerError::StaleCertificateEpoch {
            current: state.visibility.certificate_epoch,
            proposed: certificate_epoch,
        });
    }
    let durable_segment_epoch = if request.durable_segment_epoch == 0 {
        certificate_epoch
    } else {
        request.durable_segment_epoch
    };
    if durable_segment_epoch < state.durable_segment_epoch {
        return Err(ParallelWalCombinerError::StaleSegmentEpoch {
            current: state.durable_segment_epoch,
            proposed: durable_segment_epoch,
        });
    }

    let commit_seq_lo = checked_next_commit_seq(state.visibility.visible_commit_seq)?;
    let commit_seq_hi = commit_seq_lo
        .get()
        .checked_add(u64::from(request.batch_size).saturating_sub(1))
        .map(CommitSeq::new)
        .ok_or(ParallelWalCombinerError::CommitSequenceOverflow)?;
    let fallback_reason = request.fallback_reason.or({
        if request.checkpoint_active {
            Some(ParallelWalFallbackReason::CheckpointConflict)
        } else if matches!(request.control_mode, ParallelWalOperatingMode::Conservative) {
            Some(ParallelWalFallbackReason::OperatorForced)
        } else {
            None
        }
    });
    let fallback_active = fallback_reason.is_some();
    let lane_count = u16::try_from(request.lane_record_counts.len()).map_err(|_| {
        ParallelWalCombinerError::TooManyLanes {
            lane_count: request.lane_record_counts.len(),
        }
    })?;
    Ok((
        ParallelWalCommitCertificate {
            format_version: PARALLEL_WAL_COMMIT_CERTIFICATE_VERSION,
            residue: ParallelWalOrderedResidue::CommitCertificateThenPublish,
            certificate_epoch,
            commit_seq_lo,
            commit_seq_hi,
            durable_segment_epoch,
            lane_count,
            lane_record_counts: request.lane_record_counts.clone(),
            db_size_pages: request.db_size_pages,
            page_set_size: request.page_set_size,
            wal_frame_payload_digest: request.wal_frame_payload_digest,
            certificate_crc32c: 0,
            fallback_active,
        },
        fallback_reason,
    ))
}

fn build_conservative_shadow_certificate(
    state: &ParallelWalCombinerState,
    evidence: &ParallelWalConservativeShadowEvidence,
) -> Result<
    (
        ParallelWalCommitCertificate,
        Option<ParallelWalFallbackReason>,
    ),
    ParallelWalCombinerError,
> {
    if evidence.batch_ids.is_empty() {
        return Err(ParallelWalCombinerError::EmptyBatch);
    }
    let mut unique_batch_ids = HashSet::with_capacity(evidence.batch_ids.len());
    for batch_id in &evidence.batch_ids {
        if !unique_batch_ids.insert(*batch_id) {
            return Err(ParallelWalCombinerError::DuplicateBatchIdentity {
                batch_id: *batch_id,
            });
        }
    }
    if evidence.lane_record_counts.is_empty() {
        return Err(ParallelWalCombinerError::EmptyLaneEvidence);
    }
    if evidence.lane_record_counts.len() > MAX_PARALLEL_WAL_LANE_COUNT {
        return Err(ParallelWalCombinerError::TooManyLanes {
            lane_count: evidence.lane_record_counts.len(),
        });
    }

    let raw_frame_count = evidence
        .lane_record_counts
        .iter()
        .try_fold(0_u64, |total, count| total.checked_add(u64::from(*count)))
        .ok_or(ParallelWalCombinerError::ShadowCertificateMismatch)?;
    let wal_interval_len = evidence
        .wal_frame_end
        .checked_sub(evidence.wal_frame_start)
        .and_then(|span| span.checked_add(1))
        .filter(|_| evidence.wal_frame_start > 0)
        .ok_or(ParallelWalCombinerError::ShadowCertificateMismatch)?;
    if raw_frame_count != wal_interval_len || raw_frame_count != u64::from(evidence.page_set_size) {
        return Err(ParallelWalCombinerError::ShadowCertificateMismatch);
    }

    let certificate_epoch = if evidence.certificate_epoch == 0 {
        state
            .visibility
            .certificate_epoch
            .checked_add(1)
            .ok_or(ParallelWalCombinerError::PublicationGenerationOverflow)?
    } else {
        evidence.certificate_epoch
    };
    if certificate_epoch <= state.visibility.certificate_epoch {
        return Err(ParallelWalCombinerError::StaleCertificateEpoch {
            current: state.visibility.certificate_epoch,
            proposed: certificate_epoch,
        });
    }
    let durable_segment_epoch = if evidence.durable_segment_epoch == 0 {
        certificate_epoch
    } else {
        evidence.durable_segment_epoch
    };
    if durable_segment_epoch < state.durable_segment_epoch {
        return Err(ParallelWalCombinerError::StaleSegmentEpoch {
            current: state.durable_segment_epoch,
            proposed: durable_segment_epoch,
        });
    }

    let commit_seq_lo = state
        .visibility
        .visible_commit_seq
        .get()
        .checked_add(1)
        .map(CommitSeq::new)
        .ok_or(ParallelWalCombinerError::CommitSequenceOverflow)?;
    let commit_count = u64::try_from(evidence.batch_ids.len())
        .map_err(|_| ParallelWalCombinerError::CommitSequenceOverflow)?;
    let commit_seq_hi = commit_seq_lo
        .get()
        .checked_add(commit_count.saturating_sub(1))
        .map(CommitSeq::new)
        .ok_or(ParallelWalCombinerError::CommitSequenceOverflow)?;
    let fallback_reason = evidence.fallback_reason.or({
        if evidence.checkpoint_active {
            Some(ParallelWalFallbackReason::CheckpointConflict)
        } else if matches!(
            evidence.control_mode,
            ParallelWalOperatingMode::Conservative
        ) {
            Some(ParallelWalFallbackReason::OperatorForced)
        } else {
            None
        }
    });
    let lane_count = u16::try_from(evidence.lane_record_counts.len()).map_err(|_| {
        ParallelWalCombinerError::TooManyLanes {
            lane_count: evidence.lane_record_counts.len(),
        }
    })?;

    Ok((
        ParallelWalCommitCertificate {
            format_version: PARALLEL_WAL_COMMIT_CERTIFICATE_VERSION,
            residue: ParallelWalOrderedResidue::CommitCertificateThenPublish,
            certificate_epoch,
            commit_seq_lo,
            commit_seq_hi,
            durable_segment_epoch,
            lane_count,
            lane_record_counts: evidence.lane_record_counts.clone(),
            db_size_pages: evidence.db_size_pages,
            page_set_size: evidence.page_set_size,
            wal_frame_payload_digest: evidence.wal_frame_payload_digest,
            certificate_crc32c: 0,
            fallback_active: fallback_reason.is_some(),
        },
        fallback_reason,
    ))
}

#[must_use]
pub const fn parallel_wal_lookup_mode_name(mode: ParallelWalLookupMode) -> &'static str {
    match mode {
        ParallelWalLookupMode::AuthoritativeIndex => "authoritative_index",
        ParallelWalLookupMode::ConservativeIndex => "conservative_index",
    }
}

/// Trace schema shared by lane, combiner, checkpoint, recovery, and
/// control-plane events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelWalTraceRecord {
    pub component: String,
    pub trace_id: u64,
    pub scenario_id: String,
    pub decision_id: Option<u64>,
    pub mode: ParallelWalOperatingMode,
    pub lane_id: Option<usize>,
    pub epoch: Option<u64>,
    pub commit_seq_lo: Option<CommitSeq>,
    pub commit_seq_hi: Option<CommitSeq>,
    pub commit_certificate: Option<u32>,
    pub durability_seq: Option<u64>,
    pub publication_generation: Option<u64>,
    pub ordered_region_ns: Option<u64>,
    pub batch_size: Option<u32>,
    pub lookup_mode: Option<ParallelWalLookupMode>,
    pub shadow_certificate_verdict: ParallelWalShadowVerdict,
    pub compatibility_selector: String,
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
        if let Some(epoch_hex) = name_str.strip_prefix(&prefix)
            && let Ok(epoch) = u64::from_str_radix(epoch_hex, 16)
        {
            segments.push((epoch, entry.path()));
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
#[cfg(not(target_arch = "wasm32"))]
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
    // bd-xv5cm M5: extract the coordinator under the global map lock, then
    // release the lock BEFORE the blocking `stop()` (which joins the epoch-ticker
    // thread via `BlockingTaskHandle::wait()`). Holding
    // `PARALLEL_WAL_COORDINATORS` across that join pins the process-global
    // registry behind a blocking wait, serializing every OTHER path's
    // coordinator lookup/teardown behind an unrelated coordinator's shutdown
    // (and would deadlock outright should the joined work ever need the map).
    // The map removal is O(1); the join must run outside the critical section.
    let Some(coordinators) = PARALLEL_WAL_COORDINATORS.get() else {
        return;
    };
    let coordinator = {
        let mut coordinators = coordinators
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        coordinators.remove(db_path)
    };
    if let Some(coordinator) = coordinator {
        coordinator.stop();
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

    #[test]
    fn checked_atomic_increment_reports_previous_value_and_overflow() {
        let counter = AtomicU64::new(41);
        assert_eq!(
            atomic_checked_increment(&counter, Ordering::AcqRel, Ordering::Acquire),
            Ok(41)
        );
        assert_eq!(counter.load(Ordering::Acquire), 42);

        let exhausted = AtomicU64::new(u64::MAX);
        assert_eq!(
            atomic_checked_increment(&exhausted, Ordering::Relaxed, Ordering::Relaxed),
            Err(u64::MAX)
        );
        assert_eq!(exhausted.load(Ordering::Relaxed), u64::MAX);
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
    fn test_remove_coordinator_stops_running_ticker_bd_xv5cm_m5() {
        // bd-xv5cm M5: `remove_parallel_wal_coordinator` must release the global
        // `PARALLEL_WAL_COORDINATORS` map lock BEFORE the blocking `stop()`, which
        // joins the epoch-ticker thread via `BlockingTaskHandle::wait()`.
        // Previously the join ran while the map lock was held, pinning the
        // process-global registry behind an unrelated coordinator's shutdown
        // (a latency/deadlock hazard). This exercises the real running-ticker
        // removal path — registered in the map + a live ticker — which no other
        // test covers (they remove without a ticker, or stop() without removal):
        // the removal must join/stop the ticker and drop the map entry, no hang.
        let path = PathBuf::from("/tmp/bd_xv5cm_m5_remove_running_ticker.db");
        // Clear any entry a prior run left in the process-global map.
        remove_parallel_wal_coordinator(&path);

        let runtime = test_runtime();
        let cx = test_cx();
        let coordinator = parallel_wal_coordinator_for_path(&path);
        coordinator
            .start_on_runtime(&runtime.handle(), &cx)
            .expect("epoch ticker should start");
        assert!(
            coordinator.is_running(),
            "ticker must be running before removal"
        );

        // Extracts the coordinator under the map lock, then joins the ticker
        // OUTSIDE the lock.
        remove_parallel_wal_coordinator(&path);

        // The removed coordinator's ticker was joined/stopped ...
        assert!(
            !coordinator.is_running(),
            "removal must stop (join) the running ticker"
        );
        // ... and its map entry is gone (a fresh lookup builds a NEW coordinator).
        let fresh = parallel_wal_coordinator_for_path(&path);
        assert!(
            !Arc::ptr_eq(&fresh, &coordinator),
            "remove_parallel_wal_coordinator must drop the map entry"
        );
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
            scenario_id: PARALLEL_WAL_PUBLICATION_SCENARIO_ID.to_owned(),
            decision_id: None,
            mode: ParallelWalOperatingMode::Auto,
            lane_id: Some(0),
            epoch: Some(5),
            commit_seq_lo: None,
            commit_seq_hi: None,
            commit_certificate: None,
            durability_seq: None,
            publication_generation: None,
            ordered_region_ns: None,
            batch_size: None,
            lookup_mode: None,
            shadow_certificate_verdict: ParallelWalShadowVerdict::NotRun,
            compatibility_selector: PARALLEL_WAL_COMPATIBILITY_SELECTOR.to_owned(),
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
            format_version: PARALLEL_WAL_COMMIT_CERTIFICATE_VERSION,
            residue: ParallelWalOrderedResidue::default(),
            certificate_epoch: 10,
            commit_seq_lo: CommitSeq::new(1),
            commit_seq_hi: CommitSeq::new(5),
            durable_segment_epoch: 9,
            lane_count: 4,
            lane_record_counts: vec![2, 3, 0, 1],
            db_size_pages: 100,
            page_set_size: 6,
            wal_frame_payload_digest: [0xA5; 32],
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

    fn frame_payload_digest(frames: &[(u32, u32, &[u8])]) -> [u8; 32] {
        let mut builder = ParallelWalFramePayloadDigestBuilder::new();
        for &(page_number, db_size_if_commit, payload) in frames {
            builder.update(
                PageNumber::new(page_number).expect("test page number should be valid"),
                db_size_if_commit,
                payload,
            );
        }
        builder.finalize()
    }

    fn test_wal_frame_payload_digest() -> [u8; 32] {
        frame_payload_digest(&[
            (2, 0, b"frame-a"),
            (3, 0, b"frame-b"),
            (4, 0, b"frame-c"),
            (5, 0, b"frame-d"),
            (6, 17, b"frame-e"),
        ])
    }

    #[test]
    fn frame_payload_digest_builder_binds_ordered_metadata_length_and_bytes() {
        let baseline = frame_payload_digest(&[(2, 0, b"abc"), (3, 17, b"defg")]);

        let mut expected = blake3::Hasher::new_derive_key(PARALLEL_WAL_FRAME_PAYLOAD_DIGEST_DOMAIN);
        expected.update(&0_u64.to_le_bytes());
        expected.update(&2_u32.to_le_bytes());
        expected.update(&0_u32.to_le_bytes());
        expected.update(&3_u64.to_le_bytes());
        expected.update(b"abc");
        expected.update(&1_u64.to_le_bytes());
        expected.update(&3_u32.to_le_bytes());
        expected.update(&17_u32.to_le_bytes());
        expected.update(&4_u64.to_le_bytes());
        expected.update(b"defg");
        assert_eq!(baseline, *expected.finalize().as_bytes());

        let mut unkeyed = blake3::Hasher::new();
        unkeyed.update(&0_u64.to_le_bytes());
        unkeyed.update(&2_u32.to_le_bytes());
        unkeyed.update(&0_u32.to_le_bytes());
        unkeyed.update(&3_u64.to_le_bytes());
        unkeyed.update(b"abc");
        unkeyed.update(&1_u64.to_le_bytes());
        unkeyed.update(&3_u32.to_le_bytes());
        unkeyed.update(&17_u32.to_le_bytes());
        unkeyed.update(&4_u64.to_le_bytes());
        unkeyed.update(b"defg");
        assert_ne!(
            baseline,
            *unkeyed.finalize().as_bytes(),
            "derive-key domain separation must affect the digest"
        );

        assert_ne!(
            baseline,
            frame_payload_digest(&[(3, 17, b"defg"), (2, 0, b"abc")]),
            "ordered frame position must affect the digest"
        );
        assert_ne!(
            baseline,
            frame_payload_digest(&[(4, 0, b"abc"), (3, 17, b"defg")]),
            "page number must affect the digest"
        );
        assert_ne!(
            baseline,
            frame_payload_digest(&[(2, 0, b"abc"), (3, 18, b"defg")]),
            "commit database size must affect the digest"
        );
        assert_ne!(
            baseline,
            frame_payload_digest(&[(2, 0, b"abc"), (3, 17, b"defh")]),
            "payload bytes must affect the digest"
        );
        assert_ne!(
            baseline,
            frame_payload_digest(&[(2, 0, b"abc"), (3, 17, b"defg\0")]),
            "payload length must affect the digest"
        );
    }

    fn durability_request(mode: ParallelWalOperatingMode) -> ParallelWalDurabilityRequest {
        ParallelWalDurabilityRequest {
            trace_id: 7,
            scenario_id: PARALLEL_WAL_PUBLICATION_SCENARIO_ID.to_owned(),
            certificate_epoch: 0,
            durable_segment_epoch: 0,
            batch_size: 2,
            batch_ids: vec![101, 102],
            lane_record_counts: vec![3, 2],
            db_size_pages: 17,
            page_set_size: 5,
            wal_frame_payload_digest: test_wal_frame_payload_digest(),
            control_mode: mode,
            fallback_reason: None,
            checkpoint_active: false,
        }
    }

    #[test]
    fn durability_combiner_assigns_deterministic_contiguous_certificates() {
        let first_combiner = ParallelWalDurabilityCombiner::default();
        let first = first_combiner
            .certify_and_publish(durability_request(ParallelWalOperatingMode::Auto), |_| {
                Ok(())
            })
            .expect("first certificate should publish");
        assert_eq!(first.certificate.commit_seq_lo, CommitSeq::new(1));
        assert_eq!(first.certificate.commit_seq_hi, CommitSeq::new(2));
        assert_eq!(first.durability_seq, 1);
        assert_eq!(first.publication_generation, 1);
        assert_eq!(first.commit_seq_for_batch(101), Some(CommitSeq::new(1)));
        assert_eq!(first.commit_seq_for_batch(102), Some(CommitSeq::new(2)));
        assert!(first.certificate.checksum_is_valid());

        let mut next_request = durability_request(ParallelWalOperatingMode::Auto);
        next_request.batch_size = 3;
        next_request.batch_ids = vec![201, 202, 203];
        next_request.certificate_epoch = 2;
        next_request.durable_segment_epoch = 2;
        let second = first_combiner
            .certify_and_publish(next_request, |_| Ok(()))
            .expect("second certificate should publish");
        assert_eq!(second.certificate.commit_seq_lo, CommitSeq::new(3));
        assert_eq!(second.certificate.commit_seq_hi, CommitSeq::new(5));

        let reference_combiner = ParallelWalDurabilityCombiner::default();
        let reference = reference_combiner
            .certify_and_publish(durability_request(ParallelWalOperatingMode::Auto), |_| {
                Ok(())
            })
            .expect("reference certificate should publish");
        assert_eq!(first.certificate, reference.certificate);
        assert_eq!(
            first.certificate.canonical_bytes(),
            reference.certificate.canonical_bytes()
        );
    }

    #[test]
    fn commit_certificate_crc_is_sensitive_to_frame_payload_digest() {
        let certificate = ParallelWalDurabilityCombiner::default()
            .certify_and_publish(durability_request(ParallelWalOperatingMode::Auto), |_| {
                Ok(())
            })
            .expect("certificate should publish")
            .certificate;
        let mut altered = certificate.clone();
        altered.wal_frame_payload_digest[0] ^= 1;

        assert_ne!(altered.canonical_bytes(), certificate.canonical_bytes());
        assert_ne!(altered.computed_crc32c(), certificate.certificate_crc32c);
        assert!(!altered.checksum_is_valid());
    }

    #[test]
    fn authorized_tail_seeds_fresh_process_certificate_clocks() {
        let first = ParallelWalDurabilityCombiner::default()
            .certify_and_publish(durability_request(ParallelWalOperatingMode::Auto), |_| {
                Ok(())
            })
            .expect("first process publishes a certificate");
        let fresh_process = ParallelWalDurabilityCombiner::new(ParallelWalVisibilitySnapshot {
            visible_commit_seq: first.certificate.commit_seq_hi,
            db_size_pages: first.certificate.db_size_pages,
            ..ParallelWalVisibilitySnapshot::default()
        });
        fresh_process
            .reconcile_authorized_seed(&first.certificate)
            .expect("authorized tail seeds the fresh process");
        let next = fresh_process
            .certify_and_publish(durability_request(ParallelWalOperatingMode::Auto), |_| {
                Ok(())
            })
            .expect("fresh process assigns the next interval");
        assert_eq!(
            next.certificate.commit_seq_lo,
            CommitSeq::new(first.certificate.commit_seq_hi.get() + 1)
        );
        assert_eq!(
            next.certificate.certificate_epoch,
            first.certificate.certificate_epoch + 1
        );
    }

    #[test]
    fn durable_visibility_floor_prevents_reusing_an_older_commit_interval() {
        let combiner = ParallelWalDurabilityCombiner::default();
        let first = combiner
            .certify_and_publish(durability_request(ParallelWalOperatingMode::Auto), |_| {
                Ok(())
            })
            .expect("first certificate should publish");
        assert_eq!(first.certificate.commit_seq_hi, CommitSeq::new(2));

        let peer = ParallelWalDurabilityCombiner::new(ParallelWalVisibilitySnapshot {
            visible_commit_seq: first.certificate.commit_seq_hi,
            db_size_pages: first.certificate.db_size_pages,
            ..ParallelWalVisibilitySnapshot::default()
        });
        peer.reconcile_authorized_seed(&first.certificate)
            .expect("peer should import the first certificate");
        let mut peer_request = durability_request(ParallelWalOperatingMode::Auto);
        peer_request.batch_size = 1;
        peer_request.batch_ids = vec![201];
        peer_request.lane_record_counts = vec![1];
        let peer_commit = peer
            .certify_and_publish(peer_request, |_| Ok(()))
            .expect("peer should publish the next certificate");
        assert_eq!(peer_commit.certificate.commit_seq_hi, CommitSeq::new(3));

        combiner.reconcile_durable_visibility_floor(CommitSeq::new(3));
        combiner
            .reconcile_authorized_seed(&peer_commit.certificate)
            .expect("an equal durable seed must bind after the pager floor advances");
        combiner.reconcile_durable_visibility_floor(CommitSeq::new(2));
        assert_eq!(
            combiner.visibility_snapshot().visible_commit_seq,
            CommitSeq::new(3),
            "a stale durable observation must not lower the allocator"
        );

        let mut next_request = durability_request(ParallelWalOperatingMode::Auto);
        next_request.batch_ids = vec![301, 302];
        let next = combiner
            .certify_and_publish(next_request, |_| Ok(()))
            .expect("the next certificate must start above the durable floor");
        assert_eq!(next.certificate.commit_seq_lo, CommitSeq::new(4));
        assert_eq!(next.certificate.commit_seq_hi, CommitSeq::new(5));
    }

    #[test]
    fn durable_certificate_record_roundtrips_frame_payload_digest_and_checksum() {
        let receipt = ParallelWalDurabilityCombiner::default()
            .certify_and_publish(durability_request(ParallelWalOperatingMode::Auto), |_| {
                Ok(())
            })
            .expect("certificate should publish");
        let sample_db_file_id: [u8; 16] = [
            0xA1, 0xB2, 0xC3, 0xD4, 0xE5, 0xF6, 0x07, 0x18, 0x29, 0x3A, 0x4B, 0x5C, 0x6D, 0x7E,
            0x8F, 0x90,
        ];
        let record = ParallelWalDurableCertificateRecord::new(
            WalGenerationIdentity {
                checkpoint_seq: 9,
                salts: crate::checksum::WalSalts {
                    salt1: 0x1122_3344,
                    salt2: 0x5566_7788,
                },
            },
            17,
            21,
            sample_db_file_id,
            receipt.certificate,
        )
        .expect("durable record should validate");
        let expected_frame_payload_digest = record.certificate.wal_frame_payload_digest;
        let encoded = record.to_bytes();
        assert_eq!(
            encoded.len(),
            ParallelWalDurableCertificateRecord::MIN_ENCODED_SIZE
                + record.certificate.lane_record_counts.len() * 4,
            "fixed record envelope must include the 32-byte frame payload digest"
        );
        assert_eq!(
            ParallelWalDurableCertificateRecord::MIN_ENCODED_SIZE,
            152,
            "recovery readers must share the version-4 minimum envelope size (136 + 16-byte db_file_id)"
        );
        let footer_offset = encoded.len() - ParallelWalDurableCertificateRecord::LENGTH_FOOTER_SIZE;
        assert_eq!(
            usize::try_from(u32::from_le_bytes(
                encoded[footer_offset..]
                    .try_into()
                    .expect("length footer has fixed width")
            ))
            .expect("record length fits usize"),
            encoded.len(),
            "tail footer must support bounded latest-record lookup"
        );
        let decoded = ParallelWalDurableCertificateRecord::from_bytes(&encoded)
            .expect("durable record should decode");
        assert_eq!(
            decoded.certificate.wal_frame_payload_digest,
            expected_frame_payload_digest
        );
        // bd-85x9y / GH#364: the envelope identity survives the round-trip.
        assert_eq!(decoded.db_file_id, sample_db_file_id);
        assert_eq!(decoded, record);

        let mut damaged = encoded.clone();
        damaged[20] ^= 1;
        assert!(
            ParallelWalDurableCertificateRecord::from_bytes(&damaged)
                .expect_err("envelope checksum drift must fail")
                .contains("envelope checksum mismatch")
        );

        let mut damaged_footer = encoded;
        let last = damaged_footer
            .last_mut()
            .expect("encoded record has a length footer");
        *last ^= 1;
        assert!(
            ParallelWalDurableCertificateRecord::from_bytes(&damaged_footer)
                .expect_err("length footer drift must fail")
                .contains("length footer mismatch")
        );
    }

    #[test]
    fn durable_certificate_legacy_versions_are_exactly_the_shipped_predecessors() {
        // GH#372: v2 (the original envelope) and v3 (identity-less) both
        // reached disk in released builds, so both must classify as legacy —
        // an absent certificate on load, never corruption. 0 and 1 never
        // shipped, the current version is live, and a future version is
        // unknown: none of those is legacy.
        assert_eq!(PARALLEL_WAL_DURABLE_CERTIFICATE_FIRST_RECORD_VERSION, 2);
        assert!(!durable_certificate_record_version_is_legacy(0));
        assert!(!durable_certificate_record_version_is_legacy(1));
        assert!(durable_certificate_record_version_is_legacy(2));
        assert!(durable_certificate_record_version_is_legacy(3));
        assert!(!durable_certificate_record_version_is_legacy(
            PARALLEL_WAL_DURABLE_CERTIFICATE_RECORD_VERSION
        ));
        assert!(!durable_certificate_record_version_is_legacy(
            PARALLEL_WAL_DURABLE_CERTIFICATE_RECORD_VERSION + 1
        ));
        assert!(!durable_certificate_record_version_is_legacy(u16::MAX));
    }

    #[test]
    fn durable_certificate_record_v4_preserves_identity_and_rejects_v3() {
        // bd-85x9y / GH#364: a v4 record carries the 16-byte db_file_id and
        // round-trips it losslessly; a record whose version byte is rolled back
        // to the identity-less v3 envelope decodes to an error (which recovery
        // treats as "certificate absent", never a fatal failure).
        let receipt = ParallelWalDurabilityCombiner::default()
            .certify_and_publish(durability_request(ParallelWalOperatingMode::Auto), |_| {
                Ok(())
            })
            .expect("certificate should publish");
        let identity: [u8; 16] = [
            0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78, 0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2,
            0xE1, 0xF0,
        ];
        let record = ParallelWalDurableCertificateRecord::new(
            WalGenerationIdentity {
                checkpoint_seq: 3,
                salts: crate::checksum::WalSalts {
                    salt1: 0xDEAD_BEEF,
                    salt2: 0xFEED_FACE,
                },
            },
            1,
            5,
            identity,
            receipt.certificate,
        )
        .expect("v4 record validates");

        let encoded = record.to_bytes();
        // v4 marker in the little-endian version slot immediately after magic.
        assert_eq!(
            u16::from_le_bytes([encoded[8], encoded[9]]),
            PARALLEL_WAL_DURABLE_CERTIFICATE_RECORD_VERSION
        );
        assert_eq!(PARALLEL_WAL_DURABLE_CERTIFICATE_RECORD_VERSION, 4);

        let decoded =
            ParallelWalDurableCertificateRecord::from_bytes(&encoded).expect("v4 record decodes");
        assert_eq!(decoded.db_file_id, identity);
        assert_eq!(decoded, record);

        // Roll the version marker back to v3 (identity-less). `from_bytes`
        // rejects the unknown version outright; crucially it returns an Err the
        // reader can classify as an absent certificate rather than panicking.
        let mut v3_bytes = encoded;
        v3_bytes[8..10].copy_from_slice(&3u16.to_le_bytes());
        let err = ParallelWalDurableCertificateRecord::from_bytes(&v3_bytes)
            .expect_err("a v3 (identity-less) record must not decode as v4");
        assert!(
            err.contains("version"),
            "decode error must name the unsupported version, got {err:?}"
        );
    }

    #[test]
    fn durable_certificate_authorization_rejects_wrong_frame_payload_digest() {
        let request = durability_request(ParallelWalOperatingMode::Auto);
        let actual_frame_payload_digest = request.wal_frame_payload_digest;
        let receipt = ParallelWalDurabilityCombiner::default()
            .certify_and_publish(request, |_| Ok(()))
            .expect("certificate should publish");
        let wal_generation = WalGenerationIdentity {
            checkpoint_seq: 9,
            salts: crate::checksum::WalSalts {
                salt1: 0x1122_3344,
                salt2: 0x5566_7788,
            },
        };
        let record = ParallelWalDurableCertificateRecord::new(
            wal_generation,
            1,
            5,
            [0x5A; 16],
            receipt.certificate,
        )
        .expect("durable record should validate");

        assert!(record.authorizes_wal_boundary(wal_generation, 5, 5, actual_frame_payload_digest));
        let mut wrong_frame_payload_digest = actual_frame_payload_digest;
        wrong_frame_payload_digest[0] ^= 1;
        assert!(!record.authorizes_wal_boundary(wal_generation, 5, 5, wrong_frame_payload_digest));
    }

    #[test]
    fn durability_failure_retains_exact_publication_until_not_committed_is_proven() {
        let combiner = ParallelWalDurabilityCombiner::default();
        let error = combiner
            .certify_and_publish(durability_request(ParallelWalOperatingMode::Auto), |_| {
                Err("injected fsync failure".to_owned())
            })
            .expect_err("failed durability callback must reject publication");
        assert!(matches!(
            error,
            ParallelWalCombinerError::DurabilityWriteFailed(_)
        ));
        assert_eq!(
            combiner.visibility_snapshot(),
            ParallelWalVisibilitySnapshot::default()
        );
        let pending = combiner
            .pending_publication()
            .expect("ambiguous I/O failure must retain its exact publication");
        assert_eq!(pending.certificate().commit_seq_lo, CommitSeq::new(1));
        assert!(matches!(
            combiner.try_claim_ordered_residue(),
            Err(ParallelWalCombinerError::OrderedResidueBusy)
        ));

        combiner
            .abort_pending_publication(&pending)
            .expect("recovery proved the failed write did not commit");
        let receipt = combiner
            .certify_and_publish(durability_request(ParallelWalOperatingMode::Auto), |_| {
                Ok(())
            })
            .expect("an explicitly aborted interval should remain available");
        assert_eq!(receipt.certificate.commit_seq_lo, CommitSeq::new(1));
    }

    #[test]
    fn synchronous_combiner_waits_for_the_active_ordered_residue() {
        let combiner = Arc::new(ParallelWalDurabilityCombiner::default());
        let (first_entered_tx, first_entered_rx) = std::sync::mpsc::channel();
        let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();

        let first_combiner = Arc::clone(&combiner);
        let first = std::thread::spawn(move || {
            first_combiner.certify_and_publish(
                durability_request(ParallelWalOperatingMode::Auto),
                |_| {
                    first_entered_tx
                        .send(())
                        .expect("test should observe the active ordered residue");
                    release_first_rx
                        .recv()
                        .expect("test should release the active ordered residue");
                    Ok(())
                },
            )
        });
        first_entered_rx
            .recv()
            .expect("first combiner worker should enter durability");

        let second_combiner = Arc::clone(&combiner);
        let second = std::thread::spawn(move || {
            let mut request = durability_request(ParallelWalOperatingMode::Auto);
            request.batch_ids = vec![201, 202];
            second_combiner.certify_and_publish(request, |_| Ok(()))
        });

        let wait_deadline = Instant::now() + Duration::from_secs(1);
        while combiner
            .ordered_residue_blocking_waiters
            .load(Ordering::Acquire)
            == 0
        {
            assert!(
                Instant::now() < wait_deadline,
                "second combiner worker never attempted the contended residue"
            );
            std::thread::yield_now();
        }
        release_first_tx
            .send(())
            .expect("first combiner worker should still be waiting");
        let first_receipt = first
            .join()
            .expect("first combiner worker should not panic")
            .expect("first combiner worker should publish");
        let second_receipt = second
            .join()
            .expect("second combiner worker should not panic")
            .expect("second combiner worker should wait and then publish");

        assert_eq!(
            first_receipt.certificate.commit_seq_hi.get() + 1,
            second_receipt.certificate.commit_seq_lo.get()
        );
        assert_eq!(
            combiner.visibility_snapshot().visible_commit_seq,
            second_receipt.certificate.commit_seq_hi
        );
    }

    #[test]
    fn cancellation_before_async_durability_skips_callback_and_retry_is_unique() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        let runtime = test_runtime();
        let cancelled_cx = test_cx();
        cancelled_cx.cancel();
        runtime.block_on(async {
            let combiner = ParallelWalDurabilityCombiner::default();
            let durable_side_effects = Arc::new(AtomicUsize::new(0));
            let cancelled_side_effects = Arc::clone(&durable_side_effects);
            let error = combiner
                .certify_and_publish_async(
                    &cancelled_cx,
                    durability_request(ParallelWalOperatingMode::Auto),
                    move |_| {
                        cancelled_side_effects.fetch_add(1, AtomicOrdering::AcqRel);
                        async { Ok(()) }
                    },
                )
                .await
                .expect_err("pre-callback cancellation must abort the interval");
            assert_eq!(error, ParallelWalCombinerError::Cancelled);
            assert_eq!(durable_side_effects.load(AtomicOrdering::Acquire), 0);
            assert_eq!(
                combiner.visibility_snapshot(),
                ParallelWalVisibilitySnapshot::default()
            );

            let retry_cx = test_cx();
            let retry_side_effects = Arc::clone(&durable_side_effects);
            let receipt = combiner
                .certify_and_publish_async(
                    &retry_cx,
                    durability_request(ParallelWalOperatingMode::Auto),
                    move |_| {
                        retry_side_effects.fetch_add(1, AtomicOrdering::AcqRel);
                        async { Ok(()) }
                    },
                )
                .await
                .expect("retry should retain the uncommitted first interval");
            assert_eq!(receipt.certificate.commit_seq_lo, CommitSeq::new(1));
            assert_eq!(receipt.certificate.commit_seq_hi, CommitSeq::new(2));
            assert_eq!(durable_side_effects.load(AtomicOrdering::Acquire), 1);
        });
    }

    #[test]
    fn async_durability_callback_is_awaited_before_publication() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        let runtime = test_runtime();
        let cx = test_cx();
        runtime.block_on(async {
            let combiner = ParallelWalDurabilityCombiner::default();
            let callback_completed = Arc::new(AtomicBool::new(false));
            let callback_completed_for_write = Arc::clone(&callback_completed);
            let receipt = combiner
                .certify_and_publish_async(
                    &cx,
                    durability_request(ParallelWalOperatingMode::Auto),
                    move |_| {
                        Box::pin(async move {
                            asupersync::runtime::yield_now().await;
                            callback_completed_for_write.store(true, AtomicOrdering::Release);
                            Ok(())
                        })
                    },
                )
                .await
                .expect("successful async durability callback should publish");

            assert!(callback_completed.load(AtomicOrdering::Acquire));
            assert_eq!(receipt.certificate.commit_seq_lo, CommitSeq::new(1));
            assert_eq!(
                combiner.visibility_snapshot().visible_commit_seq,
                receipt.certificate.commit_seq_hi
            );
        });
    }

    #[test]
    fn cancellation_during_async_durability_commits_once_without_interval_reuse() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        let runtime = test_runtime();
        let cx = test_cx();
        runtime.block_on(async {
            let combiner = ParallelWalDurabilityCombiner::default();
            let durable_side_effects = Arc::new(AtomicUsize::new(0));
            let callback_side_effects = Arc::clone(&durable_side_effects);
            let callback_cx = cx.clone();
            let first = combiner
                .certify_and_publish_async(
                    &cx,
                    durability_request(ParallelWalOperatingMode::Auto),
                    move |_| async move {
                        callback_side_effects.fetch_add(1, AtomicOrdering::AcqRel);
                        callback_cx.cancel();
                        asupersync::runtime::yield_now().await;
                        callback_cx
                            .checkpoint()
                            .map_err(|error| error.to_string())?;
                        Ok(())
                    },
                )
                .await
                .expect("durability commit section should mask mid-write cancellation");
            assert_eq!(durable_side_effects.load(AtomicOrdering::Acquire), 1);
            assert_eq!(first.certificate.commit_seq_lo, CommitSeq::new(1));
            assert_eq!(
                combiner.visibility_snapshot().visible_commit_seq,
                first.certificate.commit_seq_hi
            );
            assert!(
                cx.checkpoint().is_err(),
                "cancellation must become observable after the commit section"
            );

            let retry_cx = test_cx();
            let retry_side_effects = Arc::clone(&durable_side_effects);
            let mut next_request = durability_request(ParallelWalOperatingMode::Auto);
            next_request.batch_ids = vec![201, 202];
            let next = combiner
                .certify_and_publish_async(&retry_cx, next_request, move |_| async move {
                    retry_side_effects.fetch_add(1, AtomicOrdering::AcqRel);
                    Ok(())
                })
                .await
                .expect("completed interval must advance rather than be retried");
            assert_eq!(
                next.certificate.commit_seq_lo.get(),
                first.certificate.commit_seq_hi.get() + 1
            );
            assert_ne!(
                next.certificate.certificate_crc32c,
                first.certificate.certificate_crc32c
            );
            assert_eq!(durable_side_effects.load(AtomicOrdering::Acquire), 2);
        });
    }

    #[test]
    fn dropped_async_durability_retains_the_ordered_residue() {
        use std::task::Poll;

        let runtime = test_runtime();
        let cx = test_cx();
        runtime.block_on(async {
            let combiner = ParallelWalDurabilityCombiner::default();
            let mut operation = Box::pin(combiner.certify_and_publish_async(
                &cx,
                durability_request(ParallelWalOperatingMode::Auto),
                |_| async {
                    std::future::pending::<()>().await;
                    Ok(())
                },
            ));
            std::future::poll_fn(|task_cx| match operation.as_mut().poll(task_cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(result) => {
                    panic!("durability callback unexpectedly completed: {result:?}")
                }
            })
            .await;
            drop(operation);

            assert!(
                combiner.ordered_residue_claimed.load(Ordering::Acquire),
                "dropping an in-flight durability callback must retain serialization until recovery"
            );
            let pending = combiner
                .pending_publication()
                .expect("dropped durability must retain exact recovery state");
            assert_eq!(
                combiner.visibility_snapshot(),
                ParallelWalVisibilitySnapshot::default()
            );
            let receipt = combiner
                .finalize_pending_publication(&pending)
                .expect("authorized recovery should publish the retained interval");
            assert_eq!(receipt.certificate, *pending.certificate());
            assert!(!combiner.ordered_residue_claimed.load(Ordering::Acquire));
        });
    }

    #[test]
    fn pending_publication_reconciliation_is_exact_and_single_use() {
        let runtime = test_runtime();
        let cx = test_cx();
        runtime.block_on(async {
            let combiner = ParallelWalDurabilityCombiner::default();
            let pending = combiner
                .prepare_pending_publication(
                    &cx,
                    durability_request(ParallelWalOperatingMode::Auto),
                )
                .await
                .expect("publication should prepare");

            let mut wrong_id = pending.clone();
            wrong_id.pending_id = wrong_id.pending_id.saturating_add(1);
            assert!(matches!(
                combiner.finalize_pending_publication(&wrong_id),
                Err(ParallelWalCombinerError::PendingPublicationMismatch { .. })
            ));
            let mut wrong_certificate = pending.clone();
            wrong_certificate.certificate.db_size_pages = wrong_certificate
                .certificate
                .db_size_pages
                .saturating_add(1);
            assert!(matches!(
                combiner.abort_pending_publication(&wrong_certificate),
                Err(ParallelWalCombinerError::PendingPublicationMismatch { .. })
            ));
            assert_eq!(
                combiner.visibility_snapshot(),
                ParallelWalVisibilitySnapshot::default()
            );

            let receipt = combiner
                .finalize_pending_publication(&pending)
                .expect("exact authorized publication should finalize once");
            assert_eq!(receipt.certificate, *pending.certificate());
            assert!(matches!(
                combiner.finalize_pending_publication(&pending),
                Err(ParallelWalCombinerError::PendingPublicationMissing { .. })
            ));

            let visibility_after_commit = combiner.visibility_snapshot();
            let mut next_request = durability_request(ParallelWalOperatingMode::Auto);
            next_request.batch_ids = vec![301, 302];
            let not_committed = combiner
                .prepare_pending_publication(&cx, next_request)
                .await
                .expect("next interval should prepare after exact finalization");
            combiner
                .abort_pending_publication(&not_committed)
                .expect("exact not-committed publication should abort once");
            assert_eq!(combiner.visibility_snapshot(), visibility_after_commit);
            assert!(matches!(
                combiner.abort_pending_publication(&not_committed),
                Err(ParallelWalCombinerError::PendingPublicationMissing { .. })
            ));
        });
    }

    #[test]
    fn pending_publication_rejects_foreign_combiner_finalize_handle() {
        let runtime = test_runtime();
        let cx = test_cx();
        runtime.block_on(async {
            let first = ParallelWalDurabilityCombiner::default();
            let second = ParallelWalDurabilityCombiner::default();
            let request = durability_request(ParallelWalOperatingMode::Auto);
            let first_pending = first
                .prepare_pending_publication(&cx, request.clone())
                .await
                .expect("first publication should prepare");
            let second_pending = second
                .prepare_pending_publication(&cx, request)
                .await
                .expect("second publication should prepare");

            assert_eq!(first_pending.pending_id, second_pending.pending_id);
            assert_eq!(first_pending.certificate, second_pending.certificate);
            assert_ne!(first_pending.combiner_id, second_pending.combiner_id);
            assert_eq!(
                first.finalize_pending_publication(&second_pending),
                Err(ParallelWalCombinerError::PendingPublicationOwnerMismatch {
                    expected_combiner_id: first_pending.combiner_id,
                    actual_combiner_id: second_pending.combiner_id,
                })
            );
            assert_eq!(
                first.pending_publication(),
                Some(first_pending.clone()),
                "foreign finalize must not consume the owner's pending publication"
            );

            first
                .finalize_pending_publication(&first_pending)
                .expect("owner should still finalize its exact publication");
            second
                .abort_pending_publication(&second_pending)
                .expect("second owner should clean up its publication");
        });
    }

    #[test]
    fn pending_publication_rejects_foreign_combiner_abort_handle() {
        let runtime = test_runtime();
        let cx = test_cx();
        runtime.block_on(async {
            let first = ParallelWalDurabilityCombiner::default();
            let second = ParallelWalDurabilityCombiner::default();
            let request = durability_request(ParallelWalOperatingMode::Auto);
            let first_pending = first
                .prepare_pending_publication(&cx, request.clone())
                .await
                .expect("first publication should prepare");
            let second_pending = second
                .prepare_pending_publication(&cx, request)
                .await
                .expect("second publication should prepare");

            assert_eq!(first_pending.pending_id, second_pending.pending_id);
            assert_eq!(first_pending.certificate, second_pending.certificate);
            assert_ne!(first_pending.combiner_id, second_pending.combiner_id);
            assert_eq!(
                second.abort_pending_publication(&first_pending),
                Err(ParallelWalCombinerError::PendingPublicationOwnerMismatch {
                    expected_combiner_id: second_pending.combiner_id,
                    actual_combiner_id: first_pending.combiner_id,
                })
            );
            assert_eq!(
                second.pending_publication(),
                Some(second_pending.clone()),
                "foreign abort must not consume the owner's pending publication"
            );

            first
                .abort_pending_publication(&first_pending)
                .expect("first owner should clean up its publication");
            second
                .finalize_pending_publication(&second_pending)
                .expect("second owner should still finalize its exact publication");
        });
    }

    #[test]
    fn async_conservative_shadow_durability_failure_leaves_state_unchanged() {
        let runtime = test_runtime();
        let cx = test_cx();
        runtime.block_on(async {
            let combiner = ParallelWalDurabilityCombiner::default();
            let request = durability_request(ParallelWalOperatingMode::ShadowCompare);
            let evidence = ParallelWalConservativeShadowEvidence {
                certificate_epoch: request.certificate_epoch,
                durable_segment_epoch: request.durable_segment_epoch,
                batch_ids: request.batch_ids.clone(),
                lane_record_counts: request.lane_record_counts.clone(),
                db_size_pages: request.db_size_pages,
                page_set_size: request.page_set_size,
                wal_frame_payload_digest: request.wal_frame_payload_digest,
                control_mode: request.control_mode,
                fallback_reason: request.fallback_reason,
                checkpoint_active: request.checkpoint_active,
                wal_frame_start: 1,
                wal_frame_end: u64::from(request.page_set_size),
            };
            let visibility_before = combiner.visibility_snapshot();
            let metrics_before = combiner.metrics_snapshot();

            let error = combiner
                .certify_and_publish_with_conservative_shadow_async(
                    &cx,
                    request.clone(),
                    evidence.clone(),
                    |_| {
                        Box::pin(async {
                            asupersync::runtime::yield_now().await;
                            Err("injected async fsync failure".to_owned())
                        })
                    },
                )
                .await
                .expect_err("failed async durability must reject publication");
            assert!(matches!(
                error,
                ParallelWalCombinerError::DurabilityWriteFailed(_)
            ));
            assert_eq!(combiner.visibility_snapshot(), visibility_before);
            assert_eq!(combiner.metrics_snapshot(), metrics_before);

            let pending = combiner
                .pending_publication()
                .expect("async I/O failure must retain exact recovery state");
            combiner
                .abort_pending_publication(&pending)
                .expect("test recovery proves the injected callback wrote nothing");
            let receipt = combiner
                .certify_and_publish_with_conservative_shadow_async(&cx, request, evidence, |_| {
                    Box::pin(async { Ok(()) })
                })
                .await
                .expect("proven-uncommitted interval should remain available for retry");
            assert_eq!(receipt.certificate.commit_seq_lo, CommitSeq::new(1));
            assert_eq!(
                receipt.shadow_certificate_verdict,
                ParallelWalShadowVerdict::Clean
            );
            assert_eq!(combiner.metrics_snapshot().shadow_comparisons, 1);
        });
    }

    #[test]
    fn external_publication_rejects_duplicate_gap_and_checksum_drift() {
        let combiner = ParallelWalDurabilityCombiner::default();
        let receipt = combiner
            .certify_and_publish(durability_request(ParallelWalOperatingMode::Auto), |_| {
                Ok(())
            })
            .expect("certificate should publish");
        assert!(matches!(
            combiner.validate_external_publication(&receipt),
            Err(ParallelWalCombinerError::DuplicateOrStalePublication { .. })
        ));

        let mut checksum_drift = receipt.clone();
        checksum_drift.certificate.certificate_crc32c ^= 1;
        assert_eq!(
            combiner.validate_external_publication(&checksum_drift),
            Err(ParallelWalCombinerError::CertificateChecksumMismatch)
        );

        let mut gap = receipt;
        gap.certificate.certificate_epoch = 2;
        gap.certificate.commit_seq_lo = CommitSeq::new(4);
        gap.certificate.commit_seq_hi = CommitSeq::new(4);
        gap.certificate.certificate_crc32c = gap.certificate.computed_crc32c();
        assert_eq!(
            combiner.validate_external_publication(&gap),
            Err(ParallelWalCombinerError::CertificateGap {
                expected: CommitSeq::new(3),
                actual: CommitSeq::new(4),
            })
        );
    }

    #[test]
    fn conservative_and_checkpoint_routes_publish_bounded_visibility() {
        let conservative = ParallelWalDurabilityCombiner::default()
            .certify_and_publish(
                durability_request(ParallelWalOperatingMode::Conservative),
                |_| Ok(()),
            )
            .expect("conservative certificate should publish");
        assert!(conservative.certificate.fallback_active);
        assert_eq!(
            conservative.fallback_reason,
            Some(ParallelWalFallbackReason::OperatorForced)
        );
        assert_eq!(
            conservative.lookup_mode,
            ParallelWalLookupMode::ConservativeIndex
        );

        let combiner = ParallelWalDurabilityCombiner::default();
        let mut checkpoint_request = durability_request(ParallelWalOperatingMode::Auto);
        checkpoint_request.checkpoint_active = true;
        let checkpoint = combiner
            .certify_and_publish(checkpoint_request, |_| Ok(()))
            .expect("checkpoint overlap should route through safe publication");
        assert_eq!(
            checkpoint.fallback_reason,
            Some(ParallelWalFallbackReason::CheckpointConflict)
        );
        assert_eq!(
            combiner.visibility_snapshot().visible_commit_seq,
            checkpoint.certificate.commit_seq_hi
        );
    }

    #[test]
    fn shadow_validation_is_atomic_on_match_and_mismatch() {
        let combiner = ParallelWalDurabilityCombiner::default();
        let mismatch = combiner
            .certify_and_publish_with_shadow(
                durability_request(ParallelWalOperatingMode::ShadowCompare),
                |_| Ok(()),
                |certificate| {
                    let mut shadow = certificate.clone();
                    shadow.page_set_size = shadow.page_set_size.saturating_add(1);
                    shadow
                },
            )
            .expect_err("shadow mismatch must reject publication");
        assert_eq!(
            mismatch,
            ParallelWalCombinerError::ShadowCertificateMismatch
        );
        assert_eq!(
            combiner.visibility_snapshot(),
            ParallelWalVisibilitySnapshot::default()
        );

        let receipt = combiner
            .certify_and_publish_with_shadow(
                durability_request(ParallelWalOperatingMode::ShadowCompare),
                |_| Ok(()),
                Clone::clone,
            )
            .expect("matching shadow certificate should publish");
        assert_eq!(
            receipt.shadow_certificate_verdict,
            ParallelWalShadowVerdict::Clean
        );

        let production_combiner = ParallelWalDurabilityCombiner::default();
        let production_request = durability_request(ParallelWalOperatingMode::ShadowCompare);
        let production_evidence = ParallelWalConservativeShadowEvidence {
            certificate_epoch: production_request.certificate_epoch,
            durable_segment_epoch: production_request.durable_segment_epoch,
            batch_ids: production_request.batch_ids.clone(),
            lane_record_counts: production_request.lane_record_counts.clone(),
            db_size_pages: production_request.db_size_pages,
            page_set_size: production_request.page_set_size,
            wal_frame_payload_digest: production_request.wal_frame_payload_digest,
            control_mode: production_request.control_mode,
            fallback_reason: production_request.fallback_reason,
            checkpoint_active: production_request.checkpoint_active,
            wal_frame_start: 1,
            wal_frame_end: u64::from(production_request.page_set_size),
        };
        let production_receipt = production_combiner
            .certify_and_publish_with_conservative_shadow(
                production_request,
                production_evidence,
                |_| Ok(()),
            )
            .expect("production shadow mode should compare independent raw evidence");
        assert_eq!(
            production_receipt.shadow_certificate_verdict,
            ParallelWalShadowVerdict::Clean
        );
        assert_eq!(
            production_combiner.metrics_snapshot().shadow_comparisons,
            1,
            "production shadow mode must execute a certificate comparison"
        );
        assert_eq!(
            ParallelWalDurabilityCombiner::default()
                .certify_and_publish(
                    durability_request(ParallelWalOperatingMode::ShadowCompare),
                    |_| Ok(()),
                )
                .expect_err("shadow mode must fail closed without independent evidence"),
            ParallelWalCombinerError::MissingConservativeShadowEvidence
        );
        let metrics = combiner.metrics_snapshot();
        assert_eq!(metrics.shadow_comparisons, 2);
        assert_eq!(metrics.shadow_mismatches, 1);
    }

    #[test]
    fn concurrent_lane_flushes_receive_one_ordered_publication_each() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        let combiner = Arc::new(ParallelWalDurabilityCombiner::default());
        let durable_writes = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for lane_id in 0..8_u32 {
            let combiner = Arc::clone(&combiner);
            let durable_writes = Arc::clone(&durable_writes);
            threads.push(std::thread::spawn(move || {
                let mut request = durability_request(ParallelWalOperatingMode::Auto);
                request.batch_size = 1;
                request.batch_ids = vec![u64::from(lane_id) + 1];
                request.lane_record_counts = vec![lane_id.saturating_add(1)];
                combiner.certify_and_publish(request, |_| {
                    durable_writes.fetch_add(1, AtomicOrdering::Relaxed);
                    Ok(())
                })
            }));
        }
        let mut receipts = threads
            .into_iter()
            .map(|thread| {
                thread
                    .join()
                    .expect("combiner worker should not panic")
                    .expect("combiner worker should publish")
            })
            .collect::<Vec<_>>();
        receipts.sort_unstable_by_key(|receipt| receipt.certificate.commit_seq_lo);
        for (index, receipt) in receipts.iter().enumerate() {
            let expected = CommitSeq::new(u64::try_from(index).unwrap_or(u64::MAX) + 1);
            assert_eq!(receipt.certificate.commit_seq_lo, expected);
            assert_eq!(receipt.certificate.commit_seq_hi, expected);
            assert_eq!(receipt.publication_generation, expected.get());
        }
        assert_eq!(durable_writes.load(AtomicOrdering::Relaxed), 8);
        assert_eq!(
            combiner.visibility_snapshot().visible_commit_seq,
            CommitSeq::new(8)
        );
        let metrics = combiner.metrics_snapshot();
        assert_eq!(metrics.certificates_published, 8);
        assert_eq!(metrics.commits_published, 8);
        assert!(metrics.ordered_region_ns_total >= metrics.ordered_region_ns_max);
    }
}
