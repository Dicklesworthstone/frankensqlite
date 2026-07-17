//! WAL checksum primitives, integrity helpers, and native commit protocol.
//!
//! Fault Matrix (`bd-966ja`) for batched append and publish paths:
//!
//! This matrix covers the byte-oriented append helpers in [`wal::WalFile`] and
//! the current durable-publish helper [`group_commit::write_consolidated_frames`].
//! It intentionally stops at WAL durability. Later SHM / snapshot-plane publish
//! remains the caller's responsibility (see `native_commit`).
//!
//! | Stage | Primary APIs | Fault conditions | Surface | State after fault |
//! | --- | --- | --- | --- | --- |
//! | Batch shape + serialization | `prepare_frame_bytes_with_transforms_into` | Frame-count mismatch, page-size mismatch, `frame_count * frame_size` overflow, salt/checksum helper failure while constructing transforms | `WalCorrupt`, `DatabaseFull`, or helper error | WAL file and in-memory WAL counters stay unchanged; caller-owned scratch may contain partial serialized bytes |
//! | Append-window validation | `prepared_append_window_still_current` | File size changed, generation header changed, short header read, header parse failure | `Ok(false)` for stale window; `WalCorrupt` / parse error for malformed WAL | No append occurs; caller must rebuild from the new seed or treat the WAL as corrupt |
//! | Checksum finalization | `finalize_prepared_frame_bytes` | Prepared buffer length mismatch, `frame_count * frame_size` overflow, salt/checksum write failure | `WalCorrupt`, `DatabaseFull`, or checksum helper error | No on-disk mutation; caller buffer may have partially rewritten checksum fields |
//! | Durable byte append | `append_finalized_prepared_frame_bytes` | Frame-count overflow, prepared buffer length mismatch, VFS `write` failure, post-write state advance overflow guard | `DatabaseFull`, `WalCorrupt`, or VFS error | Pre-write validation faults leave WAL state unchanged; a write failure happens before `advance_state_after_write`, so in-memory counters do not advance even though the on-disk tail may need replay/validation |
//! | One-shot prepared append | `append_prepared_frame_bytes` | Any finalization fault plus any finalized-append fault | Propagated error from the lower stage | Same guarantees as the composed lower layers: no publish on finalize failure; write-path faults may leave an untrusted tail that recovery must trim or validate |
//! | Fused batched append | `append_frames` | Test/fault-injection busy hook, page-size mismatch in any frame, batch-size overflow, salt/checksum helper failure, finalized append failure, test-only after-append injected fault | `Busy`, `WalCorrupt`, `DatabaseFull`, helper error, or injected test fault | Assembly faults leave `frame_scratch` restored and counters unchanged; an after-append injected fault is special because bytes/state may already be advanced even though the function returns `Err` |
//! | Durable publish | `write_consolidated_frames`, direct `WalFile::sync` callers | Frame-batch byte-size overflow, any `append_frames` fault, `sync(FULL)` failure after a successful append | `Internal`, append error, or VFS sync error | If `sync` fails, WAL bytes may already be appended and `WalFile` state may already reflect them, but durability/publish is not established, so higher layers must not advertise the batch as committed |

#[cfg(target_arch = "wasm32")]
use std::path::Path;

pub mod cell_delta_commit;
pub mod cell_delta_wal;
pub mod checkpoint;
pub mod checkpoint_executor;
pub mod checksum;
#[cfg(test)]
pub mod commit_path_histograms;
#[cfg(any(test, feature = "fault-injection"))]
pub mod fault_hooks;
pub mod group_commit;
pub mod metrics;
pub mod native_commit;
pub mod parallel_wal;
pub mod per_core_buffer;
pub mod recovery_compaction;
pub mod recovery_fence;
pub mod telemetry;
pub mod wal;
#[cfg(not(target_arch = "wasm32"))]
pub mod wal_fec;
pub mod wal_index;

pub use cell_delta_commit::{
    CellDeltaDescriptor, FullPageFrame, MixedCommitStats, MixedFrameSubmission,
    build_cell_delta_frames, serialize_mixed_frames,
};
pub use cell_delta_wal::{
    CELL_DELTA_CHECKSUM_SIZE, CELL_DELTA_FRAME_MARKER, CELL_DELTA_HEADER_SIZE,
    CELL_DELTA_MAX_DATA_SIZE, CELL_DELTA_MIN_FRAME_SIZE, CellDeltaWalFrame, CellOp,
    WalRecoverySummary, extract_page_number_from_marker, is_cell_delta_frame,
};
pub use checkpoint::{
    CheckpointMode, CheckpointPlan, CheckpointPostAction, CheckpointProgress, CheckpointState,
    plan_checkpoint,
};
pub use checkpoint_executor::{CheckpointExecutionResult, CheckpointTarget, execute_checkpoint};
pub use checksum::{
    BTREE_PAGE_TYPE_FLAGS, CRASH_MODEL_SECTOR_SIZES, ChecksumFailureKind, CrashModelContract,
    HashTier, IntegrityCheckIssue, IntegrityCheckLevel, IntegrityCheckReport,
    PAGE_CHECKSUM_RESERVED_BYTES, RecoveryAction, SQLITE_DB_HEADER_RESERVED_OFFSET,
    SQLITE_DB_HEADER_SIZE, SqliteWalChecksum, WAL_FORMAT_VERSION, WAL_FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE, WAL_MAGIC_BE, WAL_MAGIC_LE, WalChainInvalidReason, WalChainValidation,
    WalFecRepairOutcome, WalFrameHeader, WalHeader, WalRecoveryDecision, WalSalts, Xxh3Checksum128,
    attempt_wal_fec_repair, compute_wal_frame_checksum, configure_page_checksum_reserved_bytes,
    content_address_hash_128, crash_model_contract, crc32c_checksum, detect_torn_write_in_wal,
    integrity_check_database_header, integrity_check_level1_page, integrity_check_level2_btree,
    integrity_check_level3_overflow_chain, integrity_check_level4_cross_reference,
    integrity_check_level5_schema, integrity_check_sqlite_file_level1, integrity_hash_xxh3_128,
    is_valid_btree_page_type, merge_integrity_reports, page_checksum_reserved_bytes,
    read_page_checksum, read_wal_frame_checksum, read_wal_frame_salts, read_wal_header_checksum,
    read_wal_header_salts, recover_wal_frame_checksum_mismatch,
    recovery_action_for_checksum_failure, sqlite_wal_checksum, supports_torn_write_sector_size,
    tier_for_algorithm, validate_wal_chain, validate_wal_header_checksum, verify_page_checksum,
    verify_wal_fec_source_hash, wal_fec_source_hash_xxh3_128, wal_frame_db_size,
    wal_header_checksum, write_page_checksum, write_wal_frame_checksum, write_wal_frame_salts,
    write_wal_header_checksum, write_wal_header_salts, zero_page_checksum_trailer,
};
pub use group_commit::{
    ConsolidationMetrics, ConsolidationMetricsSnapshot, ConsolidationPhase, FrameSubmission,
    GLOBAL_CONSOLIDATION_METRICS, GroupCommitConfig, GroupCommitConsolidator, PhaseHistogram,
    PhasePercentiles, SubmitOutcome, SubmitReceipt, TransactionConflictPageBaseline,
    TransactionConflictSnapshot, TransactionFrameBatch, TransactionFrameBatchContext,
    WakeReasonCounters, WakeReasonSnapshot, commit_phase_timing_enabled,
    commit_phase_timing_forced_enabled, detailed_consolidation_metrics_enabled,
    set_commit_phase_timing_enabled, write_consolidated_frames,
};
pub use metrics::{
    GLOBAL_GROUP_COMMIT_METRICS, GLOBAL_WAL_FEC_REPAIR_METRICS, GLOBAL_WAL_METRICS,
    GLOBAL_WAL_RECOVERY_METRICS, GroupCommitMetrics, GroupCommitMetricsSnapshot,
    WalFecRepairCounters, WalFecRepairCountersSnapshot, WalMetrics, WalMetricsSnapshot,
    WalRecoveryCounters, WalRecoveryCountersSnapshot,
};
pub use parallel_wal::{
    FsyncPolicy, PARALLEL_WAL_COMPATIBILITY_SELECTOR, PARALLEL_WAL_FLUSH_SCENARIO_ID,
    PARALLEL_WAL_LANE_POLICY_VERSION, PARALLEL_WAL_STAGE_SCENARIO_ID, ParallelWalBatch,
    ParallelWalCommitCertificate, ParallelWalConfig, ParallelWalControlSurface,
    ParallelWalCoordinator, ParallelWalDecisionAction, ParallelWalDecisionRecord,
    ParallelWalFallbackReason, ParallelWalFrame, ParallelWalLaneBatch, ParallelWalLaneStager,
    ParallelWalOperatingMode, ParallelWalOrderedResidue, ParallelWalShadowVerdict,
    ParallelWalTraceRecord, SegmentHeader, SegmentRecoveryOptions, SegmentRecoveryResult,
    cleanup_segments, default_parallel_wal_lane_count, delete_segment, list_segments,
    max_durable_epoch, parallel_wal_coordinator_for_path, parallel_wal_fallback_reason_name,
    parallel_wal_mode_name, parallel_wal_shadow_verdict_name, parallel_wal_should_shadow_compare,
    read_segment, recover_and_apply_segments, recover_segments, remove_parallel_wal_coordinator,
    resolve_parallel_wal_control_surface_from_env, segment_path, write_segment,
};
pub use per_core_buffer::{
    AppendOutcome, BufferConfig, BufferState, DEFAULT_BUFFER_SLOT_COUNT, EpochConfig,
    EpochFlushBatch, EpochOrderCoordinator, FallbackDecision, OverflowPolicy, PerCoreWalBuffer,
    PerCoreWalBufferPool, WalRecord, thread_buffer_slot,
};
pub use recovery_fence::{
    CheckpointChecksumVerdict, ExpectedPageChecksum, PidOwnedLockEntry, PidOwnedLockRegistry,
    RECOVERY_FENCE_BACKOFF, RECOVERY_FENCE_MAX_RETRIES, RecoveryFence, RecoveryFenceGuard,
    ensure_db_fsync_before_wal_truncate, execute_recovery_barrier, fsync_db_file_full,
    pid_alive_os, verify_checkpoint_checksum_prefix,
};
pub use telemetry::{
    NoOpWalObserver, WalTelemetryEvent, WalTelemetryObserver, WalTelemetryRingBuffer,
    WalTelemetrySnapshot, wal_telemetry_snapshot,
};
pub use wal::{WalFile, WalGenerationIdentity};
#[cfg(target_arch = "wasm32")]
pub const DEFAULT_RAPTORQ_REPAIR_SYMBOLS: u8 = 2;
#[cfg(target_arch = "wasm32")]
pub const MAX_RAPTORQ_REPAIR_SYMBOLS: u8 = u8::MAX;
#[cfg(not(target_arch = "wasm32"))]
pub use wal_fec::{
    DEFAULT_RAPTORQ_REPAIR_SYMBOLS, MAX_RAPTORQ_REPAIR_SYMBOLS, WAL_FEC_GROUP_META_MAGIC,
    WAL_FEC_GROUP_META_VERSION, WalFecDecodeProof, WalFecGroupId, WalFecGroupMeta,
    WalFecGroupMetaInit, WalFecGroupRecord, WalFecRecoveredGroup, WalFecRecoveryConfig,
    WalFecRecoveryFallbackReason, WalFecRecoveryLog, WalFecRecoveryOutcome, WalFecRepairEvent,
    WalFecRepairEvidenceCard, WalFecRepairEvidenceQuery, WalFecRepairMetricsSnapshot,
    WalFecRepairPipeline, WalFecRepairPipelineConfig, WalFecRepairPipelineStats,
    WalFecRepairSeverityBucket, WalFecRepairSeverityHistogram, WalFecRepairSource,
    WalFecRepairWitnessTriple, WalFecRepairWorkItem, WalFecScanResult, WalFrameCandidate,
    append_wal_fec_group, build_source_page_hashes, ensure_wal_with_fec_sidecar,
    find_wal_fec_group, generate_wal_fec_repair_symbols, identify_damaged_commit_group,
    persist_wal_fec_raptorq_repair_symbols, query_raptorq_repair_evidence,
    raptorq_repair_events_snapshot, raptorq_repair_evidence_snapshot,
    raptorq_repair_metrics_snapshot, read_wal_fec_raptorq_repair_symbols,
    record_raptorq_recovery_log, recover_wal_fec_group_with_config,
    recover_wal_fec_group_with_decoder, recovery_log_from_outcome, reset_raptorq_repair_telemetry,
    scan_wal_fec, wal_fec_path_for_wal,
};
pub use wal_index::{
    WAL_CKPT_INFO_BYTES, WAL_CKPT_LOCK, WAL_INDEX_HASH_MASK, WAL_INDEX_HASH_MULTIPLIER,
    WAL_INDEX_HASH_SLOTS, WAL_INDEX_HDR_BYTES, WAL_INDEX_PAGE_ARRAY_ENTRIES, WAL_INDEX_VERSION,
    WAL_LOCK_SLOT_COUNT, WAL_READ_LOCK_BASE, WAL_READ_MARK_COUNT, WAL_RECOVER_LOCK,
    WAL_SHM_FIRST_HEADER_BYTES, WAL_SHM_FIRST_HEADER_U32_SLOTS, WAL_SHM_FIRST_USABLE_PAGE_ENTRIES,
    WAL_SHM_HASH_BYTES, WAL_SHM_PAGE_ARRAY_BYTES, WAL_SHM_SEGMENT_BYTES,
    WAL_SHM_SUBSEQUENT_USABLE_PAGE_ENTRIES, WAL_WRITE_LOCK, WalCkptInfo, WalHashLookup,
    WalIndexHashSegment, WalIndexHdr, WalIndexSegmentKind, decode_native_u32, encode_native_u32,
    parse_shm_header, simple_modulo_slot, usable_page_entries, wal_index_hash_slot,
    wal_index_hdr_copies_match, write_shm_header,
};

#[cfg(target_arch = "wasm32")]
pub fn read_wal_fec_raptorq_repair_symbols(_sidecar_path: &Path) -> fsqlite_error::Result<u8> {
    Ok(DEFAULT_RAPTORQ_REPAIR_SYMBOLS)
}

#[cfg(target_arch = "wasm32")]
pub fn persist_wal_fec_raptorq_repair_symbols(
    _sidecar_path: &Path,
    _value: u8,
) -> fsqlite_error::Result<()> {
    Ok(())
}
