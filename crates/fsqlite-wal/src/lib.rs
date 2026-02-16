//! WAL checksum primitives, integrity helpers, and native commit protocol.

pub mod checkpoint;
pub mod checkpoint_executor;
pub mod checksum;
pub mod metrics;
pub mod native_commit;
#[cfg(test)]
mod per_core_buffer;
pub mod recovery_compaction;
pub mod wal;
pub mod wal_fec;
pub mod wal_index;

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
pub use metrics::{
    GLOBAL_GROUP_COMMIT_METRICS, GLOBAL_WAL_FEC_REPAIR_METRICS, GLOBAL_WAL_METRICS,
    GLOBAL_WAL_RECOVERY_METRICS, GroupCommitMetrics, GroupCommitMetricsSnapshot,
    WalFecRepairCounters, WalFecRepairCountersSnapshot, WalMetrics, WalMetricsSnapshot,
    WalRecoveryCounters, WalRecoveryCountersSnapshot,
};
pub use wal::WalFile;
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
