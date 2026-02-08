//! WAL checksum primitives and integrity helpers.

pub mod checksum;
pub mod wal_index;

pub use checksum::{
    BTREE_PAGE_TYPE_FLAGS, CRASH_MODEL_SECTOR_SIZES, ChecksumFailureKind, CrashModelContract,
    HashTier, IntegrityCheckIssue, IntegrityCheckLevel, IntegrityCheckReport,
    PAGE_CHECKSUM_RESERVED_BYTES, RecoveryAction, SQLITE_DB_HEADER_RESERVED_OFFSET,
    SQLITE_DB_HEADER_SIZE, SqliteWalChecksum, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
    WalChainInvalidReason, WalChainValidation, WalFecRepairOutcome, WalRecoveryDecision, WalSalts,
    Xxh3Checksum128, attempt_wal_fec_repair, compute_wal_frame_checksum,
    configure_page_checksum_reserved_bytes, content_address_hash_128, crash_model_contract,
    crc32c_checksum, detect_torn_write_in_wal, integrity_check_database_header,
    integrity_check_level1_page, integrity_check_level2_btree,
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
pub use wal_index::{
    WAL_INDEX_HASH_MASK, WAL_INDEX_HASH_MULTIPLIER, WAL_INDEX_HASH_SLOTS,
    WAL_INDEX_PAGE_ARRAY_ENTRIES, WAL_SHM_FIRST_HEADER_BYTES, WAL_SHM_FIRST_HEADER_U32_SLOTS,
    WAL_SHM_FIRST_USABLE_PAGE_ENTRIES, WAL_SHM_HASH_BYTES, WAL_SHM_PAGE_ARRAY_BYTES,
    WAL_SHM_SEGMENT_BYTES, WAL_SHM_SUBSEQUENT_USABLE_PAGE_ENTRIES, WalHashLookup,
    WalIndexHashSegment, WalIndexSegmentKind, decode_native_u32, encode_native_u32,
    simple_modulo_slot, usable_page_entries, wal_index_hash_slot,
};
