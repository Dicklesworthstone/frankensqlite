#![cfg_attr(target_arch = "x86_64", allow(internal_features))]
#![cfg_attr(target_arch = "x86_64", feature(core_intrinsics))]

pub mod arc_cache;
#[cfg(feature = "encryption")]
pub mod encrypt;
#[cfg(feature = "evalue-eviction")]
pub mod evalue_eviction;
#[cfg(any(test, feature = "fault-injection"))]
pub mod fault_hooks;
pub mod journal;
pub mod page_buf;
pub mod page_cache;
pub mod pager;
pub mod s3_fifo;
pub mod submodular_prefetch;
pub mod thompson_partitioner;
pub mod traits;

pub use arc_cache::{ArcCache, ArcCacheInner, ArcEvictionModel, CacheKey, CacheLookup, CachedPage};
#[cfg(feature = "encryption")]
pub use encrypt::{
    Argon2Params, DATABASE_ID_SIZE, DatabaseId, ENCRYPTION_RESERVED_BYTES, EncryptError, KEY_SIZE,
    KeyManager, NONCE_SIZE, PageEncryptor, TAG_SIZE, validate_reserved_bytes,
};
#[cfg(feature = "evalue-eviction")]
pub use evalue_eviction::{
    DEFAULT_INITIAL_E, DEFAULT_R_HIT, DEFAULT_R_TICK, DEFAULT_TICK_INTERVAL, E_VALUE_CEIL,
    E_VALUE_FLOOR, EValueEvictor,
};
pub use journal::{
    CHECKSUM_STRIDE, JOURNAL_HEADER_SIZE, JOURNAL_MAGIC, JournalError, JournalHeader,
    JournalPageRecord, PENDING_BYTE_OFFSET, checksum_sample_count, journal_checksum,
    lock_byte_page,
};
pub use page_buf::{
    PageBuf, PageBufPool, PageBufPoolMetricsSnapshot, page_buffer_pool_metrics_snapshot,
    reset_page_buffer_pool_metrics,
};
pub use page_cache::{
    ArcEvictionConfig, DEFAULT_PAGE_BUFFER_MAX, PageCache, PageCacheEvictionPolicy,
    PageCacheLightweightSnapshot, PageCacheMetricsSnapshot, PageCachePageSnapshot,
    PageCacheQueueKind, ShardedPageCache, resolve_page_buffer_max,
};
pub use pager::{
    ConnectionPagerOpenMode, PAGER_METADATA_PUBLICATION_CONTRACTS, PagerCommitProfileSnapshot,
    PagerMetadataPublicationClass, PagerMetadataPublicationContract, PagerPublishedSnapshot,
    ParallelWalPublicationIntent, SimplePager, SimplePagerCheckpointWriter, SimpleTransaction,
    WalCommitSyncPolicy, pager_commit_profile_snapshot, remove_group_commit_queue,
    reset_pager_commit_profile, reset_staged_page_overwrite_steals_total,
    set_pager_commit_profile_enabled, staged_page_overwrite_steals_total,
};
pub use s3_fifo::{
    QueueKind, QueueLocation, RolloutDecision, RolloutMetrics, RolloutPolicy, S3Fifo, S3FifoConfig,
    S3FifoEvent, S3FifoRolloutGate,
};
pub use submodular_prefetch::{Candidate as PrefetchCandidate, expected_gain, greedy_select};
pub use thompson_partitioner::{BetaArm, RESAMPLE_INTERVAL, ThompsonPartitioner};
pub use traits::{
    CheckpointMode, CheckpointPageWriter, CheckpointResult, JournalMode, MemoryMockMvccPager,
    MemoryMockTransaction, MockCheckpointPageWriter, MockMvccPager, MockTransaction, MvccPager,
    TransactionHandle, TransactionKind, TransactionMode, WalBackend, WalPublicationSnapshot,
};
