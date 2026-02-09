#![cfg_attr(target_family = "unix", feature(peer_credentials_unix_socket))]
#![cfg_attr(target_family = "unix", feature(unix_socket_ancillary_data))]

//! MVCC page-level versioning for concurrent writers.
//!
//! This crate is intentionally small in early phases: it defines the core MVCC
//! primitives and the cross-process witness/lock-table coordination types.

pub mod bocpd;
pub mod cache_aligned;
pub mod compat;
pub mod conflict_model;
pub mod coordinator_ipc;
pub mod core_types;
pub mod deterministic_rebase;
pub mod gc;
pub mod hot_witness_index;
pub mod index_regen;
pub mod invariants;
pub mod lifecycle;
pub mod rowid_alloc;
pub mod shared_lock_table;
pub mod sheaf_conformal;
pub mod shm;
pub mod ssi_abort_policy;
pub mod ssi_validation;
pub mod witness_hierarchy;
pub mod witness_objects;
pub mod witness_plane;
pub mod witness_publication;
pub mod witness_refinement;
pub mod write_coordinator;
pub mod xor_delta;

pub use bocpd::{BocpdConfig, BocpdMonitor, ConjugateModel, HazardFunction, RegimeStats};
pub use cache_aligned::{
    CACHE_LINE_BYTES, CLAIMING_TIMEOUT_NO_PID_SECS, CLAIMING_TIMEOUT_SECS, CacheAligned, RcriEntry,
    RcriOverflowError, RecentlyCommittedReadersIndex, SLOT_PAYLOAD_MASK, SLOT_TAG_MASK,
    SLOT_TAG_SHIFT, SharedTxnSlot, SlotAcquireError, TAG_CLAIMING, TAG_CLEANING, TxnSlotArray,
    decode_payload, decode_tag, encode_claiming, encode_cleaning, is_sentinel, rcri_bloom,
    slot_mode, slot_state,
};
pub use compat::{
    CompatMode, CoordinatorProbeResult, HybridShmState, ReadLockOutcome, RecoveryPlan,
    UpdatedLegacyShm, begin_concurrent_check, choose_reader_slot,
};
pub use conflict_model::{
    AmsSketch, AmsSketchConfig, DEFAULT_AMS_R, ams_sign, birthday_conflict_probability_m2,
    birthday_conflict_probability_uniform, effective_collision_pool, exact_m2, mix64,
    pairwise_conflict_probability,
};
pub use core_types::{
    CommitIndex, CommitLog, CommitRecord, DrainProgress, DrainResult, GcHorizonResult,
    InProcessPageLockTable, LOCK_TABLE_SHARDS, OrphanedSlotCleanupStats, RebuildError,
    RebuildResult, SlotCleanupResult, Transaction, TransactionMode, TransactionState, VersionArena,
    VersionIdx, cleanup_and_raise_gc_horizon, cleanup_orphaned_slots, raise_gc_horizon,
    try_cleanup_orphaned_slot, try_cleanup_sentinel_slot,
};
pub use deterministic_rebase::{
    BaseRowReader, RebaseEligibility, RebaseError, RebaseResult, RebaseSchemaLookup, ReplayResult,
    TableConstraints, UpdateExpressionCandidate, can_emit_update_expression,
    check_rebase_eligibility, check_schema_epoch, deterministic_rebase, replay_update_expression,
};
pub use gc::{
    GC_F_MAX_HZ, GC_F_MIN_HZ, GC_PAGES_BUDGET, GC_TARGET_CHAIN_LENGTH, GC_VERSIONS_BUDGET,
    GcScheduler, GcTickResult, GcTodo, PruneResult, gc_tick, prune_page_chain,
};
pub use hot_witness_index::{
    ColdPlaneMode, ColdWitnessStore, HotWitnessBucketEntry, HotWitnessIndex, bitset_to_slot_ids,
};
pub use index_regen::{
    Collation, IndexDef, IndexKeyPart, IndexRegenError, IndexRegenOps, NoOpUniqueChecker,
    UniqueChecker, apply_column_updates, compute_index_key, discard_stale_index_ops,
    eval_rebase_expr, regenerate_index_ops,
};
pub use invariants::{
    SerializedWriteMutex, TxnManager, VersionStore, idx_to_version_pointer, visible,
};
pub use lifecycle::{BeginKind, CommitResponse, MvccError, Savepoint, TransactionManager};
pub use rowid_alloc::{
    AllocatorKey, ConcurrentRowIdAllocator, DEFAULT_RANGE_SIZE, LocalRowIdCache, RangeReservation,
    RowIdAllocError, SQLITE_FULL, SQLITE_SCHEMA,
};
pub use shared_lock_table::{
    AcquireResult, DEFAULT_TABLE_CAPACITY, DrainStatus, RebuildLeaseError,
    RebuildResult as SharedRebuildResult, SharedPageLockTable,
};
pub use sheaf_conformal::{
    ConformalCalibratorConfig, ConformalOracleCalibrator, ConformalPrediction, InvariantScore,
    OpportunityScore, OracleReport, PredictionSetEntry, Section, SheafObstruction, SheafResult,
    check_sheaf_consistency,
};
pub use shm::{SharedMemoryLayout, ShmSnapshot};
pub use ssi_abort_policy::{
    AbortDecision, AbortDecisionEnvelope, ConformalCalibrator, ConformalConfig, CycleStatus,
    LossMatrix, SsiFpMonitor, SsiFpMonitorConfig, TxnCost, Victim, VictimDecision, select_victim,
};
pub use ssi_validation::{
    ActiveTxnView, CommittedReaderInfo, CommittedWriterInfo, DiscoveredEdge, SsiAbortReason,
    SsiBusySnapshot, SsiState, SsiValidationOk, discover_incoming_edges, discover_outgoing_edges,
    ssi_validate_and_publish,
};
pub use witness_hierarchy::{
    HotWitnessIndexDerivationV1, HotWitnessIndexSizingV1, WitnessHierarchyConfigV1,
    WitnessHotIndexManifestV1, WitnessSizingError, derive_range_keys, extract_prefix,
    range_key_bucket_index, witness_key_canonical_bytes, witness_key_hash,
};
pub use witness_objects::{
    AbortPolicy, AbortReason, AbortWitness, ColdPlaneRefinementResult, DependencyEdgeKind,
    EcsCommitProof, EcsDependencyEdge, EcsReadWitness, EcsWriteWitness, EdgeKeyBasis,
    HotPlaneCandidates, KeySummary, KeySummaryChunk, LogicalTime, WitnessDelta, WitnessDeltaKind,
    WitnessParticipation, WriteKind, cold_plane_refine, hot_plane_discover,
};
pub use witness_plane::{WitnessSet, validate_txn_token, witness_keys_overlap};
pub use witness_publication::{
    ActiveSlotSnapshot, CommitMarkerStore, CommittedPublication, DefaultProofValidator,
    GcEligibility, ProofCarryingCommit, ProofCarryingValidator, PublicationError, PublicationPhase,
    ReservationId, ReservationToken, ValidationVerdict, WitnessGcCoordinator, WitnessPublisher,
};
pub use witness_refinement::{
    RefinementBudget, RefinementDecision, RefinementPriority, RefinementResult, VoiMetrics,
    refine_edges,
};
pub use write_coordinator::{
    CommitWriteSet, CompatCommitRequest, CompatCommitResponse, CoordinatorLease, CoordinatorMode,
    DEFAULT_MAX_BATCH_SIZE, DEFAULT_SPILL_THRESHOLD, NativePublishRequest, NativePublishResponse,
    SpillHandle, SpillLoc, SpilledWriteSet, WriteCoordinator,
};
pub use xor_delta::{
    DEFAULT_DELTA_THRESHOLD_PCT, DELTA_FIXED_OVERHEAD_BYTES, DELTA_HEADER_BYTES, DELTA_MAGIC,
    DELTA_RUN_HEADER_BYTES, DELTA_SPARSE_OVERHEAD_PCT, DELTA_VERSION, DeltaEncoding, DeltaError,
    DeltaThresholdConfig, SparseXorDeltaObject, count_nonzero_xor, decode_sparse_xor_delta,
    encode_page_delta, encode_sparse_xor_delta, estimate_sparse_delta_size, max_delta_bytes,
    reconstruct_chain_from_newest, use_delta,
};
