// Peer credentials and ancillary fd passing now use the `nix` crate (stable Rust)
// instead of nightly #![feature(peer_credentials_unix_socket)] and
// #![feature(unix_socket_ancillary_data)].

//! MVCC page-level versioning for concurrent writers.
//!
//! This crate is intentionally small in early phases: it defines the core MVCC
//! primitives and the cross-process witness/lock-table coordination types.

pub mod atomic_amortizers;
pub mod begin_concurrent;
pub mod bocpd;
pub mod cache_aligned;
pub mod cell_delta_wal;
pub mod cell_mvcc_boundary;
pub mod cell_routing;
pub mod cell_visibility;
pub mod commit_combiner;
pub mod compat;
pub mod conflict_model;
pub mod conformal_martingale;
pub mod coordinator_ipc;
pub mod core_types;
pub mod deterministic_rebase;
pub mod differential_privacy;
pub mod ebr;
pub mod flat_combining;
pub mod flat_combining_page_locks;
pub mod gc;
pub mod history_compression;
pub mod hot_witness_index;
pub mod htm_fast_path;
pub mod index_regen;
pub mod invariants;
pub mod left_right;
pub mod lifecycle;
pub mod materialize;
pub mod mica_commit_log;
pub mod morsel_parallel_insert;
pub mod mpc_commit_controller;
pub mod observability;
pub mod physical_merge;
pub mod provenance;
pub mod rcu;
pub mod reclamation;
pub mod regime_monitor;
pub mod retry_policy;
pub mod rowid_alloc;
pub mod seqlock;
pub mod shared_lock_table;
pub mod sheaf_conformal;
pub mod shm;
#[cfg(not(target_arch = "wasm32"))]
pub mod silo_epoch;
pub mod sketch_telemetry;
pub mod ssi_abort_policy;
pub mod ssi_eprocess_gate;
pub mod ssi_validation;
pub mod stackelberg_coordinator;
pub mod time_travel;
pub mod two_phase_commit;
pub mod witness_hierarchy;
pub mod witness_objects;
pub mod witness_plane;
pub mod witness_publication;
pub mod witness_refinement;
pub mod write_coordinator;
pub mod writer_routing_telemetry;
pub mod xor_delta;

#[cfg(test)]
mod test_support {
    use std::sync::{Mutex, MutexGuard};

    // `tracing` callsite-interest caching is process-global, even when a test
    // installs a thread-local subscriber with `with_default`. Serializing only
    // the tests that capture tracing output prevents concurrent subscriber
    // registration/drop from making a capture intermittently miss events.
    static TRACING_CAPTURE_LOCK: Mutex<()> = Mutex::new(());

    pub struct TracingCaptureGuard {
        _lock: MutexGuard<'static, ()>,
    }

    pub fn tracing_capture_guard() -> TracingCaptureGuard {
        let lock = TRACING_CAPTURE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        TracingCaptureGuard { _lock: lock }
    }
}

/// Reader-visible MVCC metadata classes that participate in Track E3's
/// publication-plane design.
///
/// The intent is to pin the actual hot metadata surfaces to explicit primitive
/// choices so downstream implementation beads do not reopen the concurrency
/// debate ad hoc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MvccMetadataPublicationClass {
    /// Per-page committed visibility sequence in `core_types.rs::CommitIndex`.
    CommitIndex,
    /// First-touch page ownership directory in `core_types.rs::InProcessPageLockTable`.
    PageOwnershipDirectory,
    /// Globally ordered commit-sequence allocator.
    CommitSequenceCounter,
    /// Durable commit-record history consumed by recovery and diagnostics.
    CommitLog,
    /// Active SSI session set plus committed reader/writer lookup indexes in
    /// `begin_concurrent.rs`.
    ActiveTxnRegistry,
    /// Committed SSI reader/writer conflict ledgers.
    CommittedConflictLedger,
    /// Cross-process `(commit_seq, schema_epoch, ecs_epoch)` publication in
    /// `shm.rs`.
    SharedSnapshotTriple,
    /// Schema invalidation epoch when it is read outside the shared triple.
    SchemaEpoch,
    /// Committed page-version chains and their reader guard discipline.
    CommittedVersionStore,
    /// GC horizon and reader-pin floor used to decide when retired metadata may
    /// be reclaimed.
    ReclamationHorizon,
    /// SSI abort-policy and conflict evidence telemetry.
    SsiDecisionTelemetry,
    /// Proof-carrying witness reservation and committed-publication archive.
    WitnessPublicationArchive,
}

impl MvccMetadataPublicationClass {
    /// All MVCC-owned metadata classes that need an E3.2 primitive decision.
    pub const ALL: [Self; 12] = [
        Self::CommitIndex,
        Self::PageOwnershipDirectory,
        Self::CommitSequenceCounter,
        Self::CommitLog,
        Self::ActiveTxnRegistry,
        Self::CommittedConflictLedger,
        Self::SharedSnapshotTriple,
        Self::SchemaEpoch,
        Self::CommittedVersionStore,
        Self::ReclamationHorizon,
        Self::SsiDecisionTelemetry,
        Self::WitnessPublicationArchive,
    ];
}

/// Design-time publication contract for one MVCC metadata class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MvccMetadataPublicationContract {
    /// Concrete metadata class on the hot path.
    pub class: MvccMetadataPublicationClass,
    /// Current implementation touchpoint.
    pub touchpoint: &'static str,
    /// Primitive in the current code.
    pub current_primitive: &'static str,
    /// Primitive selected by the E3 design contract.
    pub selected_primitive: &'static str,
    /// Why the selected primitive fits FrankenSQLite's MVCC model.
    pub fit_rationale: &'static str,
    /// Required candidate-family map for E3.2.
    pub primitive_family_map: &'static str,
    /// Explicitly rejected options so downstream beads do not reopen them.
    pub rejected_options: &'static str,
    /// Read-side consistency and retry rule.
    pub retry_contract: &'static str,
    /// Reclamation / lifetime rule for superseded metadata.
    pub reclamation_contract: &'static str,
}

/// Concrete MVCC metadata-publication mapping for Track E3.
pub const MVCC_METADATA_PUBLICATION_CONTRACTS: [MvccMetadataPublicationContract; 12] = [
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::CommitIndex,
        touchpoint: "core_types.rs::CommitIndex",
        current_primitive: "direct-indexed AtomicU64 hot array plus sharded cold-page fallback",
        selected_primitive: "keep direct per-page atomics for hot pages; keep sharded fallback as the cold-page shape",
        fit_rationale: "page visibility is a one-word monotone publish; readers can safely observe a stale-old value under their snapshot and do not need a cross-page image",
        primitive_family_map: "RCU: compatible only for a future cold-page hash; seqlock: rejected for cross-page arrays; BRAVO: rejected because this is not a reader lock; Left-Right: compatible only for cold fallback, not hot array; RLU: rejected as whole-map copy indirection; sharded: selected for cold/large-page fallback",
        rejected_options: "whole-array seqlock, BRAVO/RwLock, whole-map RLU copy, whole-map Left-Right on the hot array",
        retry_contract: "readers issue an acquire load for one page and compare against their snapshot; no blocking retry path is permitted",
        reclamation_contract: "none for hot entries; cold fallback entries are overwritten or retained by shard ownership",
    },
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::PageOwnershipDirectory,
        touchpoint: "core_types.rs::InProcessPageLockTable",
        current_primitive: "flat atomic CAS table plus sharded overflow maps and bounded waiter queues",
        selected_primitive: "sharded exact-ownership CAS directory with bounded handoff waiters",
        fit_rationale: "first-touch ownership is linearizable state, not an eventually consistent metadata view; a writer must know whether it owns the page now",
        primitive_family_map: "RCU: rejected because stale ownership loses updates; seqlock: rejected because readers cannot retry a lock claim as a snapshot read; BRAVO: rejected because lock claims are write-heavy; Left-Right: rejected because duplicate ownership maps can disagree; RLU: rejected because transactional object copies are too heavy for CAS claims; sharded: selected around the exact atomic directory",
        rejected_options: "RCU ownership maps, seqlock directories, BRAVO/RwLock wrapping, Left-Right duplicated ownership, RLU copied lock objects",
        retry_contract: "claim retries are bounded CAS/park attempts, not optimistic snapshot retries",
        reclamation_contract: "lock slots are cleared in place; waiter records are scoped to bounded handoff queues",
    },
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::CommitSequenceCounter,
        touchpoint: "invariants.rs::TxnManager plus commit_combiner.rs::CommitSequenceCombiner",
        current_primitive: "AtomicU64 commit clock routed through flat combining",
        selected_primitive: "flat-combined atomic fetch_add with optional future reservation blocks",
        fit_rationale: "commit order is one irreducible global scalar; the optimization axis is reducing cache-line traffic, not changing snapshot semantics",
        primitive_family_map: "RCU: rejected because readers do not bind objects; seqlock: rejected because the value is allocated, not sampled as a snapshot; BRAVO: rejected because there is no read lock; Left-Right: rejected because duplicated counters break total order; RLU: rejected because object transactions are irrelevant; sharded: compatible only as preallocated ranges with global reconciliation",
        rejected_options: "seqlock counter reads, Left-Right duplicate counters, BRAVO locks, RLU counter objects, unconstrained per-shard commit order",
        retry_contract: "writers either receive a unique sequence from the combiner or fall back to the atomic allocator; readers consume published commit indexes instead",
        reclamation_contract: "none; the counter is a monotone scalar",
    },
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::CommitLog,
        touchpoint: "core_types.rs::CommitLog",
        current_primitive: "append-only commit records under current transaction-manager ownership",
        selected_primitive: "append-only immutable segments with atomic tail publication when the log leaves the manager lock",
        fit_rationale: "readers consume prefix-stable history and writers never need to mutate already published commit records",
        primitive_family_map: "RCU: compatible for immutable segment tail swaps; seqlock: rejected for large variable records; BRAVO: rejected because the log is append-mostly, not lock-read-mostly; Left-Right: rejected for whole-log duplication; RLU: rejected for whole-log copy transactions; sharded: compatible by database or epoch segment",
        rejected_options: "whole-log seqlock, whole-log Left-Right, BRAVO/RwLock around append history, RLU whole-log copies",
        retry_contract: "readers bind a published tail/prefix and may resample if they need newer history",
        reclamation_contract: "old immutable segments retire only after recovery, diagnostics, and time-travel readers release their references",
    },
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::SharedSnapshotTriple,
        touchpoint: "shm.rs::SharedMemoryLayout::{publish_snapshot,load_consistent_snapshot}",
        current_primitive: "explicit seqlock triple over commit_seq/schema_epoch/ecs_epoch",
        selected_primitive: "keep seqlock triple",
        fit_rationale: "the payload is exactly three words that must be mutually coherent and whose readers can cheaply retry while a writer publishes",
        primitive_family_map: "RCU: rejected as allocation/reclamation overhead for three words; seqlock: selected; BRAVO: rejected because there is no reader lock to bias; Left-Right: rejected as duplicated tiny payload; RLU: rejected as transactional-copy overkill; sharded: rejected because the triple is singular cross-process state",
        rejected_options: "independent atomics, RCU object swap, BRAVO/RwLock, Left-Right pair/triple copies, RLU copied triples, sharded triples",
        retry_contract: "readers retry while snapshot_seq is odd or changes; no read-side write lock is allowed",
        reclamation_contract: "none; the triple is overwritten in place",
    },
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::ActiveTxnRegistry,
        touchpoint: "begin_concurrent.rs::ConcurrentRegistry active set plus committed reader/writer indexes",
        current_primitive: "global Mutex<HashMap> plus per-handle Mutex leaves",
        selected_primitive: "RCU/QSBR-published registry snapshot with per-handle mutable leaves retained separately",
        fit_rationale: "SSI validation wants one immutable image of the active set while writers continue session lifecycle work outside the reader scan",
        primitive_family_map: "RCU: selected for the bounded active-session image; seqlock: rejected because long SSI scans can livelock under churn; BRAVO: rejected because registry lifecycle operations are writes; Left-Right: compatible but heavier than the existing RCU/QSBR snapshot prototype; RLU: rejected as unsafe-heavy object-copy discipline for session handles; sharded: compatible for writer-side lifecycle after the immutable read image exists",
        rejected_options: "long-held global Mutex during SSI scan, whole-registry seqlock, BRAVO/RwLock, unsafe-heavy RLU handle copies, full Left-Right registry duplication as the first cut",
        retry_contract: "SSI readers bind to one immutable registry image per validation pass instead of holding the writer lock",
        reclamation_contract: "retired registry snapshots wait for a QSBR grace period before recycle",
    },
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::CommittedConflictLedger,
        touchpoint: "begin_concurrent.rs::ConcurrentRegistry committed reader/writer lookup indexes",
        current_primitive: "committed conflict maps updated under the registry Mutex",
        selected_primitive: "append-only sharded conflict ledgers with immutable segment headers and epoch retirement",
        fit_rationale: "committed conflict evidence is lookup-heavy historical metadata; published records are immutable and naturally partition by page or cell key",
        primitive_family_map: "RCU: compatible for published segment heads; seqlock: rejected for variable-size ledgers; BRAVO: rejected because ledger writes occur on commit; Left-Right: rejected for whole-ledger duplication; RLU: rejected as copied indexes would inflate commit cost; sharded: selected by page/cell key family",
        rejected_options: "whole-registry conflict map Mutex as the long-term shape, seqlock vectors, BRAVO/RwLock maps, whole-ledger Left-Right, RLU copied indexes",
        retry_contract: "readers bind a stable segment prefix for validation and may rebind for fresher diagnostic history",
        reclamation_contract: "segments retire after no active snapshot or evidence reader can reference their epoch",
    },
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::SchemaEpoch,
        touchpoint: "lifecycle.rs::schema_epoch plus shm.rs::load_schema_epoch",
        current_primitive: "monotone atomic epoch when standalone; seqlock triple when coupled to durable snapshot state",
        selected_primitive: "keep scalar atomic invalidation epoch; bind through SharedSnapshotTriple when schema must be coherent with commit_seq",
        fit_rationale: "most readers only need stale-schema detection, while durable multi-field readers already have the SHM seqlock triple",
        primitive_family_map: "RCU: rejected for standalone epoch but compatible for future schema-object caches; seqlock: selected only when bundled in the shared triple; BRAVO: rejected as a lock for a scalar token; Left-Right: rejected because schema objects are connection-local here; RLU: rejected as copied schema transactions are out of scope; sharded: rejected because the epoch is global invalidation state",
        rejected_options: "standalone schema-object RCU cache, BRAVO/RwLock token, Left-Right schema duplicate, RLU schema copies, sharded schema epochs",
        retry_contract: "prepared statements compare epochs/cookies and reprepare on mismatch; no global lock is taken",
        reclamation_contract: "none for the scalar; schema objects remain owned by connection-local caches",
    },
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::CommittedVersionStore,
        touchpoint: "invariants.rs::VersionStore plus ebr.rs::VersionGuardRegistry",
        current_primitive: "append-only committed chains guarded by crossbeam epoch pins",
        selected_primitive: "keep append-only EBR publication",
        fit_rationale: "committed page histories need stable chain traversal plus delayed reclamation, not replacement of entire histories on every commit",
        primitive_family_map: "RCU: selected in its epoch/EBR form for node lifetime; seqlock: rejected because chain traversal cannot tolerate torn mutable nodes; BRAVO: rejected because there is no read lock to bias; Left-Right: rejected for whole-chain duplication on commit; RLU: rejected as copied chain objects are too expensive and subtle; sharded: selected for chain heads and allocation locality",
        rejected_options: "seqlock-protected mutable chains, BRAVO/RwLock chains, whole-chain Left-Right copies, RLU copied version nodes, whole-store global Mutex",
        retry_contract: "readers pin the epoch and traverse a committed chain; no seqlock retry path is permitted",
        reclamation_contract: "versions retire only after the min pinned epoch moves past the retire epoch",
    },
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::ReclamationHorizon,
        touchpoint: "core_types.rs::raise_gc_horizon plus ebr.rs::VersionGuardRegistry::min_pinned_epoch",
        current_primitive: "monotone atomics plus active-slot and reader-pin floors",
        selected_primitive: "keep monotone atomic horizon; do not wrap it in RCU or seqlock",
        fit_rationale: "GC consumers require a safe lower bound; stale-low is correct and cheaper than constructing a coherent synthetic snapshot",
        primitive_family_map: "RCU: rejected because the horizon object is not reclaimed; seqlock: rejected because stale-low reads are safe; BRAVO: rejected as no lock is needed; Left-Right: rejected as duplicate floors add drain waits; RLU: rejected as copy transactions add no safety; sharded: compatible only for per-slot floor collection before min-reduction",
        rejected_options: "RCU floor snapshots, seqlock floor bundles, BRAVO/RwLock floor reads, Left-Right floor duplicates, RLU copied floor objects",
        retry_contract: "stale low horizons are safe and may be reread; readers never require a globally locked snapshot",
        reclamation_contract: "the horizon itself is not reclaimed and only gates reclamation of other metadata",
    },
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::SsiDecisionTelemetry,
        touchpoint: "ssi_abort_policy.rs evidence ledger plus observability.rs conflict heat telemetry",
        current_primitive: "bounded async/off-path evidence recording with atomic counters",
        selected_primitive: "keep bounded async ring and sharded counters off the commit-critical path",
        fit_rationale: "telemetry informs operators and adaptive policy; losing or reading stale samples must not affect MVCC correctness",
        primitive_family_map: "RCU: compatible for diagnostic snapshots only; seqlock: rejected on the commit path; BRAVO: rejected because readers are rare; Left-Right: rejected as overbuilt for lossy telemetry; RLU: rejected because copied diagnostics are not worth commit cost; sharded: selected for counters and heat buckets",
        rejected_options: "synchronous global telemetry Mutex, seqlock evidence log on commit, BRAVO/RwLock telemetry map, Left-Right diagnostic copies, RLU evidence objects",
        retry_contract: "policy readers may sample and retry diagnostics, but commit/abort decisions never wait for telemetry publication",
        reclamation_contract: "bounded rings overwrite oldest entries; exported snapshots own copied evidence",
    },
    MvccMetadataPublicationContract {
        class: MvccMetadataPublicationClass::WitnessPublicationArchive,
        touchpoint: "witness_publication.rs::WitnessPublisher",
        current_primitive: "private pending reservations plus committed proof records behind publisher ownership",
        selected_primitive: "two-plane publication: exact pending reservation ownership plus append-only immutable committed chunks",
        fit_rationale: "the reserve/write/commit protocol already separates mutable private state from reader-visible committed proof history",
        primitive_family_map: "RCU: compatible for committed chunk heads; seqlock: rejected for archive-wide variable history; BRAVO: rejected because writes are protocol events, not read-lock traffic; Left-Right: rejected for whole-archive duplication; RLU: rejected as copied archives would inflate proof publication; sharded: compatible by database, reservation, or witness-key family",
        rejected_options: "archive-wide seqlock, BRAVO/RwLock archive, whole-archive Left-Right, RLU archive copies, eventually consistent pending-reservation maps",
        retry_contract: "pending ownership remains exact; readers consume immutable committed chunks and may rebind for newer proof history",
        reclamation_contract: "committed chunks retire only after proof readers and recovery consumers release them",
    },
];

#[cfg(test)]
mod ssi_anomaly_tests;

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod metadata_publication_contract_tests {
    use super::{
        MVCC_METADATA_PUBLICATION_CONTRACTS, MvccMetadataPublicationClass,
        MvccMetadataPublicationContract,
    };

    fn contract(class: MvccMetadataPublicationClass) -> MvccMetadataPublicationContract {
        MVCC_METADATA_PUBLICATION_CONTRACTS
            .iter()
            .find(|contract| contract.class == class)
            .copied()
            .expect("MVCC metadata contract must exist")
    }

    #[test]
    fn test_mvcc_metadata_publication_contract_selects_rcu_for_registry() {
        let registry = contract(MvccMetadataPublicationClass::ActiveTxnRegistry);
        assert_eq!(
            registry.selected_primitive,
            "RCU/QSBR-published registry snapshot with per-handle mutable leaves retained separately"
        );
        assert!(
            registry.reclamation_contract.contains("QSBR grace period"),
            "RCU registry design must carry grace-period reclamation"
        );
    }

    #[test]
    fn test_mvcc_metadata_publication_contract_keeps_seqlock_for_snapshot_triple() {
        let snapshot = contract(MvccMetadataPublicationClass::SharedSnapshotTriple);
        assert_eq!(snapshot.selected_primitive, "keep seqlock triple");
        assert!(
            snapshot.retry_contract.contains("retry"),
            "seqlock snapshot readers must have an explicit retry rule"
        );
    }

    #[test]
    fn test_mvcc_metadata_publication_contract_covers_all_mvcc_classes() {
        assert_eq!(
            MVCC_METADATA_PUBLICATION_CONTRACTS.len(),
            MvccMetadataPublicationClass::ALL.len(),
            "every MVCC metadata class must have one publication contract"
        );

        for class in MvccMetadataPublicationClass::ALL {
            let matching = MVCC_METADATA_PUBLICATION_CONTRACTS
                .iter()
                .filter(|contract| contract.class == class)
                .count();
            assert_eq!(
                matching, 1,
                "MVCC metadata class {class:?} must have exactly one publication contract"
            );
        }
    }

    #[test]
    fn test_mvcc_metadata_publication_contract_maps_required_primitive_families() {
        const REQUIRED_FAMILIES: [&str; 6] =
            ["RCU", "seqlock", "BRAVO", "Left-Right", "RLU", "sharded"];

        for contract in MVCC_METADATA_PUBLICATION_CONTRACTS {
            assert!(
                !contract.fit_rationale.is_empty(),
                "{:?} must explain why the selected primitive fits",
                contract.class
            );
            assert!(
                !contract.rejected_options.is_empty(),
                "{:?} must keep rejected options explicit",
                contract.class
            );
            for family in REQUIRED_FAMILIES {
                assert!(
                    contract.primitive_family_map.contains(family),
                    "{:?} must map candidate family {family}",
                    contract.class
                );
            }
        }
    }
}

pub use begin_concurrent::{
    ConcurrentHandle, ConcurrentPageState, ConcurrentRegistry, ConcurrentSavepoint, FcwResult,
    MAX_CONCURRENT_WRITERS, PreparedConcurrentCommit, SharedConcurrentHandle, SsiResult,
    concurrent_abort, concurrent_clear_page_state, concurrent_commit, concurrent_commit_read_only,
    concurrent_commit_with_ssi, concurrent_free_page, concurrent_has_page_state,
    concurrent_is_metadata_exempt, concurrent_mark_metadata_exempt, concurrent_page_is_freed,
    concurrent_page_is_synthetic_conflict_only, concurrent_page_read_state,
    concurrent_page_read_status, concurrent_page_state, concurrent_prepare_write_page,
    concurrent_read_page, concurrent_record_metadata_read, concurrent_restore_page_state,
    concurrent_rollback_to_savepoint, concurrent_savepoint, concurrent_stage_prepared_write_marker,
    concurrent_stage_prepared_write_page, concurrent_track_write_conflict_page,
    concurrent_write_metadata_page, concurrent_write_page,
    finalize_prepared_concurrent_commit_with_ssi, is_concurrent_mode,
    prepare_concurrent_commit_fcw_only, prepare_concurrent_commit_with_ssi,
    validate_first_committer_wins,
};
pub use bocpd::{BocpdConfig, BocpdMonitor, ConjugateModel, HazardFunction, RegimeStats};
pub use cache_aligned::{
    CACHE_LINE_BYTES, CLAIMING_TIMEOUT_NO_PID_SECS, CLAIMING_TIMEOUT_SECS, CacheAligned, RcriEntry,
    RcriOverflowError, RecentlyCommittedReadersIndex, SLOT_PAYLOAD_MASK, SLOT_TAG_MASK,
    SLOT_TAG_SHIFT, SharedTxnSlot, SlotAcquireError, TAG_CLAIMING, TAG_CLEANING, TxnSlotArray,
    decode_payload, decode_tag, encode_claiming, encode_cleaning, is_sentinel, rcri_bloom,
    slot_mode, slot_state,
};
pub use cell_delta_wal::{
    CELL_DELTA_CHECKSUM_SIZE, CELL_DELTA_FRAME_MARKER, CELL_DELTA_HEADER_SIZE,
    CELL_DELTA_MAX_DATA_LEN, CELL_DELTA_MIN_FRAME_SIZE, CellDeltaOp, CellDeltaRecoverySummary,
    CellDeltaWalFrame, deserialize_cell_delta_batch, serialize_cell_delta_batch,
};
pub use cell_routing::{
    CellMvccMode, EscalationResult, PageTrackingState, RoutingContext, RoutingDecision,
    RoutingReason, TxnEscalationTracker, escalate_to_page_level, get_cell_mvcc_mode,
    set_cell_mvcc_mode, should_use_cell_path,
};
pub use cell_visibility::{
    CellConflict, CellDelta, CellDeltaArena, CellDeltaIdx, CellDeltaKind, CellGcStats, CellKey,
    CellVisibilityLog, MutationOutcome, can_be_logical_insert, will_be_logical_delete,
};
pub use compat::{
    CompatMode, CoordinatorProbeResult, HybridShmState, ReadLockOutcome, RecoveryPlan,
    UpdatedLegacyShm, begin_concurrent_check, choose_reader_slot,
};
pub use conflict_model::{
    AMS_SKETCH_VERSION, AmsEvidenceLedger, AmsSketch, AmsSketchConfig, AmsWindowCollector,
    AmsWindowCollectorConfig, AmsWindowEstimate, DEFAULT_AMS_R, DEFAULT_HEAVY_HITTER_K,
    DEFAULT_NITRO_PRECISION, DEFAULT_ZIPF_MAX_ITERS, HeadTailDecomposition, HeavyHitterLedgerEntry,
    InstrumentationCounters, MAX_AMS_R, MAX_HEAVY_HITTER_K, MAX_NITRO_PRECISION, MIN_AMS_R,
    MIN_HEAVY_HITTER_K, MIN_NITRO_PRECISION, NITRO_SKETCH_VERSION, NitroSketch, NitroSketchConfig,
    SpaceSavingEntry, SpaceSavingSummary, WindowCloseReason, ZIPF_S_MAX, ZIPF_S_MIN, ZipfMleResult,
    ams_sign, birthday_conflict_probability_m2, birthday_conflict_probability_uniform,
    compute_head_tail_decomposition, dedup_write_set, effective_collision_pool,
    effective_w_index_multiplier, effective_w_leaf_split, effective_w_root_split, exact_m2, mix64,
    p_abort_attempt, p_drift, pairwise_conflict_probability, policy_collision_mass_input,
    tps_estimate, validate_ams_r, validate_heavy_hitter_k, validate_nitro_precision,
    zipf_mle_from_ranked_counts,
};
pub use conformal_martingale::{ConformalMartingaleConfig, ConformalMartingaleMonitor};
pub use core_types::{
    CommitIndex, CommitLog, CommitRecord, DrainProgress, DrainResult, GcHorizonResult,
    InProcessPageLockTable, LOCK_TABLE_SHARDS, OrphanedSlotCleanupStats, ReaderPinCommitSeq,
    RebuildError, RebuildResult, SlotCleanupResult, Transaction, TransactionMode, TransactionState,
    VersionArena, VersionIdx, cleanup_and_raise_gc_horizon,
    cleanup_and_raise_gc_horizon_with_reader_clamp, cleanup_orphaned_slots, raise_gc_horizon,
    raise_gc_horizon_with_reader_clamp, try_cleanup_orphaned_slot, try_cleanup_sentinel_slot,
};
pub use deterministic_rebase::{
    BaseRowReader, RebaseEligibility, RebaseError, RebaseResult, RebaseSchemaLookup, ReplayResult,
    TableConstraints, UpdateExpressionCandidate, can_emit_update_expression,
    check_rebase_eligibility, check_schema_epoch, deterministic_rebase, replay_update_expression,
};
pub use differential_privacy::{
    DpEngine, DpError, DpMetrics, DpQueryResult, NoiseMechanism, PrivacyBudget, dp_metrics,
    reset_dp_metrics, sensitivity,
};
pub use ebr::{
    DEFAULT_MAX_PENDING_VERSIONS_PER_PAGE, EbrMetrics, EbrMetricsSnapshot, EbrRetireQueue,
    GLOBAL_EBR_METRICS, ReaderPinSnapshot, StaleReaderConfig, VersionGuard, VersionGuardRegistry,
    VersionGuardTicket,
};
pub use flat_combining::{
    FcHandle, FlatCombiner, FlatCombiningMetrics, MAX_FC_SHARDS, MAX_FC_THREADS, OP_ADD, OP_READ,
    ShardedFcHandle, ShardedFlatCombiner, flat_combining_metrics, reset_flat_combining_metrics,
};
pub use flat_combining_page_locks::{FcPageLockShard, MAX_FC_SLOTS};
pub use gc::{
    GC_F_MAX_HZ, GC_F_MIN_HZ, GC_PAGES_BUDGET, GC_TARGET_CHAIN_LENGTH, GC_VERSIONS_BUDGET,
    GcScheduler, GcTickResult, GcTodo, PruneResult, gc_tick, prune_page_chain,
};
pub use history_compression::{
    CertificateVerificationError, CircuitBreakerEvent, CompressedPageHistory,
    CompressedPageVersion, CompressedVersionData, HistoryCompressionError, MergeCertificate,
    MergeCertificatePostState, MergeKind, VERIFIER_VERSION, are_intent_ops_independent,
    circuit_breaker_check, collapse_join_max_updates, compress_page_history,
    compute_footprint_digest, compute_op_digest, extract_join_max_constant, foata_normal_form,
    generate_merge_certificate, is_join_max_int_update, is_mergeable_intent,
    verify_merge_certificate,
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
    CHAIN_HEAD_EMPTY, CHAIN_HEAD_SHARDS, CasInstallResult, ChainHeadTable, SerializedWriteMutex,
    SnapshotResolveTrace, TxnManager, VersionStore, VersionVisibilityRange, idx_to_version_pointer,
    visible,
};
pub use left_right::{
    LeftRight, LeftRightMetrics, LeftRightPair, LeftRightTriple, leftright_metrics,
    reset_leftright_metrics,
};
pub use lifecycle::{BeginKind, CommitResponse, MvccError, Savepoint, TransactionManager};
pub use materialize::{
    DEFAULT_MATERIALIZATION_THRESHOLD, MaterializationError, MaterializationResult,
    MaterializationTrigger, materialize_page, should_materialize_eagerly,
};
pub use observability::{
    CasMetricsSnapshot, CasRetriesHistogram, ConflictHeatContext, ConflictHeatEdge,
    ConflictHeatObservation, ConflictHeatPageSummary, ConflictHeatSnapshot,
    ConflictOverlapDirection, ConflictOverlapSummary, SharedObserver, SnapshotReadMetricsSnapshot,
    SsiMetricsSnapshot, VersionsTraversedHistogram, cas_metrics_snapshot,
    conflict_heat_telemetry_enabled, conflict_heat_telemetry_snapshot, emit_conflict_resolved,
    emit_fcw_base_drift, emit_page_lock_contention, emit_ssi_abort, mvcc_snapshot_established,
    mvcc_snapshot_metrics_snapshot, mvcc_snapshot_released, record_cas_attempt,
    record_conflict_heat_observation, record_snapshot_read_versions_traversed, record_ssi_abort,
    record_ssi_commit, reset_cas_metrics, reset_conflict_heat_telemetry,
    reset_mvcc_snapshot_metrics, reset_ssi_metrics, set_conflict_heat_telemetry_enabled,
    ssi_metrics_snapshot,
};
pub use physical_merge::{
    CellOp, CellOpKind, FreeSpaceOp, HeaderOp, MergeError, MergeLadderResult, ParsedCell,
    ParsedPage, RangeXorPatch, StructuredPagePatch, apply_patch, diff_parsed_pages,
    evaluate_merge_ladder, merge_structured_patches, parse_btree_page, repack_btree_page,
};
pub use provenance::{
    ProvenanceAnnotation, ProvenanceMetrics, ProvenanceMode, ProvenanceReport, ProvenanceToken,
    ProvenanceTracker, TupleId, WhyNotResult, provenance_metrics, reset_provenance_metrics,
    why_not,
};
pub use rcu::{
    ActiveTxnSnapshotEntry, ActiveTxnSnapshotImage, MAX_ACTIVE_TXN_SNAPSHOT_ENTRIES,
    MAX_RCU_THREADS, QsbrHandle, QsbrRegistry, RcuActiveTxnSnapshotTable, RcuCell, RcuMetrics,
    RcuPair, RcuTriple, rcu_metrics, record_rcu_reclaimed, reset_rcu_metrics,
};
pub use regime_monitor::{RegimeMonitor, RegimeMonitorConfig};
pub use retry_policy::{
    BetaPosterior, ContentionBucketKey, DEFAULT_CANDIDATE_WAITS_MS, DEFAULT_STARVATION_THRESHOLD,
    HazardModelParams, MAX_CONTENTION_BUCKETS, RetryAction, RetryController, RetryCostParams,
    RetryEvidenceEntry, expected_loss_failnow, expected_loss_retry, gittins_index_approx,
    gittins_threshold,
};
pub use rowid_alloc::{
    AllocatorKey, ConcurrentRowIdAllocator, DEFAULT_RANGE_SIZE, LocalRowIdCache, RangeReservation,
    RowIdAllocError, SQLITE_FULL, SQLITE_SCHEMA,
};
pub use seqlock::{
    SeqLock, SeqLockPair, SeqLockTriple, SeqlockMetrics, reset_seqlock_metrics, seqlock_metrics,
};
pub use shared_lock_table::{
    AcquireResult, DEFAULT_TABLE_CAPACITY, DrainStatus, RebuildLeaseError,
    RebuildResult as SharedRebuildResult, SharedPageLockTable,
};
pub use sheaf_conformal::{
    ConformalCalibratorConfig, ConformalOracleCalibrator, ConformalPrediction, InvariantScore,
    OpportunityScore, OracleReport, PredictionSetEntry, Section, SheafObstruction, SheafResult,
    check_sheaf_consistency, check_sheaf_consistency_with_chains,
};
pub use shm::{SharedMemoryLayout, ShmSnapshot};
pub use sketch_telemetry::{
    CMS_VERSION, CountMinSketch, CountMinSketchConfig, DEFAULT_ALLOC_SIZE_BUCKETS,
    DEFAULT_CMS_DEPTH, DEFAULT_CMS_WIDTH, DEFAULT_LATENCY_BUCKETS_US, HISTOGRAM_VERSION,
    HistogramSnapshot, MemoryAllocationTracker, MemoryTrackerSnapshot,
    NITROSKETCH_STREAMING_VERSION, SketchTelemetryMetrics, SlidingWindowCms, SlidingWindowConfig,
    SlidingWindowHistogram, SlidingWindowHistogramSnapshot, StreamingHistogram,
    reset_sketch_telemetry_metrics, sketch_telemetry_metrics,
};
pub use ssi_abort_policy::{
    AbortDecision, AbortDecisionEnvelope, ConformalCalibrator, ConformalConfig, CycleStatus,
    DroHotPathDecision, DroLossMatrix, DroObservedRateKind, DroRadiusCertificate, DroRiskTolerance,
    DroVolatilityTracker, DroVolatilityTrackerConfig, DroVolatilityTrackerError,
    DroWindowObservation, LossMatrix, SsiDecisionCard, SsiDecisionCardDraft, SsiDecisionQuery,
    SsiDecisionType, SsiEvidenceLedger, SsiFpMonitor, SsiFpMonitorConfig, SsiReadSetSummary,
    TxnCost, Victim, VictimDecision, dro_wasserstein_radius, select_victim,
};
pub use ssi_eprocess_gate::{
    GateAlertState, SsiEProcessConfig, SsiEProcessGate, SsiEProcessSnapshot,
};
pub use ssi_validation::{
    ActiveTxnView, CommittedReaderInfo, CommittedWriterInfo, DiscoveredEdge,
    EvidenceRecordMetricsSnapshot, SsiAbortReason, SsiBusySnapshot, SsiEvidenceBudgetConfig,
    SsiEvidenceRecordingMode, SsiState, SsiValidationOk, discover_incoming_edges,
    discover_outgoing_edges, reset_ssi_evidence_metrics, set_ssi_evidence_budget_config,
    set_ssi_evidence_recording_mode, ssi_evidence_budget_config, ssi_evidence_metrics_snapshot,
    ssi_evidence_query, ssi_evidence_recording_mode, ssi_evidence_snapshot,
    ssi_validate_and_publish,
};
pub use time_travel::{
    TimeTravelError, TimeTravelSnapshot, TimeTravelTarget, create_time_travel_snapshot,
    resolve_page_at_commit, resolve_timestamp_via_commit_log, resolve_timestamp_via_markers,
};
pub use two_phase_commit::{
    COMMIT_MARKER_MAGIC, COMMIT_MARKER_MIN_SIZE, DatabaseId, GlobalCommitMarker, MAIN_DB_ID,
    MAX_TOTAL_DATABASES, ParticipantState, PrepareResult, RecoveryAction, SQLITE_MAX_ATTACHED,
    TEMP_DB_ID, TwoPhaseCoordinator, TwoPhaseError, TwoPhaseState,
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
pub use writer_routing_telemetry::{
    WRITER_ROUTING_TELEMETRY_SOURCES, WriterConflictHistoryTelemetry, WriterHomeHint,
    WriterHomeHintDisposition, WriterLockHolderClue, WriterOwnershipLineageTelemetry,
    WriterRetryAttribution, WriterRetryCause, WriterRoutingDecision, WriterRoutingDecisionConfig,
    WriterRoutingDecisionError, WriterRoutingDecisionReason, WriterRoutingHintDegradation,
    WriterRoutingLaneId, WriterRoutingLaneScore, WriterRoutingLaneSnapshot, WriterRoutingMode,
    WriterRoutingNodeId, WriterRoutingPlacementProfile, WriterRoutingSyntheticComparison,
    WriterRoutingSyntheticConfig, WriterRoutingSyntheticFairnessSummary,
    WriterRoutingSyntheticSummary, WriterRoutingSyntheticWorkload,
    WriterRoutingTelemetryCaptureCost, WriterRoutingTelemetryClass, WriterRoutingTelemetryInput,
    WriterRoutingTelemetryPhase, WriterRoutingTelemetryShape, WriterRoutingTelemetrySignal,
    WriterRoutingTelemetrySourceSpec, WriterTierSurfaceCounts, WriterTouchSurfaceTelemetry,
    compare_writer_routing_synthetic_workload, decide_writer_routing_target,
    evaluate_writer_routing_synthetic_workload,
};
pub use xor_delta::{
    DEFAULT_DELTA_THRESHOLD_PCT, DELTA_FIXED_OVERHEAD_BYTES, DELTA_HEADER_BYTES, DELTA_MAGIC,
    DELTA_RUN_HEADER_BYTES, DELTA_SPARSE_OVERHEAD_PCT, DELTA_VERSION, DeltaEncoding, DeltaError,
    DeltaThresholdConfig, SparseXorDeltaObject, count_nonzero_xor, decode_sparse_xor_delta,
    encode_page_delta, encode_sparse_xor_delta, estimate_sparse_delta_size, max_delta_bytes,
    reconstruct_chain_from_newest, use_delta,
};
