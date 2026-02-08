//! MVCC page-level versioning for concurrent writers.
//!
//! This crate is intentionally small in early phases: it defines the core MVCC
//! primitives and the cross-process witness/lock-table coordination types.

pub mod core_types;
pub mod witness_hierarchy;

pub use core_types::{
    CommitIndex, CommitLog, CommitRecord, InProcessPageLockTable, LOCK_TABLE_SHARDS, PageBuf,
    Transaction, TransactionMode, TransactionState, VersionArena, VersionIdx,
};
pub use witness_hierarchy::{
    HotWitnessIndexDerivationV1, HotWitnessIndexSizingV1, WitnessHierarchyConfigV1,
    WitnessHotIndexManifestV1, WitnessSizingError,
};
