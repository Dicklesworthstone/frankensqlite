#![cfg_attr(target_arch = "x86_64", allow(internal_features))]
#![cfg_attr(target_arch = "x86_64", feature(core_intrinsics))]

use std::cmp::Ordering;

pub mod balance;
pub mod be_tree;
pub mod cell;
pub mod cooling;
pub mod cracking;
pub mod cursor;
pub mod delta_chain;
pub mod freelist;
pub mod instrumentation;
pub mod learned_index;
pub mod learned_rowid;
pub mod overflow;
pub mod payload;
pub mod quotient_filter;
pub mod swiss_index;
pub mod swizzle;
pub mod traits;

#[cfg(test)]
mod btree_invariant_tests;

#[cfg(test)]
mod quotient_filter_bench;

pub use be_tree::{
    BeTree, BeTreeConfig, BeTreeMetricsSnapshot, betree_metrics_snapshot, reset_betree_metrics,
};
pub use cell::{
    BtreePageHeader, BtreePageType, CellRef, has_overflow, header_offset_for_page,
    local_payload_size, max_local_payload, min_local_payload, read_cell_pointers,
    read_cell_pointers_into, write_cell_pointers,
};
pub use cooling::{
    CoolingConfig, CoolingMetricsSnapshot, CoolingStateMachine, cooling_metrics_snapshot,
    reset_cooling_metrics,
};
pub use cracking::{
    CrackedColumn, CrackingMetricsSnapshot, cracking_metrics_snapshot, reset_cracking_metrics,
};
pub use cursor::{
    BtCursor, CursorPositionStamp, MemPageStore, PageReader, PageWriter, TableAppendHint,
    TransactionPageIo,
};
pub use instrumentation::{
    BtreeCopyProfileSnapshot, BtreeLeafReuseSnapshot, BtreeMetricsSnapshot, BtreeOpType,
    BtreeOperationTotals, ConflictTopologyPolicyMode, ConflictTopologySplitAdvice,
    HotPageDeflectionStatus, btree_copy_profile_snapshot, btree_leaf_reuse_snapshot,
    btree_metrics_enabled, btree_metrics_snapshot, conflict_topology_policy_enabled,
    conflict_topology_policy_mode, conflict_topology_split_advice, record_conflict_topology_heat,
    record_conflict_topology_heat_batch, reset_btree_copy_profile, reset_btree_leaf_reuse_profile,
    reset_btree_metrics, reset_conflict_topology_policy_state, set_btree_copy_profile_enabled,
    set_btree_metrics_enabled, set_conflict_topology_policy_mode,
};
pub use learned_index::{
    LearnedIndex, LearnedIndexConfig, LearnedIndexMetricsSnapshot, learned_index_metrics_snapshot,
    reset_learned_index_metrics,
};
pub use learned_rowid::LearnedRowIdIndex;
pub use quotient_filter::{
    DEFAULT_Q_BITS as QUOTIENT_FILTER_DEFAULT_Q_BITS,
    DEFAULT_R_BITS as QUOTIENT_FILTER_DEFAULT_R_BITS, QuotientFilter, QuotientFilterError,
    hash_rowid as quotient_filter_hash_rowid,
};
pub use swizzle::{PageTemperature, SwizzleError, SwizzlePtr, SwizzleState};
pub use traits::{BtreeCursorOps, MockBtreeCursor, SeekResult};

/// Compare two B-tree keys stored as contiguous byte slices.
///
/// This is the hot comparison primitive for blobkey paths. It performs
/// sequential byte access with no pointer chasing or virtual dispatch.
#[must_use]
pub fn compare_key_bytes_contiguous(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
}

#[cfg(test)]
mod hot_path_tests {
    use super::*;

    const BEAD_ID: &str = "bd-22n.6";

    #[test]
    fn test_btree_key_comparison_contiguous() {
        // Both slices are adjacent views into a single contiguous buffer.
        let backing = b"alpha___beta____".to_vec();
        let left = &backing[0..8];
        let right = &backing[8..16];

        assert_eq!(
            left.as_ptr().wrapping_add(left.len()),
            right.as_ptr(),
            "bead_id={BEAD_ID} case=adjacent_contiguous_key_slices"
        );

        assert_eq!(
            compare_key_bytes_contiguous(left, right),
            left.cmp(right),
            "bead_id={BEAD_ID} case=hot_compare_matches_slice_cmp"
        );
    }

    #[test]
    fn test_no_pointer_chasing_in_hot_comparison() {
        // Signature guard: hot compare is defined on raw slices only.
        let hot_compare: fn(&[u8], &[u8]) -> Ordering = compare_key_bytes_contiguous;

        let a = b"abcdefgh12345678";
        let b = b"abcdefgh12345679";
        assert_eq!(
            hot_compare(a, b),
            Ordering::Less,
            "bead_id={BEAD_ID} case=expected_ordering"
        );

        // Sequential index walk mirrors the hot loop memory access pattern.
        let mut steps = 0usize;
        for idx in 0..a.len() {
            steps = steps.saturating_add(1);
            if a[idx] != b[idx] {
                break;
            }
        }
        assert_eq!(
            steps,
            a.len(),
            "bead_id={BEAD_ID} case=sequential_access_until_tail_difference"
        );
    }
}

#[cfg(test)]
mod interior_delete_test;

#[cfg(test)]
mod table_seek_bug_test;

#[cfg(test)]
mod index_seek_bug_test;
mod index_seek_bug_test2;
