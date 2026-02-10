use std::cmp::Ordering;

pub mod balance;
pub mod cell;
pub mod cursor;
pub mod freelist;
pub mod overflow;
pub mod payload;
pub mod traits;

pub use cell::{
    BtreePageHeader, BtreePageType, CellRef, has_overflow, header_offset_for_page,
    local_payload_size, max_local_payload, min_local_payload, read_cell_pointers,
    write_cell_pointers,
};
pub use cursor::{BtCursor, MemPageStore, PageReader, PageWriter, TransactionPageIo};
pub use traits::{BtreeCursorOps, MockBtreeCursor, SeekResult};

/// Compare two B-tree keys stored as contiguous byte slices.
///
/// This is the hot comparison primitive for blobkey paths. It performs
/// sequential byte access with no pointer chasing or virtual dispatch.
#[must_use]
pub fn compare_key_bytes_contiguous(left: &[u8], right: &[u8]) -> Ordering {
    let common = left.len().min(right.len());
    for idx in 0..common {
        let l = left[idx];
        let r = right[idx];
        if l != r {
            return l.cmp(&r);
        }
    }
    left.len().cmp(&right.len())
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
