//! §3.1 RaptorQ Source Block Partitioning for Large Databases (bd-1hi.6).
//!
//! Databases larger than a single RaptorQ source block (K_max = 56,403
//! source symbols) must be partitioned into multiple contiguous blocks.
//! This module implements the RFC 6330 §4.4.1 partitioning algorithm
//! adapted for page-level encoding.

use fsqlite_error::{FrankenError, Result};
use tracing::{debug, info};

const BEAD_ID: &str = "bd-1hi.6";

/// RFC 6330 maximum source symbols per source block.
pub const K_MAX: u32 = 56_403;

/// RFC 6330 bounds Source Block Number to 8 bits.
pub const SBN_MAX: u8 = 255;

/// Maximum total pages that can be partitioned (K_MAX * 256).
pub const MAX_PARTITIONABLE_PAGES: u64 = K_MAX as u64 * 256;

/// A contiguous range of database pages forming one RaptorQ source block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceBlock {
    /// 0-based block index (Source Block Number). Fits in `u8` (max 255).
    pub index: u8,
    /// 1-based first page number in this block.
    pub start_page: u32,
    /// Number of source symbols (pages) in this block. Always <= `K_MAX`.
    pub num_pages: u32,
}

/// Partition `total_pages` database pages into source blocks per RFC 6330 §4.4.1.
///
/// Pages are 1-based (page 1 is always in the first source block).
///
/// # Errors
///
/// Returns `FrankenError::OutOfRange` if `total_pages` exceeds `MAX_PARTITIONABLE_PAGES`
/// (i.e. would require more than 256 source blocks).
pub fn partition_source_blocks(total_pages: u32) -> Result<Vec<SourceBlock>> {
    if total_pages == 0 {
        debug!(bead_id = BEAD_ID, "empty database, no source blocks");
        return Ok(Vec::new());
    }

    let p = u64::from(total_pages);

    if p > MAX_PARTITIONABLE_PAGES {
        return Err(FrankenError::OutOfRange {
            what: "total_pages".to_owned(),
            value: total_pages.to_string(),
        });
    }

    if total_pages <= K_MAX {
        info!(
            bead_id = BEAD_ID,
            total_pages, "single source block covers entire database"
        );
        return Ok(vec![SourceBlock {
            index: 0,
            start_page: 1,
            num_pages: total_pages,
        }]);
    }

    // Multiple source blocks needed.
    // Partition P pages into Z blocks as evenly as possible.
    // RFC 6330 §4.4.1: Z_L blocks of K_L symbols, Z_S blocks of K_S symbols.
    let z = total_pages.div_ceil(K_MAX);
    let k_l = total_pages.div_ceil(z);
    let k_s = total_pages / z;
    let z_l = total_pages - k_s * z;
    let z_s = z - z_l;

    info!(
        bead_id = BEAD_ID,
        total_pages, z, k_l, k_s, z_l, z_s, "partitioned database into multiple source blocks"
    );

    let mut blocks = Vec::with_capacity(usize::try_from(z).unwrap_or(256));
    let mut offset: u32 = 1; // 1-based page numbers

    for i in 0..z_l {
        let idx = u8::try_from(i).expect("SBN checked by MAX_PARTITIONABLE_PAGES guard");
        blocks.push(SourceBlock {
            index: idx,
            start_page: offset,
            num_pages: k_l,
        });
        offset = offset
            .checked_add(k_l)
            .expect("offset overflow checked by MAX_PARTITIONABLE_PAGES guard");
    }

    for i in 0..z_s {
        let idx = u8::try_from(z_l + i).expect("SBN checked by MAX_PARTITIONABLE_PAGES guard");
        blocks.push(SourceBlock {
            index: idx,
            start_page: offset,
            num_pages: k_s,
        });
        offset = offset
            .checked_add(k_s)
            .expect("offset overflow checked by MAX_PARTITIONABLE_PAGES guard");
    }

    debug_assert_eq!(
        offset,
        total_pages + 1,
        "bead_id={BEAD_ID} partition coverage mismatch"
    );

    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_BEAD_ID: &str = "bd-1hi.6";

    // -----------------------------------------------------------------------
    // Unit tests from spec comment: §3.1 Source Block Partitioning
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_empty_db() {
        let blocks = partition_source_blocks(0).expect("empty db should succeed");
        assert!(
            blocks.is_empty(),
            "bead_id={TEST_BEAD_ID} case=empty_db_no_blocks"
        );
    }

    #[test]
    fn test_partition_single_page() {
        let blocks = partition_source_blocks(1).expect("single page should succeed");
        assert_eq!(
            blocks.len(),
            1,
            "bead_id={TEST_BEAD_ID} case=single_page_one_block"
        );
        assert_eq!(blocks[0].index, 0);
        assert_eq!(blocks[0].start_page, 1);
        assert_eq!(blocks[0].num_pages, 1);
    }

    #[test]
    fn test_partition_small_db() {
        // test_partition_small_db: 64-page DB → 1 source block. K=64.
        let blocks = partition_source_blocks(64).expect("64 pages should succeed");
        assert_eq!(
            blocks.len(),
            1,
            "bead_id={TEST_BEAD_ID} case=small_db_single_block"
        );
        assert_eq!(blocks[0].index, 0);
        assert_eq!(blocks[0].start_page, 1);
        assert_eq!(blocks[0].num_pages, 64);
    }

    #[test]
    fn test_partition_small_db_100() {
        // From bead spec: Single block: P=100
        let blocks = partition_source_blocks(100).expect("100 pages should succeed");
        assert_eq!(
            blocks.len(),
            1,
            "bead_id={TEST_BEAD_ID} case=single_block_p100"
        );
        assert_eq!(blocks[0].start_page, 1);
        assert_eq!(blocks[0].num_pages, 100);
    }

    #[test]
    fn test_partition_boundary_exactly_k_max() {
        // P=56403 (exactly K_max), verify single block
        let blocks = partition_source_blocks(K_MAX).expect("exactly K_MAX should succeed");
        assert_eq!(
            blocks.len(),
            1,
            "bead_id={TEST_BEAD_ID} case=boundary_exactly_k_max"
        );
        assert_eq!(blocks[0].index, 0);
        assert_eq!(blocks[0].start_page, 1);
        assert_eq!(blocks[0].num_pages, K_MAX);
    }

    #[test]
    fn test_partition_two_blocks() {
        // P=56404 (just over K_max), verify 2 blocks with correct sizes
        let p = K_MAX + 1;
        let blocks = partition_source_blocks(p).expect("K_MAX+1 should succeed");
        assert_eq!(blocks.len(), 2, "bead_id={TEST_BEAD_ID} case=two_blocks");

        // Z = ceil(56404/56403) = 2
        // K_L = ceil(56404/2) = 28202
        // K_S = floor(56404/2) = 28202
        // Z_L = 56404 - 28202*2 = 0
        // Z_S = 2 - 0 = 2
        // Both blocks have 28202 pages
        assert_eq!(blocks[0].index, 0);
        assert_eq!(blocks[0].start_page, 1);
        assert_eq!(blocks[1].index, 1);

        // Total coverage
        let total: u32 = blocks.iter().map(|b| b.num_pages).sum();
        assert_eq!(total, p, "bead_id={TEST_BEAD_ID} case=two_blocks_coverage");

        // Both blocks within K_MAX
        for block in &blocks {
            assert!(
                block.num_pages <= K_MAX,
                "bead_id={TEST_BEAD_ID} case=two_blocks_k_max block={}",
                block.index
            );
        }

        // Contiguous
        assert_eq!(
            blocks[1].start_page,
            blocks[0].start_page + blocks[0].num_pages
        );
    }

    #[test]
    fn test_partition_large_db_worked_example() {
        // From spec: 1GB database with 4096-byte pages = 262,144 pages
        let p = 262_144_u32;
        let blocks = partition_source_blocks(p).expect("large db should succeed");

        // Z = ceil(262144/56403) = 5 source blocks
        assert_eq!(
            blocks.len(),
            5,
            "bead_id={TEST_BEAD_ID} case=large_db_5_blocks"
        );

        // K_L = ceil(262144/5) = 52429
        // K_S = floor(262144/5) = 52428
        // Z_L = 262144 - 52428*5 = 262144 - 262140 = 4 blocks of 52429 pages
        // Z_S = 5 - 4 = 1 block of 52428 pages
        let expected_k_l = 52_429_u32;
        let expected_k_s = 52_428_u32;

        for block in &blocks[..4] {
            assert_eq!(
                block.num_pages, expected_k_l,
                "bead_id={TEST_BEAD_ID} case=large_db_k_l_block block={}",
                block.index
            );
        }
        assert_eq!(
            blocks[4].num_pages, expected_k_s,
            "bead_id={TEST_BEAD_ID} case=large_db_k_s_block"
        );

        // Verify exact block boundaries from spec
        assert_eq!(blocks[0].start_page, 1);
        assert_eq!(blocks[0].start_page + blocks[0].num_pages - 1, 52_429);
        assert_eq!(blocks[1].start_page, 52_430);
        assert_eq!(blocks[1].start_page + blocks[1].num_pages - 1, 104_858);
        assert_eq!(blocks[2].start_page, 104_859);
        assert_eq!(blocks[2].start_page + blocks[2].num_pages - 1, 157_287);
        assert_eq!(blocks[3].start_page, 157_288);
        assert_eq!(blocks[3].start_page + blocks[3].num_pages - 1, 209_716);
        assert_eq!(blocks[4].start_page, 209_717);
        assert_eq!(blocks[4].start_page + blocks[4].num_pages - 1, 262_144);

        // Total coverage
        let total: u32 = blocks.iter().map(|b| b.num_pages).sum();
        assert_eq!(
            total, p,
            "bead_id={TEST_BEAD_ID} case=large_db_total_coverage"
        );

        // Sequential indices
        for (i, block) in blocks.iter().enumerate() {
            assert_eq!(
                block.index,
                u8::try_from(i).unwrap(),
                "bead_id={TEST_BEAD_ID} case=large_db_sequential_indices"
            );
        }
    }

    #[test]
    fn test_partition_large_db_10000() {
        // test_partition_large_db: 10,000-page DB → multiple source blocks
        let blocks = partition_source_blocks(10_000).expect("10k pages should succeed");
        // 10,000 < K_MAX (56,403), so this is a single block
        assert_eq!(
            blocks.len(),
            1,
            "bead_id={TEST_BEAD_ID} case=large_db_10k_single_block"
        );
        assert_eq!(blocks[0].num_pages, 10_000);
    }

    #[test]
    fn test_partition_uneven() {
        // test_partition_uneven: DB size not evenly divisible
        let p = K_MAX * 3 + 7; // 169,216 pages
        let blocks = partition_source_blocks(p).expect("uneven split should succeed");

        // Z = ceil(169216/56403) = 4 (not 3, since 169216 > 56403*3 = 169209)
        // Actually: 56403 * 3 = 169209, and 169216 > 169209, so Z = ceil(169216/56403) = 4

        // Total coverage
        let total: u32 = blocks.iter().map(|b| b.num_pages).sum();
        assert_eq!(
            total, p,
            "bead_id={TEST_BEAD_ID} case=uneven_total_coverage"
        );

        // All blocks within K_MAX
        for block in &blocks {
            assert!(
                block.num_pages <= K_MAX,
                "bead_id={TEST_BEAD_ID} case=uneven_k_max block={}",
                block.index
            );
        }

        // Sizes differ by at most 1
        let max_k = blocks.iter().map(|b| b.num_pages).max().unwrap();
        let min_k = blocks.iter().map(|b| b.num_pages).min().unwrap();
        assert!(
            max_k - min_k <= 1,
            "bead_id={TEST_BEAD_ID} case=uneven_balanced max_k={max_k} min_k={min_k}"
        );

        // Contiguous
        for window in blocks.windows(2) {
            assert_eq!(
                window[1].start_page,
                window[0].start_page + window[0].num_pages,
                "bead_id={TEST_BEAD_ID} case=uneven_contiguous"
            );
        }
    }

    #[test]
    fn test_partition_page1_special() {
        // Page 1 (header) always in first source block
        for p in [1_u32, 64, K_MAX, K_MAX + 1, 262_144] {
            let blocks = partition_source_blocks(p).expect("partition should succeed");
            assert!(
                !blocks.is_empty(),
                "bead_id={TEST_BEAD_ID} case=page1_nonempty p={p}"
            );
            assert_eq!(
                blocks[0].start_page, 1,
                "bead_id={TEST_BEAD_ID} case=page1_in_first_block p={p}"
            );
        }
    }

    #[test]
    fn test_partition_maximum_blocks() {
        // P = K_MAX * 256, verify 256 blocks (SBN boundary)
        let p_u64 = u64::from(K_MAX) * 256;
        assert!(
            u32::try_from(p_u64).is_ok(),
            "test precondition: fits in u32"
        );
        let p = u32::try_from(p_u64).unwrap();
        let blocks = partition_source_blocks(p).expect("max blocks should succeed");
        assert_eq!(
            blocks.len(),
            256,
            "bead_id={TEST_BEAD_ID} case=max_blocks_256"
        );

        // Each block has exactly K_MAX pages
        for block in &blocks {
            assert_eq!(
                block.num_pages, K_MAX,
                "bead_id={TEST_BEAD_ID} case=max_blocks_each_k_max block={}",
                block.index
            );
        }

        // Last block index is 255
        assert_eq!(blocks[255].index, 255);

        // Total coverage
        let total: u64 = blocks.iter().map(|b| u64::from(b.num_pages)).sum();
        assert_eq!(
            total, p_u64,
            "bead_id={TEST_BEAD_ID} case=max_blocks_coverage"
        );
    }

    #[test]
    fn test_partition_overflow_too_many_pages() {
        // P > K_MAX * 256 must error
        let p_u64 = u64::from(K_MAX) * 256 + 1;
        if let Ok(p) = u32::try_from(p_u64) {
            let result = partition_source_blocks(p);
            assert!(
                result.is_err(),
                "bead_id={TEST_BEAD_ID} case=overflow_error p={p}"
            );
        }
        // If p_u64 doesn't fit in u32, the overflow is caught at the type level
    }

    // -----------------------------------------------------------------------
    // Property tests
    // -----------------------------------------------------------------------

    #[test]
    fn prop_partition_coverage() {
        // For a range of DB sizes, total pages across blocks == P
        for p in [
            1_u32,
            2,
            63,
            64,
            100,
            1000,
            K_MAX - 1,
            K_MAX,
            K_MAX + 1,
            K_MAX * 2,
            262_144,
            K_MAX * 100,
        ] {
            let blocks = partition_source_blocks(p).expect("partition should succeed");
            let total: u32 = blocks.iter().map(|b| b.num_pages).sum();
            assert_eq!(total, p, "bead_id={TEST_BEAD_ID} case=prop_coverage p={p}");
        }
    }

    #[test]
    fn prop_partition_deterministic() {
        // Same input always produces same output
        for p in [1_u32, K_MAX, K_MAX + 1, 262_144] {
            let a = partition_source_blocks(p).expect("first call");
            let b = partition_source_blocks(p).expect("second call");
            assert_eq!(a, b, "bead_id={TEST_BEAD_ID} case=prop_deterministic p={p}");
        }
    }

    #[test]
    fn prop_partition_k_max_bounds() {
        // Every block has num_pages <= K_MAX
        for p in [K_MAX + 1, K_MAX * 2, K_MAX * 3 + 7, 262_144, K_MAX * 100] {
            let blocks = partition_source_blocks(p).expect("partition should succeed");
            for block in &blocks {
                assert!(
                    block.num_pages <= K_MAX,
                    "bead_id={TEST_BEAD_ID} case=prop_k_max_bound p={p} block={} num_pages={}",
                    block.index,
                    block.num_pages
                );
            }
        }
    }

    #[test]
    fn prop_partition_contiguous_non_overlapping() {
        // Blocks are contiguous and non-overlapping
        for p in [K_MAX + 1, K_MAX * 5, 262_144] {
            let blocks = partition_source_blocks(p).expect("partition should succeed");
            for window in blocks.windows(2) {
                let end_prev = window[0].start_page + window[0].num_pages;
                assert_eq!(
                    window[1].start_page, end_prev,
                    "bead_id={TEST_BEAD_ID} case=prop_contiguous p={p}"
                );
            }
            // First block starts at page 1
            assert_eq!(blocks[0].start_page, 1);
            // Last block ends at page P
            let last = blocks.last().unwrap();
            assert_eq!(last.start_page + last.num_pages - 1, p);
        }
    }

    #[test]
    fn prop_partition_sequential_indices() {
        for p in [K_MAX + 1, K_MAX * 3, 262_144] {
            let blocks = partition_source_blocks(p).expect("partition should succeed");
            for (i, block) in blocks.iter().enumerate() {
                assert_eq!(
                    block.index,
                    u8::try_from(i).unwrap(),
                    "bead_id={TEST_BEAD_ID} case=prop_sequential_indices p={p}"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Compliance tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bd_1hi_6_unit_compliance_gate() {
        // Verify section-specific unit validation hooks are wired.
        assert_eq!(K_MAX, 56_403, "RFC 6330 K_max constant");
        assert_eq!(SBN_MAX, 255, "RFC 6330 SBN_max constant");
        assert_eq!(
            MAX_PARTITIONABLE_PAGES,
            56_403 * 256,
            "max partitionable pages"
        );

        // Verify the function exists and is callable
        let _ = partition_source_blocks(0);
        let _ = partition_source_blocks(1);
        let _ = partition_source_blocks(K_MAX);
    }

    #[test]
    fn prop_bd_1hi_6_structure_compliance() {
        // Property check that required structure blocks are present.
        // For any valid P, blocks form a valid partition.
        let test_sizes = [
            1_u32,
            100,
            K_MAX,
            K_MAX + 1,
            K_MAX * 2,
            K_MAX * 100,
            K_MAX * 256,
        ];
        for &p in &test_sizes {
            let blocks = partition_source_blocks(p).expect("valid partition");
            // Coverage
            let total: u32 = blocks.iter().map(|b| b.num_pages).sum();
            assert_eq!(total, p);
            // Bounds
            for b in &blocks {
                assert!(b.num_pages <= K_MAX);
                assert!(b.num_pages > 0);
            }
            // Sequential
            for (i, b) in blocks.iter().enumerate() {
                assert_eq!(b.index, u8::try_from(i).unwrap());
            }
        }
    }
}
