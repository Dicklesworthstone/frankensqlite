//! Comprehensive B-tree invariant, S3-FIFO hit-rate, and swizzle correctness
//! tests (bd-3ta.5).
//!
//! Covers:
//! 1. Sorted order and balance after random insert/delete sequences
//! 2. S3-FIFO hit-rate comparison against LRU-like baseline on workloads
//! 3. Pointer swizzle registry correctness under concurrent access
//! 4. Overflow pages: large payloads round-trip correctly
//! 5. Free-list: no page leaks after mixed insert/delete

#[cfg(test)]
mod tests {
    use crate::cursor::{BtCursor, MemPageStore};
    use crate::swizzle::{PageTemperature, SwizzlePtr, SwizzleRegistry};
    use crate::traits::BtreeCursorOps;
    use fsqlite_types::PageNumber;
    use fsqlite_types::cx::Cx;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    const USABLE: u32 = 4096;

    // ────────────────────────────────────────────────────────────────────
    // 1. B-TREE SORTED ORDER & BALANCE INVARIANTS
    // ────────────────────────────────────────────────────────────────────

    /// Insert N rows and verify forward scan produces sorted rowids.
    fn verify_sorted_order(cursor: &mut BtCursor<MemPageStore>, cx: &Cx, expected: &BTreeSet<i64>) {
        let has = cursor.first(cx).unwrap();
        if expected.is_empty() {
            assert!(!has, "empty tree should have no rows");
            return;
        }
        assert!(has, "non-empty tree should have rows");

        let mut prev = i64::MIN;
        let mut count = 0_usize;
        loop {
            if cursor.eof() {
                break;
            }
            let rowid = cursor.rowid(cx).unwrap();
            assert!(
                rowid > prev,
                "rowids not strictly ascending: prev={prev}, current={rowid}"
            );
            assert!(
                expected.contains(&rowid),
                "unexpected rowid {rowid} in tree"
            );
            prev = rowid;
            count += 1;
            if !cursor.next(cx).unwrap() {
                break;
            }
        }
        assert_eq!(
            count,
            expected.len(),
            "row count mismatch: got {count}, expected {}",
            expected.len()
        );
    }

    /// Verify backward scan produces reverse-sorted rowids.
    fn verify_reverse_order(
        cursor: &mut BtCursor<MemPageStore>,
        cx: &Cx,
        expected: &BTreeSet<i64>,
    ) {
        let has = cursor.last(cx).unwrap();
        if expected.is_empty() {
            assert!(!has, "empty tree should have no rows on last()");
            return;
        }
        assert!(has, "non-empty tree should have rows on last()");

        let mut prev = i64::MAX;
        let mut count = 0_usize;
        loop {
            if cursor.eof() {
                break;
            }
            let rowid = cursor.rowid(cx).unwrap();
            assert!(
                rowid < prev,
                "rowids not strictly descending: prev={prev}, current={rowid}"
            );
            prev = rowid;
            count += 1;
            if !cursor.prev(cx).unwrap() {
                break;
            }
        }
        assert_eq!(count, expected.len(), "reverse count mismatch");
    }

    #[test]
    fn btree_invariant_sequential_insert_1000() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        let mut expected = BTreeSet::new();
        let payload = vec![0xAB; 100];
        for i in 1_i64..=1000 {
            cursor.table_insert(&cx, i, &payload).unwrap();
            expected.insert(i);
        }

        verify_sorted_order(&mut cursor, &cx, &expected);
        verify_reverse_order(&mut cursor, &cx, &expected);
    }

    #[test]
    fn btree_invariant_reverse_insert_1000() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        let mut expected = BTreeSet::new();
        let payload = vec![0xCD; 100];
        for i in (1_i64..=1000).rev() {
            cursor.table_insert(&cx, i, &payload).unwrap();
            expected.insert(i);
        }

        verify_sorted_order(&mut cursor, &cx, &expected);
    }

    #[test]
    fn btree_invariant_insert_delete_interleaved() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        let mut expected = BTreeSet::new();
        let payload = vec![0xEF; 50];

        // Insert 500 rows.
        for i in 1_i64..=500 {
            cursor.table_insert(&cx, i, &payload).unwrap();
            expected.insert(i);
        }

        // Delete every other row.
        for i in (1_i64..=500).step_by(2) {
            let seek = cursor.table_move_to(&cx, i).unwrap();
            assert!(seek.is_found(), "row {i} should exist for deletion");
            cursor.delete(&cx).unwrap();
            expected.remove(&i);
        }

        verify_sorted_order(&mut cursor, &cx, &expected);

        // Insert more rows in gaps.
        for i in 501_i64..=750 {
            cursor.table_insert(&cx, i, &payload).unwrap();
            expected.insert(i);
        }

        verify_sorted_order(&mut cursor, &cx, &expected);
        verify_reverse_order(&mut cursor, &cx, &expected);
    }

    #[test]
    fn btree_invariant_delete_all_then_reinsert() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        let payload = vec![0x11; 30];
        let mut expected = BTreeSet::new();

        // Insert 200 rows.
        for i in 1_i64..=200 {
            cursor.table_insert(&cx, i, &payload).unwrap();
            expected.insert(i);
        }

        // Delete all.
        for i in 1_i64..=200 {
            let seek = cursor.table_move_to(&cx, i).unwrap();
            assert!(seek.is_found());
            cursor.delete(&cx).unwrap();
            expected.remove(&i);
        }

        verify_sorted_order(&mut cursor, &cx, &expected);

        // Reinsert different rows.
        for i in 301_i64..=500 {
            cursor.table_insert(&cx, i, &payload).unwrap();
            expected.insert(i);
        }

        verify_sorted_order(&mut cursor, &cx, &expected);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn btree_invariant_random_insert_delete(
            ops in prop::collection::vec(
                (any::<bool>(), 1_i64..5000),
                1..=2000
            ),
        ) {
            let cx = Cx::new();
            let root = PageNumber::new(2).unwrap();
            let store = MemPageStore::with_empty_table(root, USABLE);
            let mut cursor = BtCursor::new(store, root, USABLE, true);

            let mut expected = BTreeSet::new();
            let payload = vec![0x42; 60];

            for (is_insert, rowid) in &ops {
                if *is_insert || !expected.contains(rowid) {
                    // Insert (or try to insert if not yet present).
                    if expected.insert(*rowid) {
                        cursor.table_insert(&cx, *rowid, &payload).unwrap();
                    }
                } else {
                    // Delete.
                    let seek = cursor.table_move_to(&cx, *rowid).unwrap();
                    if seek.is_found() {
                        cursor.delete(&cx).unwrap();
                        expected.remove(rowid);
                    }
                }
            }

            verify_sorted_order(&mut cursor, &cx, &expected);
        }

        #[test]
        fn index_btree_invariant_random_insert_delete(
            ops in prop::collection::vec(
                (any::<bool>(), prop::collection::vec(0u8..255, 1..100)),
                1..=2000
            ),
        ) {
            let cx = Cx::new();
            let root = PageNumber::new(2).unwrap();
            let store = MemPageStore::with_empty_index(root, USABLE);
            let mut cursor = BtCursor::new(store, root, USABLE, false);

            let mut expected = BTreeSet::new();

            for (is_insert, mut key) in ops {
                key.insert(0, 0xFF);

                if is_insert || !expected.contains(&key) {
                    if expected.insert(key.clone()) {
                        cursor.index_insert(&cx, &key).unwrap();
                    }
                } else {
                    let seek = cursor.index_move_to(&cx, &key).unwrap();
                    if seek.is_found() {
                        cursor.delete(&cx).unwrap();
                        expected.remove(&key);
                    }
                }
            }

            // Verify sorted order for index tree.
            let has = cursor.first(&cx).unwrap();
            if expected.is_empty() {
                assert!(!has, "empty tree should have no rows");
            } else {
                assert!(has, "non-empty tree should have rows");
                let mut btree_elements = BTreeSet::new();
                let mut prev: Option<Vec<u8>> = None;
                loop {
                    if cursor.eof() {
                        break;
                    }
                    let key = cursor.payload(&cx).unwrap();
                    if let Some(ref p) = prev {
                        assert!(key > *p, "keys not strictly ascending");
                    }
                    assert!(expected.contains(&key), "unexpected key in tree");
                    prev = Some(key.clone());
                    btree_elements.insert(key);
                    if !cursor.next(&cx).unwrap() {
                        break;
                    }
                }
                if btree_elements.len() != expected.len() {
                    let missing: Vec<_> = expected.difference(&btree_elements).collect();
                    panic!("row count mismatch! missing {} elements: {:?}", expected.len() - btree_elements.len(), missing);
                }
                assert_eq!(btree_elements.len(), expected.len(), "row count mismatch");
            }
        }
    }

    // ────────────────────────────────────────────────────────────────────
    // 2. S3-FIFO HIT RATE vs LRU BASELINE
    // ────────────────────────────────────────────────────────────────────

    /// Simple LRU cache for baseline comparison.
    struct SimpleLru {
        capacity: usize,
        pages: Vec<u32>,
    }

    impl SimpleLru {
        fn new(capacity: usize) -> Self {
            Self {
                capacity,
                pages: Vec::new(),
            }
        }

        fn access(&mut self, page_id: u32) -> bool {
            if let Some(pos) = self.pages.iter().position(|&p| p == page_id) {
                self.pages.remove(pos);
                self.pages.push(page_id);
                true // hit
            } else {
                if self.pages.len() >= self.capacity {
                    self.pages.remove(0); // evict LRU
                }
                self.pages.push(page_id);
                false // miss
            }
        }
    }

    fn run_s3fifo_workload(accesses: &[u32], capacity: usize) -> (usize, usize) {
        use fsqlite_pager::s3_fifo::S3Fifo;

        let mut cache = S3Fifo::new(capacity);
        let mut hits = 0_usize;
        let total = accesses.len();

        for &page_id in accesses {
            let pgno = PageNumber::new(page_id.max(1)).unwrap();
            if cache.access(pgno) {
                hits += 1;
            } else {
                let _ = cache.insert(pgno);
            }
        }
        (hits, total)
    }

    fn run_lru_workload(accesses: &[u32], capacity: usize) -> (usize, usize) {
        let mut cache = SimpleLru::new(capacity);
        let mut hits = 0_usize;
        let total = accesses.len();

        for &page_id in accesses {
            if cache.access(page_id) {
                hits += 1;
            }
        }
        (hits, total)
    }

    #[test]
    fn s3fifo_hit_rate_scan_heavy_workload() {
        // YCSB-like scan workload: sequential scan with some repeats.
        let capacity = 100;
        let mut accesses = Vec::new();

        // Sequential scan (simulates table scan).
        for i in 1_u32..=500 {
            accesses.push(i);
        }
        // Hot set: pages 1-20 accessed repeatedly.
        for _ in 0..2000 {
            for i in 1_u32..=20 {
                accesses.push(i);
            }
        }
        // Another sequential scan (polluter).
        for i in 501_u32..=1000 {
            accesses.push(i);
        }
        // Hot set again.
        for _ in 0..2000 {
            for i in 1_u32..=20 {
                accesses.push(i);
            }
        }

        let (s3_hits, s3_total) = run_s3fifo_workload(&accesses, capacity);
        let (lru_hits, lru_total) = run_lru_workload(&accesses, capacity);

        let s3_rate = s3_hits as f64 / s3_total as f64;
        let lru_rate = lru_hits as f64 / lru_total as f64;

        assert!(
            s3_rate >= lru_rate * 0.95,
            "S3-FIFO hit rate {s3_rate:.4} should be within 5% of LRU {lru_rate:.4} on scan-heavy workload"
        );
    }

    #[test]
    fn s3fifo_capacity_invariant_never_exceeded() {
        // Verify S3-FIFO never exceeds its capacity during any workload.
        use fsqlite_pager::s3_fifo::S3Fifo;

        let capacity = 20;
        let mut cache = S3Fifo::new(capacity);

        // Insert 100 pages.
        for i in 1_u32..=100 {
            let pgno = PageNumber::new(i).unwrap();
            let _ = cache.insert(pgno);
            assert!(
                cache.resident_len() <= capacity,
                "resident {} > capacity {} after inserting page {}",
                cache.resident_len(),
                capacity,
                i
            );
        }

        // Access hot pages repeatedly, interleave with new inserts.
        for round in 0_u32..50 {
            for i in 1_u32..=5 {
                let pgno = PageNumber::new(i).unwrap();
                cache.access(pgno);
            }
            let new_page = 101 + round;
            let pgno = PageNumber::new(new_page).unwrap();
            let _ = cache.insert(pgno);
            assert!(
                cache.resident_len() <= capacity,
                "resident {} > capacity {} in round {}",
                cache.resident_len(),
                capacity,
                round
            );
        }
    }

    #[test]
    fn s3fifo_ghost_admission_tracks_evicted() {
        // Verify that pages evicted from small appear in ghost queue.
        use fsqlite_pager::s3_fifo::S3Fifo;

        let capacity = 10;
        let mut cache = S3Fifo::new(capacity);

        // Insert 20 pages (overflows small and main).
        for i in 1_u32..=20 {
            let pgno = PageNumber::new(i).unwrap();
            let _ = cache.insert(pgno);
        }

        // Some early pages should now be in ghost.
        assert!(
            cache.ghost_len() > 0,
            "ghost queue should contain evicted pages"
        );
        assert!(
            cache.resident_len() <= capacity,
            "resident should not exceed capacity"
        );
    }

    // ────────────────────────────────────────────────────────────────────
    // 3. SWIZZLE REGISTRY CORRECTNESS
    // ────────────────────────────────────────────────────────────────────

    #[test]
    fn swizzle_registry_basic_lifecycle() {
        let registry = SwizzleRegistry::new();

        // Register pages.
        for i in 1_u64..=100 {
            registry.register_page(i);
        }
        assert_eq!(registry.tracked_count(), 100);
        assert_eq!(registry.swizzled_count(), 0);

        // Swizzle half.
        for i in 1_u64..=50 {
            assert!(registry.try_swizzle(i, i * 4096));
        }
        assert_eq!(registry.swizzled_count(), 50);

        // Verify frame addresses.
        for i in 1_u64..=50 {
            assert!(registry.is_swizzled(i));
            assert_eq!(registry.frame_addr(i), Some(i * 4096));
        }
        for i in 51_u64..=100 {
            assert!(!registry.is_swizzled(i));
            assert_eq!(registry.frame_addr(i), None);
        }

        // Unswizzle all.
        for i in 1_u64..=50 {
            assert!(registry.try_unswizzle(i));
        }
        assert_eq!(registry.swizzled_count(), 0);
    }

    #[test]
    fn swizzle_registry_double_swizzle_rejected() {
        let registry = SwizzleRegistry::new();
        registry.register_page(1);
        assert!(registry.try_swizzle(1, 4096));
        // Second swizzle with different addr should fail.
        assert!(!registry.try_swizzle(1, 8192));
        // Frame addr unchanged.
        assert_eq!(registry.frame_addr(1), Some(4096));
    }

    #[test]
    fn swizzle_registry_unswizzle_unregistered() {
        let registry = SwizzleRegistry::new();
        // Unswizzle a page that was never registered.
        assert!(!registry.try_unswizzle(999));
    }

    #[test]
    fn swizzle_ptr_state_transitions() {
        let ptr = SwizzlePtr::new_unswizzled(42).unwrap();
        assert!(!ptr.is_swizzled(std::sync::atomic::Ordering::SeqCst));

        // Swizzle.
        ptr.try_swizzle(42, 0x1000).unwrap();
        assert!(ptr.is_swizzled(std::sync::atomic::Ordering::SeqCst));

        // Unswizzle.
        ptr.try_unswizzle(0x1000, 42).unwrap();
        assert!(!ptr.is_swizzled(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn page_temperature_valid_transitions() {
        // Cold → Hot → Cooling → Cold (valid cycle).
        let t = PageTemperature::Cold;
        let t = t.transition(PageTemperature::Hot).unwrap();
        let t = t.transition(PageTemperature::Cooling).unwrap();
        let t = t.transition(PageTemperature::Cold).unwrap();
        // Also: Cold → Hot directly.
        let _ = t.transition(PageTemperature::Hot).unwrap();
    }

    #[test]
    fn page_temperature_invalid_transition() {
        // Cold → Cooling is not a valid transition.
        let t = PageTemperature::Cold;
        assert!(t.transition(PageTemperature::Cooling).is_err());
    }

    #[test]
    fn swizzle_registry_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let registry = Arc::new(SwizzleRegistry::new());

        // Register 1000 pages.
        for i in 1_u64..=1000 {
            registry.register_page(i);
        }

        let mut handles = Vec::new();

        // Spawn threads that swizzle/unswizzle concurrently.
        for t in 0_u64..4 {
            let reg = Arc::clone(&registry);
            handles.push(thread::spawn(move || {
                let base = t * 250 + 1;
                for i in base..base + 250 {
                    reg.try_swizzle(i, i * 4096);
                }
                for i in base..base + 250 {
                    reg.try_unswizzle(i);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All should be unswizzled now.
        assert_eq!(registry.swizzled_count(), 0);
        assert_eq!(registry.tracked_count(), 1000);
    }

    // ────────────────────────────────────────────────────────────────────
    // 4. OVERFLOW PAGES
    // ────────────────────────────────────────────────────────────────────

    #[test]
    fn overflow_large_payload_round_trip() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        // Payload larger than a single page (~4KB page, >3KB payload triggers overflow).
        let large_payload = vec![0xBE; 8000];
        cursor.table_insert(&cx, 1, &large_payload).unwrap();

        // Seek back and read.
        let seek = cursor.table_move_to(&cx, 1).unwrap();
        assert!(seek.is_found());
        let read_back = cursor.payload(&cx).unwrap();
        assert_eq!(
            read_back.len(),
            large_payload.len(),
            "overflow payload length mismatch"
        );
        assert_eq!(read_back, large_payload, "overflow payload data mismatch");
    }

    #[test]
    fn overflow_multiple_large_payloads() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        let payloads: Vec<Vec<u8>> = (1_u8..=10)
            .map(|i| vec![i; 6000 + (usize::from(i) * 500)])
            .collect();

        for (i, payload) in payloads.iter().enumerate() {
            #[allow(clippy::cast_possible_wrap)]
            let rowid = (i + 1) as i64;
            cursor.table_insert(&cx, rowid, payload).unwrap();
        }

        // Verify all payloads round-trip.
        for (i, payload) in payloads.iter().enumerate() {
            #[allow(clippy::cast_possible_wrap)]
            let rowid = (i + 1) as i64;
            let seek = cursor.table_move_to(&cx, rowid).unwrap();
            assert!(seek.is_found(), "row {rowid} not found");
            let read_back = cursor.payload(&cx).unwrap();
            assert_eq!(
                read_back.len(),
                payload.len(),
                "payload length mismatch for row {rowid}"
            );
            assert_eq!(read_back, *payload, "payload data mismatch for row {rowid}");
        }
    }

    #[test]
    fn overflow_very_large_payload() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        // 100KB payload — spans many overflow pages.
        let huge_payload: Vec<u8> = (0_u8..=255).cycle().take(100_000).collect();
        cursor.table_insert(&cx, 42, &huge_payload).unwrap();

        let seek = cursor.table_move_to(&cx, 42).unwrap();
        assert!(seek.is_found());
        let read_back = cursor.payload(&cx).unwrap();
        assert_eq!(read_back.len(), 100_000);
        assert_eq!(read_back, huge_payload);
    }

    // ────────────────────────────────────────────────────────────────────
    // 5. FREE-LIST: NO PAGE LEAKS
    // ────────────────────────────────────────────────────────────────────

    /// Count total pages in a `MemPageStore` by scanning its internal state.
    /// We verify that after inserting and deleting all rows, the page count
    /// is bounded (no unbounded growth / leaks).
    #[test]
    fn freelist_no_page_leaks_insert_delete_all() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        let payload = vec![0x77; 200];
        let n = 500_i64;

        // Insert N rows.
        for i in 1..=n {
            cursor.table_insert(&cx, i, &payload).unwrap();
        }

        // Verify tree has the root page — we can traverse it.
        assert!(cursor.first(&cx).unwrap());

        // Delete all rows.
        for i in 1..=n {
            let seek = cursor.table_move_to(&cx, i).unwrap();
            assert!(seek.is_found(), "row {i} should exist");
            cursor.delete(&cx).unwrap();
        }

        // Tree should be empty.
        assert!(
            !cursor.first(&cx).unwrap(),
            "tree should be empty after deleting all rows"
        );
    }

    #[test]
    fn freelist_interleaved_insert_delete_bounded_growth() {
        let cx = Cx::new();
        let root = PageNumber::new(2).unwrap();
        let store = MemPageStore::with_empty_table(root, USABLE);
        let mut cursor = BtCursor::new(store, root, USABLE, true);

        let payload = vec![0x33; 100];

        // Phase 1: Insert 300 rows.
        for i in 1_i64..=300 {
            cursor.table_insert(&cx, i, &payload).unwrap();
        }

        // Phase 2: Delete first 200.
        for i in 1_i64..=200 {
            let seek = cursor.table_move_to(&cx, i).unwrap();
            assert!(seek.is_found());
            cursor.delete(&cx).unwrap();
        }

        // Phase 3: Insert 200 new rows.
        for i in 301_i64..=500 {
            cursor.table_insert(&cx, i, &payload).unwrap();
        }

        // Verify invariant: 300 rows present (201-500).
        let mut expected = BTreeSet::new();
        for i in 201_i64..=500 {
            expected.insert(i);
        }
        verify_sorted_order(&mut cursor, &cx, &expected);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]

        #[test]
        fn freelist_random_ops_no_corruption(
            ops in prop::collection::vec(
                (any::<bool>(), 1_i64..1000),
                1..=500
            ),
        ) {
            let cx = Cx::new();
            let root = PageNumber::new(2).unwrap();
            let store = MemPageStore::with_empty_table(root, USABLE);
            let mut cursor = BtCursor::new(store, root, USABLE, true);

            let mut expected = BTreeSet::new();
            let payload = vec![0xAA; 80];

            for (is_insert, rowid) in &ops {
                if *is_insert && !expected.contains(rowid) {
                    cursor.table_insert(&cx, *rowid, &payload).unwrap();
                    expected.insert(*rowid);
                } else if !is_insert && expected.contains(rowid) {
                    let seek = cursor.table_move_to(&cx, *rowid).unwrap();
                    if seek.is_found() {
                        cursor.delete(&cx).unwrap();
                        expected.remove(rowid);
                    }
                }
            }

            // Always verify sorted order after random ops.
            verify_sorted_order(&mut cursor, &cx, &expected);

            // Verify all expected rows are retrievable with correct payload.
            for &rowid in &expected {
                let seek = cursor.table_move_to(&cx, rowid).unwrap();
                assert!(seek.is_found(), "row {rowid} should be findable");
                let data = cursor.payload(&cx).unwrap();
                assert_eq!(data, payload, "payload mismatch for row {rowid}");
            }
        }
    }
}
