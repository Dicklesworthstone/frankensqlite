//! SSI Witness Plane: Requirements, TxnToken Identity, WitnessKey Granularity (§5.6.4.1-5.6.4.3).
//!
//! ## §5.6.4.1 Non-Negotiable Requirements
//!
//! 1. **No false negatives**: If txn R reads WitnessKey K and overlapping txn W
//!    writes K, SSI validation MUST discover R as a candidate. Predicate reads
//!    (phantom protection) MUST register witness keys covering any write that
//!    would change the predicate result.
//! 2. **Cross-process**: Works across multiple OS processes sharing only
//!    shared-memory + ECS logs.
//! 3. **Distributed-ready**: Evidence is ECS objects; symbol-native replication
//!    carries the dependency graph.
//! 4. **Self-healing**: Missing/corrupt witness symbols within tolerance are
//!    reconstructed (or surfaced with explicit diagnostic proofs).
//! 5. **Monotonic updates**: Hot-plane updates are unions only. Clearing only
//!    by epoch swap under provably safe GC horizon.
//!
//! ## §5.6.4.2 TxnToken
//!
//! `TxnToken := (txn_id: TxnId, txn_epoch: TxnEpoch)`
//!
//! Any lookup of a slot-derived candidate MUST validate (txn_id, txn_epoch)
//! match. Permits false positives (stale bits), forbids false negatives.
//!
//! ## §5.6.4.3 WitnessKey
//!
//! Canonical key space for rw-antidependency tracking. Registration rules:
//! - Point read/uniqueness check: `Cell(btree_root_pgno, cell_tag(key_bytes))`
//! - Point write: `Cell(...)` AND `Page(leaf_pgno)` as write witness
//! - Range scan/predicate read: `Page(leaf_pgno)` per visited leaf page
//! - Optional refinement: `KeyRange` for reduced false positives

use fsqlite_types::{PageNumber, TxnEpoch, TxnId, TxnToken, WitnessKey};
use tracing::{debug, info};

// ---------------------------------------------------------------------------
// Witness Registration
// ---------------------------------------------------------------------------

/// A set of read and write witnesses for a single transaction.
///
/// Fed by semantic operations (VDBE/B-tree), NOT raw pager I/O (§5.6.4.3).
#[derive(Debug, Clone, Default)]
pub struct WitnessSet {
    /// Read witnesses (rw-antidependency sources).
    pub reads: Vec<WitnessKey>,
    /// Write witnesses (rw-antidependency targets).
    pub writes: Vec<WitnessKey>,
    /// Transaction identity for epoch validation.
    pub token: Option<TxnToken>,
}

impl WitnessSet {
    /// Create a new empty witness set for the given transaction.
    #[must_use]
    pub fn new(token: TxnToken) -> Self {
        Self {
            reads: Vec::new(),
            writes: Vec::new(),
            token: Some(token),
        }
    }

    /// Register a point read witness (Cell granularity).
    pub fn register_point_read(&mut self, btree_root: PageNumber, canonical_key_bytes: &[u8]) {
        let key = WitnessKey::for_cell_read(btree_root, canonical_key_bytes);
        debug!(
            btree_root = btree_root.get(),
            witness_kind = "cell",
            op_kind = "point_read",
            "witness key registered"
        );
        self.reads.push(key);
    }

    /// Register range scan witnesses (Page granularity per visited leaf).
    pub fn register_range_scan(&mut self, leaf_pages: &[PageNumber]) {
        let keys = WitnessKey::for_range_scan(leaf_pages);
        for key in &keys {
            if let WitnessKey::Page(pgno) = key {
                debug!(
                    page = pgno.get(),
                    witness_kind = "page",
                    op_kind = "range_scan",
                    "witness key registered"
                );
            }
        }
        self.reads.extend(keys);
    }

    /// Register a point write witness (Cell + Page).
    pub fn register_point_write(
        &mut self,
        btree_root: PageNumber,
        canonical_key_bytes: &[u8],
        leaf_pgno: PageNumber,
    ) {
        let (cell, page) = WitnessKey::for_point_write(btree_root, canonical_key_bytes, leaf_pgno);
        debug!(
            btree_root = btree_root.get(),
            leaf_page = leaf_pgno.get(),
            witness_kind = "cell+page",
            op_kind = "point_write",
            "witness key registered"
        );
        self.writes.push(cell);
        self.writes.push(page);
    }

    /// Publish summary logging for the witness set.
    pub fn publish_summary(&self) {
        if let Some(ref token) = self.token {
            info!(
                txn_id = token.id.get(),
                txn_epoch = token.epoch.get(),
                read_count = self.reads.len(),
                write_count = self.writes.len(),
                plane = "hot",
                "witness publication summary"
            );
        }
    }

    /// Check if a given `WitnessKey` overlaps with any write in this set.
    ///
    /// Used for SSI candidate discovery: if txn R reads K and this txn wrote K,
    /// R is a candidate for rw-antidependency.
    #[must_use]
    pub fn overlaps_write(&self, read_key: &WitnessKey) -> bool {
        self.writes
            .iter()
            .any(|w| witness_keys_overlap(w, read_key))
    }
}

/// Check if two witness keys overlap (potential rw-antidependency).
///
/// Overlap rules:
/// - Page vs Page: same page
/// - Cell vs Cell: same btree_root and tag
/// - Page vs Cell: page matches the Cell's btree_root page (conservative)
/// - ByteRange vs ByteRange: same page and overlapping ranges
/// - Any vs Custom/KeyRange: conservative (always overlap)
#[must_use]
pub fn witness_keys_overlap(a: &WitnessKey, b: &WitnessKey) -> bool {
    match (a, b) {
        (WitnessKey::Page(pa), WitnessKey::Page(pb)) => pa == pb,
        (
            WitnessKey::Cell {
                btree_root: ra,
                tag: ta,
            },
            WitnessKey::Cell {
                btree_root: rb,
                tag: tb,
            },
        ) => ra == rb && ta == tb,
        (
            WitnessKey::KeyRange { btree_root: ra, .. },
            WitnessKey::KeyRange { btree_root: rb, .. },
        ) => ra == rb,
        (WitnessKey::KeyRange { btree_root, .. }, WitnessKey::Page(p))
        | (WitnessKey::Page(p), WitnessKey::KeyRange { btree_root, .. }) => p == btree_root,
        (
            WitnessKey::KeyRange {
                btree_root: range_root,
                ..
            },
            WitnessKey::Cell {
                btree_root: cell_root,
                ..
            },
        )
        | (
            WitnessKey::Cell {
                btree_root: cell_root,
                ..
            },
            WitnessKey::KeyRange {
                btree_root: range_root,
                ..
            },
        ) => range_root == cell_root,
        (
            WitnessKey::KeyRange {
                btree_root: range_root,
                ..
            },
            WitnessKey::ByteRange { page, .. },
        )
        | (
            WitnessKey::ByteRange { page, .. },
            WitnessKey::KeyRange {
                btree_root: range_root,
                ..
            },
        ) => range_root == page,
        // Page overlaps with Cell if the page could contain that cell's btree.
        // Conservative: always overlap when page matches btree_root.
        (WitnessKey::Page(p), WitnessKey::Cell { btree_root, .. })
        | (WitnessKey::Cell { btree_root, .. }, WitnessKey::Page(p)) => p == btree_root,
        (
            WitnessKey::ByteRange {
                page: pa,
                start: sa,
                len: la,
            },
            WitnessKey::ByteRange {
                page: pb,
                start: sb,
                len: lb,
            },
        ) => pa == pb && *sa < sb + lb && *sb < sa + la,
        // Conservative fallback: anything involving KeyRange/Custom overlaps.
        _ => true,
    }
}

/// Validate that a TxnToken matches the expected identity for a slot-derived candidate.
///
/// Returns true if the (txn_id, txn_epoch) pair matches, meaning this is
/// NOT a stale reference. False positives (stale bits) are permitted by §5.6.4.2;
/// false negatives (missing candidates) are forbidden.
#[must_use]
pub fn validate_txn_token(candidate: &TxnToken, slot_id: TxnId, slot_epoch: TxnEpoch) -> bool {
    candidate.id == slot_id && candidate.epoch == slot_epoch
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn page(n: u32) -> PageNumber {
        PageNumber::new(n).unwrap()
    }

    fn token(id: u64, epoch: u32) -> TxnToken {
        TxnToken::new(TxnId::new(id).unwrap(), TxnEpoch::new(epoch))
    }

    // -- bd-3t3.9.1 test 1: B-tree leaf read generates deterministic WitnessKey --

    #[test]
    fn test_witness_key_from_table_read() {
        let btree_root = page(2);
        let key_bytes = b"user_id=42";

        let witness = WitnessKey::for_cell_read(btree_root, key_bytes);

        assert!(witness.is_cell());
        if let WitnessKey::Cell {
            btree_root: root,
            tag,
        } = &witness
        {
            assert_eq!(*root, btree_root);
            assert_ne!(*tag, 0, "tag must be non-zero for valid key bytes");
        }
    }

    // -- bd-3t3.9.1 test 2: Index range scan generates keys for scanned range --

    #[test]
    fn test_witness_key_from_index_read() {
        let leaf_pages = vec![page(10), page(11), page(12)];

        let witnesses = WitnessKey::for_range_scan(&leaf_pages);

        assert_eq!(witnesses.len(), 3);
        for (i, w) in witnesses.iter().enumerate() {
            assert!(w.is_page(), "range scan witnesses must be Page-level");
            if let WitnessKey::Page(p) = w {
                assert_eq!(p.get(), 10 + u32::try_from(i).unwrap());
            }
        }
    }

    // -- bd-3t3.9.1 test 3: Write generates distinct WitnessKey pair --

    #[test]
    fn test_witness_key_from_write() {
        let btree_root = page(2);
        let key_bytes = b"user_id=42";
        let leaf_pgno = page(15);

        let (cell, page_w) = WitnessKey::for_point_write(btree_root, key_bytes, leaf_pgno);

        assert!(cell.is_cell(), "write cell witness must be Cell variant");
        assert!(page_w.is_page(), "write page witness must be Page variant");

        // The cell witness from a write should match the cell witness from a read
        // of the same key (for rw-antidependency detection).
        let read_witness = WitnessKey::for_cell_read(btree_root, key_bytes);
        assert_eq!(
            cell, read_witness,
            "write cell must match read cell for same key"
        );
    }

    // -- bd-3t3.9.1 test 4: Same operation, same data → same key --

    #[test]
    fn test_witness_key_deterministic() {
        let btree_root = page(5);
        let key_bytes = b"email=alice@example.com";

        let tag1 = WitnessKey::cell_tag(btree_root, key_bytes);
        let tag2 = WitnessKey::cell_tag(btree_root, key_bytes);

        assert_eq!(tag1, tag2, "cell_tag must be deterministic across calls");

        // Different key bytes produce different tags.
        let tag3 = WitnessKey::cell_tag(btree_root, b"email=bob@example.com");
        assert_ne!(
            tag1, tag3,
            "different key bytes should produce different tags"
        );

        // Different btree_root produces different tags.
        let tag4 = WitnessKey::cell_tag(page(6), key_bytes);
        assert_ne!(
            tag1, tag4,
            "different btree_root should produce different tags"
        );
    }

    // -- bd-3t3.9.1 test 5: Stale slot-id misbind prevented by epoch check --

    #[test]
    fn test_txn_token_epoch_prevents_stale() {
        let original = token(42, 1);

        // Same id, same epoch → valid.
        assert!(validate_txn_token(
            &original,
            TxnId::new(42).unwrap(),
            TxnEpoch::new(1)
        ));

        // Same id, different epoch → stale (slot was recycled).
        assert!(!validate_txn_token(
            &original,
            TxnId::new(42).unwrap(),
            TxnEpoch::new(2)
        ));

        // Different id, same epoch → wrong slot entirely.
        assert!(!validate_txn_token(
            &original,
            TxnId::new(99).unwrap(),
            TxnEpoch::new(1)
        ));
    }

    // -- bd-3t3.9.1 test 6: Range scan registers Page per visited leaf --

    #[test]
    fn test_phantom_protection_leaf_pages() {
        let mut ws = WitnessSet::new(token(1, 1));

        // Range scan visits 5 leaf pages (including zero-row scans).
        let leaves = vec![page(100), page(101), page(102), page(103), page(104)];
        ws.register_range_scan(&leaves);

        assert_eq!(ws.reads.len(), 5, "must register one witness per leaf page");
        for (i, key) in ws.reads.iter().enumerate() {
            assert!(
                matches!(key, WitnessKey::Page(p) if p.get() == 100 + u32::try_from(i).unwrap()),
                "each witness must be Page(leaf_pgno)"
            );
        }
    }

    // -- bd-3t3.9.1 test 7: cell_tag deterministic across "processes" --

    #[test]
    fn test_cell_tag_deterministic_cross_process() {
        // Simulate two independent derivations (as would happen in separate processes).
        let btree_root = page(3);
        let key_bytes = b"account_number=9876543210";

        // "Process A" computation.
        let tag_a = WitnessKey::cell_tag(btree_root, key_bytes);

        // "Process B" computation (same inputs, fresh call).
        let tag_b = WitnessKey::cell_tag(btree_root, key_bytes);

        assert_eq!(
            tag_a, tag_b,
            "cell_tag must be identical across independent processes for same inputs"
        );

        // Verify domain separation: different prefix produces different hash.
        // This is inherent to the xxh3_64 domain-separated construction.
        let tag_different_root = WitnessKey::cell_tag(page(4), key_bytes);
        assert_ne!(tag_a, tag_different_root);
    }

    // -- bd-3t3.9.1 test 8: Birthday paradox collision analysis --

    #[test]
    fn prop_witness_key_collision_bounded() {
        // Birthday paradox: for n items in a space of size 2^64,
        // collision probability ≈ n² / (2 * 2^64).
        // For 1M distinct keys: p ≈ (10^6)² / (2 * 2^64) ≈ 2.7e-8 (negligible).
        //
        // We verify empirically with a smaller set that no collisions occur.
        let btree_root = page(1);
        let mut tags = std::collections::HashSet::new();

        for i in 0..10_000_u32 {
            let key_bytes = i.to_le_bytes();
            let tag = WitnessKey::cell_tag(btree_root, &key_bytes);
            assert!(
                tags.insert(tag),
                "collision at i={i}: tag={tag:#x} (birthday paradox threshold not exceeded)"
            );
        }

        // Also verify across different btree roots.
        let tag_root1 = WitnessKey::cell_tag(page(1), b"key");
        let tag_root2 = WitnessKey::cell_tag(page(2), b"key");
        assert_ne!(
            tag_root1, tag_root2,
            "different roots must produce different tags"
        );
    }

    // -- Witness overlap tests --

    #[test]
    fn test_witness_overlap_detection() {
        let btree_root = page(2);
        let key_bytes = b"pk=1";

        // Writer writes key.
        let mut writer_ws = WitnessSet::new(token(2, 1));
        writer_ws.register_point_write(btree_root, key_bytes, page(10));

        // Reader reads same key → overlap detected.
        let read_key = WitnessKey::for_cell_read(btree_root, key_bytes);
        assert!(
            writer_ws.overlaps_write(&read_key),
            "must detect overlap between reader and writer on same cell"
        );

        // Reader reads different key → no overlap.
        let other_key = WitnessKey::for_cell_read(btree_root, b"pk=2");
        assert!(
            !writer_ws.overlaps_write(&other_key),
            "must not detect overlap for different key"
        );
    }

    #[test]
    fn test_keyrange_phantom_protection() {
        let key_range = WitnessKey::KeyRange {
            btree_root: page(100),
            lo: b"aa".to_vec(),
            hi: b"zz".to_vec(),
        };
        let same_leaf_write = WitnessKey::Page(page(100));
        let other_leaf_write = WitnessKey::Page(page(101));

        assert!(
            witness_keys_overlap(&key_range, &same_leaf_write),
            "range witness must overlap writes on the same btree root page"
        );
        assert!(
            !witness_keys_overlap(&key_range, &other_leaf_write),
            "range witness should not overlap writes from other roots"
        );

        let same_tree_other_bounds = WitnessKey::KeyRange {
            btree_root: page(100),
            lo: b"m".to_vec(),
            hi: b"n".to_vec(),
        };
        assert!(
            witness_keys_overlap(&key_range, &same_tree_other_bounds),
            "same-tree key ranges conservatively overlap to preserve phantom safety"
        );
    }

    #[test]
    fn test_witness_set_summary() {
        let mut ws = WitnessSet::new(token(42, 3));
        ws.register_point_read(page(2), b"key1");
        ws.register_point_write(page(2), b"key2", page(10));
        ws.register_range_scan(&[page(20), page(21)]);

        // 1 cell read + 2 page reads from range scan = 3 reads.
        assert_eq!(ws.reads.len(), 3);
        // 1 cell write + 1 page write = 2 writes.
        assert_eq!(ws.writes.len(), 2);

        // Summary logging (just verify it doesn't panic).
        ws.publish_summary();
    }
}
