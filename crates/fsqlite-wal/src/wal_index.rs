//! WAL-index hash table primitives.
//!
//! This module implements the SQLite-compatible SHM hash function:
//! `slot = (page_number * 383) & 8191` with linear probing.
//!
//! The constants and layout mirror SQLite's WAL-index design:
//! - 32 KiB SHM segments
//! - 4096 page-number entries + 8192 hash slots
//! - first segment reserves 136 header bytes, leaving 4062 usable entries

use fsqlite_error::{FrankenError, Result};

/// SQLite's prime hash multiplier (`HASHTABLE_HASH_1` in `wal.c`).
pub const WAL_INDEX_HASH_MULTIPLIER: u32 = 383;
/// Number of page-number entries per SHM segment.
pub const WAL_INDEX_PAGE_ARRAY_ENTRIES: usize = 4096;
/// Number of hash slots per SHM segment.
pub const WAL_INDEX_HASH_SLOTS: usize = 8192;
/// Slot mask for modulo `WAL_INDEX_HASH_SLOTS` (power-of-two table).
pub const WAL_INDEX_HASH_MASK: u32 = 8191;
/// SHM segment size in bytes.
pub const WAL_SHM_SEGMENT_BYTES: usize = 32 * 1024;
/// Hash table bytes per segment (`u16[8192]`).
pub const WAL_SHM_HASH_BYTES: usize = WAL_INDEX_HASH_SLOTS * 2;
/// Page array bytes per segment (`u32[4096]`).
pub const WAL_SHM_PAGE_ARRAY_BYTES: usize = WAL_INDEX_PAGE_ARRAY_ENTRIES * 4;
/// First-segment WAL-index header size in bytes.
pub const WAL_SHM_FIRST_HEADER_BYTES: usize = 136;
/// Header overlap measured in u32 entries.
pub const WAL_SHM_FIRST_HEADER_U32_SLOTS: usize = WAL_SHM_FIRST_HEADER_BYTES.div_ceil(4);
/// Usable frame entries in first segment.
pub const WAL_SHM_FIRST_USABLE_PAGE_ENTRIES: usize =
    WAL_INDEX_PAGE_ARRAY_ENTRIES - WAL_SHM_FIRST_HEADER_U32_SLOTS;
/// Usable frame entries in non-first segments.
pub const WAL_SHM_SUBSEQUENT_USABLE_PAGE_ENTRIES: usize = WAL_INDEX_PAGE_ARRAY_ENTRIES;

/// Segment kind controls capacity (first segment reserves header bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalIndexSegmentKind {
    First,
    Subsequent,
}

/// Lookup result for a page number in the hash table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalHashLookup {
    /// 0-based hash slot used for this mapping.
    pub slot: u32,
    /// 1-based page-entry index (0 means empty).
    pub one_based_index: u16,
    /// Matched page number.
    pub page_number: u32,
}

/// Minimal WAL-index hash segment model:
/// - page-number array entries (`u32`)
/// - hash table slots (`u16`, 1-based page index)
#[derive(Debug, Clone)]
pub struct WalIndexHashSegment {
    kind: WalIndexSegmentKind,
    page_numbers: Vec<u32>,
    hash_slots: [u16; WAL_INDEX_HASH_SLOTS],
}

impl WalIndexHashSegment {
    /// Create an empty hash segment.
    #[must_use]
    pub fn new(kind: WalIndexSegmentKind) -> Self {
        Self {
            kind,
            page_numbers: Vec::with_capacity(usable_page_entries(kind)),
            hash_slots: [0; WAL_INDEX_HASH_SLOTS],
        }
    }

    /// Segment kind (`First` or `Subsequent`).
    #[must_use]
    pub const fn kind(&self) -> WalIndexSegmentKind {
        self.kind
    }

    /// Capacity of page-number entries for this segment.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        usable_page_entries(self.kind)
    }

    /// Number of populated page-number entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.page_numbers.len()
    }

    /// Whether no entries are populated.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.page_numbers.is_empty()
    }

    /// Hash slots (`u16` one-based indexes).
    #[must_use]
    pub fn hash_slots(&self) -> &[u16; WAL_INDEX_HASH_SLOTS] {
        &self.hash_slots
    }

    /// Insert a page number using linear probing.
    ///
    /// If the same page already exists in the probe chain, its slot is updated
    /// to point at the newest entry.
    pub fn insert(&mut self, page_number: u32) -> Result<u16> {
        if self.page_numbers.len() >= self.capacity() {
            return Err(FrankenError::DatabaseFull);
        }

        self.page_numbers.push(page_number);
        let one_based_index = u16::try_from(self.page_numbers.len())
            .map_err(|_| FrankenError::internal("WAL page-number index overflowed u16 capacity"))?;

        let start_slot = wal_index_hash_slot(page_number);
        let mut slot = start_slot;

        loop {
            let slot_usize = usize::try_from(slot).expect("hash slot must fit usize");
            let existing = self.hash_slots[slot_usize];
            if existing == 0 {
                self.hash_slots[slot_usize] = one_based_index;
                return Ok(one_based_index);
            }

            let existing_idx = usize::from(existing.saturating_sub(1));
            if self.page_numbers[existing_idx] == page_number {
                self.hash_slots[slot_usize] = one_based_index;
                return Ok(one_based_index);
            }

            slot = (slot + 1) & WAL_INDEX_HASH_MASK;
            if slot == start_slot {
                return Err(FrankenError::DatabaseFull);
            }
        }
    }

    /// Lookup page number via hash + linear probing.
    #[must_use]
    pub fn lookup(&self, page_number: u32) -> Option<WalHashLookup> {
        let start_slot = wal_index_hash_slot(page_number);
        let mut slot = start_slot;

        loop {
            let slot_usize = usize::try_from(slot).expect("hash slot must fit usize");
            let one_based = self.hash_slots[slot_usize];
            if one_based == 0 {
                return None;
            }

            let idx = usize::from(one_based - 1);
            if self.page_numbers[idx] == page_number {
                return Some(WalHashLookup {
                    slot,
                    one_based_index: one_based,
                    page_number,
                });
            }

            slot = (slot + 1) & WAL_INDEX_HASH_MASK;
            if slot == start_slot {
                return None;
            }
        }
    }
}

/// Compute SQLite-compatible WAL-index hash slot.
#[must_use]
pub const fn wal_index_hash_slot(page_number: u32) -> u32 {
    page_number.wrapping_mul(WAL_INDEX_HASH_MULTIPLIER) & WAL_INDEX_HASH_MASK
}

/// Compute simple modulo hash (used only for compatibility comparison tests).
#[must_use]
pub const fn simple_modulo_slot(page_number: u32) -> u32 {
    page_number & WAL_INDEX_HASH_MASK
}

/// Number of usable page entries per segment kind.
#[must_use]
pub const fn usable_page_entries(kind: WalIndexSegmentKind) -> usize {
    match kind {
        WalIndexSegmentKind::First => WAL_SHM_FIRST_USABLE_PAGE_ENTRIES,
        WalIndexSegmentKind::Subsequent => WAL_SHM_SUBSEQUENT_USABLE_PAGE_ENTRIES,
    }
}

/// Encode a SHM u32 field in native byte order.
#[must_use]
pub const fn encode_native_u32(value: u32) -> [u8; 4] {
    value.to_ne_bytes()
}

/// Decode a SHM u32 field from native byte order.
#[must_use]
pub const fn decode_native_u32(bytes: [u8; 4]) -> u32 {
    u32::from_ne_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_hash_function_basic() {
        assert_eq!(wal_index_hash_slot(1), 383);
        assert_eq!(wal_index_hash_slot(2), 766);
        assert_eq!(wal_index_hash_slot(10), 3830);
        for pgno in 1_u32..=100 {
            let expected = pgno.wrapping_mul(383) & 8191;
            assert_eq!(wal_index_hash_slot(pgno), expected);
        }
    }

    #[test]
    fn test_wal_hash_sequential_distribution() {
        let mut buckets = vec![0_u16; WAL_INDEX_HASH_SLOTS];
        for pgno in 1_u32..=u32::try_from(WAL_INDEX_PAGE_ARRAY_ENTRIES).expect("fits") {
            let slot = usize::try_from(wal_index_hash_slot(pgno)).expect("slot fits");
            buckets[slot] += 1;
        }
        let max_bucket = buckets.into_iter().max().unwrap_or(0);
        assert!(max_bucket <= 1, "expected perfect spread, got {max_bucket}");
    }

    #[test]
    fn test_wal_hash_vs_simple_modulo() {
        let mut differences = 0_u32;
        for pgno in 1_u32..=100 {
            if wal_index_hash_slot(pgno) != simple_modulo_slot(pgno) {
                differences += 1;
            }
        }
        assert!(
            differences >= 90,
            "expected >=90 differing slots, got {differences}"
        );
    }

    #[test]
    fn test_wal_hash_zero_page() {
        assert_eq!(wal_index_hash_slot(0), 0);
    }

    #[test]
    fn test_wal_hash_large_page_numbers() {
        let values = [8192_u32, 65_536_u32, 2_147_483_648_u32, u32::MAX];
        for value in values {
            let slot = wal_index_hash_slot(value);
            assert!(slot <= WAL_INDEX_HASH_MASK);
        }
    }

    #[test]
    fn test_wal_hash_table_insert_lookup() {
        let mut seg = WalIndexHashSegment::new(WalIndexSegmentKind::Subsequent);
        seg.insert(42).expect("insert should succeed");
        let lookup = seg.lookup(42).expect("lookup should find inserted page");
        assert_eq!(lookup.page_number, 42);
        assert_eq!(lookup.one_based_index, 1);
    }

    #[test]
    fn test_wal_hash_table_collision_chain() {
        let mut seg = WalIndexHashSegment::new(WalIndexSegmentKind::Subsequent);
        let first = 22_u32;
        let second = first + 8192_u32; // guaranteed same slot under mask-based hash
        let start_slot = wal_index_hash_slot(first);
        assert_eq!(start_slot, wal_index_hash_slot(second));

        seg.insert(first).expect("first insert should succeed");
        seg.insert(second).expect("second insert should succeed");

        let first_lookup = seg.lookup(first).expect("first page should be found");
        let second_lookup = seg.lookup(second).expect("second page should be found");
        assert_ne!(first_lookup.one_based_index, second_lookup.one_based_index);
        assert_eq!(first_lookup.slot, start_slot);
        assert_eq!(
            second_lookup.slot,
            (start_slot + 1) & WAL_INDEX_HASH_MASK,
            "second colliding key should linear-probe to next slot"
        );
    }

    #[test]
    fn test_shm_first_segment_usable_entries() {
        assert_eq!(WAL_SHM_FIRST_HEADER_BYTES, 136);
        assert_eq!(WAL_SHM_FIRST_HEADER_U32_SLOTS, 34);
        assert_eq!(usable_page_entries(WalIndexSegmentKind::First), 4062);
    }

    #[test]
    fn test_shm_first_segment_capacity_enforced() {
        let mut first = WalIndexHashSegment::new(WalIndexSegmentKind::First);
        for pgno in 1_u32..=u32::try_from(WAL_SHM_FIRST_USABLE_PAGE_ENTRIES).expect("fits") {
            first
                .insert(pgno)
                .expect("entry within first-segment capacity must succeed");
        }
        assert_eq!(first.len(), WAL_SHM_FIRST_USABLE_PAGE_ENTRIES);
        let overflow = first.insert(99_999).expect_err("4063rd entry must fail");
        assert!(matches!(overflow, FrankenError::DatabaseFull));
    }

    #[test]
    fn test_lookup_correctness_across_segments() {
        let mut first = WalIndexHashSegment::new(WalIndexSegmentKind::First);
        let mut second = WalIndexHashSegment::new(WalIndexSegmentKind::Subsequent);

        // Fill first segment to ensure subsequent inserts are modeled in segment 2.
        for pgno in 1_u32..=u32::try_from(WAL_SHM_FIRST_USABLE_PAGE_ENTRIES).expect("fits") {
            first
                .insert(pgno)
                .expect("first-segment insert should succeed");
        }
        second
            .insert(1_000_001)
            .expect("second-segment insert should succeed");

        assert!(
            first.lookup(42).is_some(),
            "page in first segment must be found"
        );
        assert!(
            second.lookup(1_000_001).is_some(),
            "page in second segment must be found"
        );
        assert!(first.lookup(9_999_999).is_none());
        assert!(second.lookup(9_999_999).is_none());
    }

    #[test]
    fn test_shm_subsequent_segment_full_entries() {
        assert_eq!(usable_page_entries(WalIndexSegmentKind::Subsequent), 4096);
        assert_eq!(WAL_SHM_PAGE_ARRAY_BYTES, 16_384);
        assert_eq!(WAL_SHM_HASH_BYTES, 16_384);
        assert_eq!(WAL_SHM_SEGMENT_BYTES, 32 * 1024);
    }

    #[test]
    fn test_shm_native_byte_order() {
        let value = 0x12_34_56_78_u32;
        let encoded = encode_native_u32(value);
        assert_eq!(decode_native_u32(encoded), value);
        if cfg!(target_endian = "little") {
            assert_eq!(encoded, value.to_le_bytes());
        } else {
            assert_eq!(encoded, value.to_be_bytes());
        }
    }

    #[test]
    fn test_wal_hash_interop_c_sqlite() {
        // Known-value checks against SQLite's `walHash(pgno) = (pgno*383)&8191`.
        let cases = [
            (1_u32, 383_u32),
            (2, 766),
            (22, 234),
            (4096, (4096 * 383) & 8191),
            (8193, (8193 * 383) & 8191),
        ];
        for (pgno, expected_slot) in cases {
            assert_eq!(wal_index_hash_slot(pgno), expected_slot, "pgno={pgno}");
        }
    }
}
