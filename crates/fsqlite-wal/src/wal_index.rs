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

/// SQLite's prime hash multiplier (`HASHTABLE_HASH_1` in upstream SQLite).
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

// ── WAL-index header constants ──────────────────────────────────────

/// WAL-index header version (must be 3007000).
pub const WAL_INDEX_VERSION: u32 = 3_007_000;

/// Size of a single `WalIndexHdr` copy in bytes.
pub const WAL_INDEX_HDR_BYTES: usize = 48;

/// Size of the `WalCkptInfo` region in bytes.
pub const WAL_CKPT_INFO_BYTES: usize = 40;

/// Number of reader marks in `WalCkptInfo`.
pub const WAL_READ_MARK_COUNT: usize = 5;

/// Number of SHM lock slots in `WalCkptInfo`.
pub const WAL_LOCK_SLOT_COUNT: usize = 8;

/// Lock slot index for the WAL write lock.
pub const WAL_WRITE_LOCK: usize = 0;
/// Lock slot index for the WAL checkpoint lock.
pub const WAL_CKPT_LOCK: usize = 1;
/// Lock slot index for the WAL recovery lock.
pub const WAL_RECOVER_LOCK: usize = 2;
/// First lock slot index for reader locks (indices 3..7).
pub const WAL_READ_LOCK_BASE: usize = 3;

/// Parsed 48-byte WAL-index header (`WalIndexHdr`).
///
/// All fields are stored in **native** byte order (SHM is not portable
/// across architectures -- it is reconstructed from the WAL on startup).
///
/// ```text
/// Offset  Size  Field
///   0       4   iVersion (3007000)
///   4       4   unused
///   8       4   iChange (schema cookie mirror)
///  12       1   isInit (1 if initialized)
///  13       1   bigEndCksum (1 if WAL uses big-endian checksums)
///  14       2   szPage (database page size)
///  16       4   mxFrame (highest valid frame index in WAL)
///  20       4   nPage (database size in pages)
///  24       8   aFrameCksum[2] (running WAL checksum pair)
///  32       8   aSalt[2] (WAL salt pair)
///  40       8   aCksum[2] (checksum of this header, bytes 0..40)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalIndexHdr {
    /// Must be `WAL_INDEX_VERSION` (3007000).
    pub i_version: u32,
    /// Reserved/unused field.
    pub unused: u32,
    /// Schema cookie mirror (incremented on schema changes).
    pub i_change: u32,
    /// 1 if this header has been initialized.
    pub is_init: u8,
    /// 1 if the WAL uses big-endian checksums.
    pub big_end_cksum: u8,
    /// Database page size.
    pub sz_page: u16,
    /// Highest valid frame index in the WAL (0 = empty WAL).
    pub mx_frame: u32,
    /// Database size in pages.
    pub n_page: u32,
    /// Running WAL frame checksum pair.
    pub a_frame_cksum: [u32; 2],
    /// WAL salt pair (copied from WAL header).
    pub a_salt: [u32; 2],
    /// Header checksum (covers bytes 0..40 of this struct).
    pub a_cksum: [u32; 2],
}

impl WalIndexHdr {
    /// Parse a `WalIndexHdr` from a 48-byte native-order buffer.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < WAL_INDEX_HDR_BYTES {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WalIndexHdr too small: expected >= {WAL_INDEX_HDR_BYTES}, got {}",
                    buf.len()
                ),
            });
        }
        Ok(Self {
            i_version: decode_native_u32(read4(buf, 0)),
            unused: decode_native_u32(read4(buf, 4)),
            i_change: decode_native_u32(read4(buf, 8)),
            is_init: buf[12],
            big_end_cksum: buf[13],
            sz_page: u16::from_ne_bytes([buf[14], buf[15]]),
            mx_frame: decode_native_u32(read4(buf, 16)),
            n_page: decode_native_u32(read4(buf, 20)),
            a_frame_cksum: [
                decode_native_u32(read4(buf, 24)),
                decode_native_u32(read4(buf, 28)),
            ],
            a_salt: [
                decode_native_u32(read4(buf, 32)),
                decode_native_u32(read4(buf, 36)),
            ],
            a_cksum: [
                decode_native_u32(read4(buf, 40)),
                decode_native_u32(read4(buf, 44)),
            ],
        })
    }

    /// Serialize to a 48-byte native-order buffer.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; WAL_INDEX_HDR_BYTES] {
        let mut buf = [0u8; WAL_INDEX_HDR_BYTES];
        write4(&mut buf, 0, self.i_version);
        write4(&mut buf, 4, self.unused);
        write4(&mut buf, 8, self.i_change);
        buf[12] = self.is_init;
        buf[13] = self.big_end_cksum;
        buf[14..16].copy_from_slice(&self.sz_page.to_ne_bytes());
        write4(&mut buf, 16, self.mx_frame);
        write4(&mut buf, 20, self.n_page);
        write4(&mut buf, 24, self.a_frame_cksum[0]);
        write4(&mut buf, 28, self.a_frame_cksum[1]);
        write4(&mut buf, 32, self.a_salt[0]);
        write4(&mut buf, 36, self.a_salt[1]);
        write4(&mut buf, 40, self.a_cksum[0]);
        write4(&mut buf, 44, self.a_cksum[1]);
        buf
    }
}

/// Parsed 40-byte WAL checkpoint info (`WalCkptInfo`), at SHM offset 96.
///
/// ```text
/// Offset  Size  Field
///  96       4   nBackfill
/// 100      20   aReadMark[5] (5 u32 reader marks)
/// 120       8   aLock[8] (SHM lock slot bytes)
/// 128       4   nBackfillAttempted
/// 132       4   notUsed0
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCkptInfo {
    /// Number of frames backfilled into the database.
    pub n_backfill: u32,
    /// Reader mark values (WAL frame counts at reader-begin time).
    pub a_read_mark: [u32; WAL_READ_MARK_COUNT],
    /// Lock slot bytes (OS-level locks operate on these byte offsets).
    pub a_lock: [u8; WAL_LOCK_SLOT_COUNT],
    /// Number of frames attempted for backfill.
    pub n_backfill_attempted: u32,
    /// Reserved/unused.
    pub not_used0: u32,
}

impl WalCkptInfo {
    /// Parse from 40 bytes at the checkpoint info region (SHM offset 96).
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < WAL_CKPT_INFO_BYTES {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WalCkptInfo too small: expected >= {WAL_CKPT_INFO_BYTES}, got {}",
                    buf.len()
                ),
            });
        }
        let mut a_read_mark = [0u32; WAL_READ_MARK_COUNT];
        for (i, mark) in a_read_mark.iter_mut().enumerate() {
            *mark = decode_native_u32(read4(buf, 4 + i * 4));
        }
        let mut a_lock = [0u8; WAL_LOCK_SLOT_COUNT];
        a_lock.copy_from_slice(&buf[24..32]);

        Ok(Self {
            n_backfill: decode_native_u32(read4(buf, 0)),
            a_read_mark,
            a_lock,
            n_backfill_attempted: decode_native_u32(read4(buf, 32)),
            not_used0: decode_native_u32(read4(buf, 36)),
        })
    }

    /// Serialize to a 40-byte native-order buffer.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; WAL_CKPT_INFO_BYTES] {
        let mut buf = [0u8; WAL_CKPT_INFO_BYTES];
        write4(&mut buf, 0, self.n_backfill);
        for (i, &mark) in self.a_read_mark.iter().enumerate() {
            write4(&mut buf, 4 + i * 4, mark);
        }
        buf[24..32].copy_from_slice(&self.a_lock);
        write4(&mut buf, 32, self.n_backfill_attempted);
        write4(&mut buf, 36, self.not_used0);
        buf
    }
}

/// Verify that two `WalIndexHdr` copies match (lock-free consistency check).
///
/// The SHM header stores two copies of `WalIndexHdr` at offsets 0..48 and
/// 48..96. A reader accepts the header only if both copies are identical.
#[must_use]
pub fn wal_index_hdr_copies_match(buf: &[u8]) -> bool {
    if buf.len() < 2 * WAL_INDEX_HDR_BYTES {
        return false;
    }
    buf[..WAL_INDEX_HDR_BYTES] == buf[WAL_INDEX_HDR_BYTES..2 * WAL_INDEX_HDR_BYTES]
}

/// Parse the full 136-byte SHM header: dual `WalIndexHdr` copies + `WalCkptInfo`.
///
/// Returns `None` if the two header copies do not match (concurrent writer).
pub fn parse_shm_header(buf: &[u8]) -> Result<Option<(WalIndexHdr, WalCkptInfo)>> {
    if buf.len() < WAL_SHM_FIRST_HEADER_BYTES {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "SHM header too small: expected >= {WAL_SHM_FIRST_HEADER_BYTES}, got {}",
                buf.len()
            ),
        });
    }
    if !wal_index_hdr_copies_match(buf) {
        return Ok(None);
    }
    let hdr = WalIndexHdr::from_bytes(buf)?;
    let ckpt = WalCkptInfo::from_bytes(&buf[2 * WAL_INDEX_HDR_BYTES..])?;
    Ok(Some((hdr, ckpt)))
}

/// Write both copies of `WalIndexHdr` + `WalCkptInfo` into a 136-byte buffer.
pub fn write_shm_header(buf: &mut [u8], hdr: &WalIndexHdr, ckpt: &WalCkptInfo) -> Result<()> {
    if buf.len() < WAL_SHM_FIRST_HEADER_BYTES {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "SHM header buffer too small: expected >= {WAL_SHM_FIRST_HEADER_BYTES}, got {}",
                buf.len()
            ),
        });
    }
    let hdr_bytes = hdr.to_bytes();
    buf[..WAL_INDEX_HDR_BYTES].copy_from_slice(&hdr_bytes);
    buf[WAL_INDEX_HDR_BYTES..2 * WAL_INDEX_HDR_BYTES].copy_from_slice(&hdr_bytes);
    let ckpt_bytes = ckpt.to_bytes();
    buf[2 * WAL_INDEX_HDR_BYTES..WAL_SHM_FIRST_HEADER_BYTES].copy_from_slice(&ckpt_bytes);
    Ok(())
}

// ── Native-order read/write helpers (internal) ─────────────────────

fn read4(buf: &[u8], offset: usize) -> [u8; 4] {
    let mut out = [0u8; 4];
    out.copy_from_slice(&buf[offset..offset + 4]);
    out
}

fn write4(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&encode_native_u32(value));
}

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

    // ── bd-94us §11.10-11.12 WAL-index header tests ────────────────────

    #[test]
    fn test_wal_index_header_layout() {
        // Verify WalIndexHdr is 48 bytes and fields land at correct offsets.
        assert_eq!(WAL_INDEX_HDR_BYTES, 48);
        assert_eq!(
            2 * WAL_INDEX_HDR_BYTES + WAL_CKPT_INFO_BYTES,
            WAL_SHM_FIRST_HEADER_BYTES
        );

        let hdr = WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: 42,
            is_init: 1,
            big_end_cksum: 0,
            sz_page: 4096,
            mx_frame: 100,
            n_page: 50,
            a_frame_cksum: [0xAAAA_BBBB, 0xCCCC_DDDD],
            a_salt: [0x1111_2222, 0x3333_4444],
            a_cksum: [0x5555_6666, 0x7777_8888],
        };
        let bytes = hdr.to_bytes();
        assert_eq!(bytes.len(), 48);

        // iVersion at offset 0.
        assert_eq!(decode_native_u32(read4(&bytes, 0)), WAL_INDEX_VERSION);
        // szPage at offset 14 (u16 native).
        assert_eq!(u16::from_ne_bytes([bytes[14], bytes[15]]), 4096);
        // mxFrame at offset 16.
        assert_eq!(decode_native_u32(read4(&bytes, 16)), 100);
        // nPage at offset 20.
        assert_eq!(decode_native_u32(read4(&bytes, 20)), 50);
    }

    #[test]
    fn test_wal_index_header_duplication() {
        let hdr = WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: 7,
            is_init: 1,
            big_end_cksum: 0,
            sz_page: 4096,
            mx_frame: 50,
            n_page: 25,
            a_frame_cksum: [1, 2],
            a_salt: [3, 4],
            a_cksum: [5, 6],
        };
        let ckpt = WalCkptInfo {
            n_backfill: 0,
            a_read_mark: [0; WAL_READ_MARK_COUNT],
            a_lock: [0; WAL_LOCK_SLOT_COUNT],
            n_backfill_attempted: 0,
            not_used0: 0,
        };

        let mut buf = [0u8; WAL_SHM_FIRST_HEADER_BYTES];
        write_shm_header(&mut buf, &hdr, &ckpt).expect("write should succeed");

        // Matching copies: accepted.
        assert!(wal_index_hdr_copies_match(&buf));
        let (parsed_hdr, _parsed_ckpt) = parse_shm_header(&buf)
            .expect("parse")
            .expect("copies match");
        assert_eq!(parsed_hdr.mx_frame, 50);

        // Corrupt copy 2: rejected.
        buf[WAL_INDEX_HDR_BYTES + 16] ^= 0xFF;
        assert!(!wal_index_hdr_copies_match(&buf));
        let result = parse_shm_header(&buf).expect("parse succeeds");
        assert!(result.is_none(), "mismatched copies must be rejected");
    }

    #[test]
    fn test_wal_ckpt_info_layout() {
        // Verify WalCkptInfo is 40 bytes and fields at correct relative offsets.
        assert_eq!(WAL_CKPT_INFO_BYTES, 40);

        let ckpt = WalCkptInfo {
            n_backfill: 42,
            a_read_mark: [10, 20, 30, 40, 50],
            a_lock: [1, 2, 3, 4, 5, 6, 7, 8],
            n_backfill_attempted: 100,
            not_used0: 0,
        };
        let bytes = ckpt.to_bytes();

        // nBackfill at relative offset 0.
        assert_eq!(decode_native_u32(read4(&bytes, 0)), 42);
        // aReadMark[0..5] at relative offsets 4-23.
        for i in 0..WAL_READ_MARK_COUNT {
            let mark = decode_native_u32(read4(&bytes, 4 + i * 4));
            let expected_mark = u32::try_from(i + 1).expect("fits") * 10;
            assert_eq!(mark, expected_mark, "aReadMark[{i}]");
        }
        // aLock[0..8] at relative offsets 24-31.
        for i in 0..WAL_LOCK_SLOT_COUNT {
            let expected_lock = u8::try_from(i + 1).expect("fits");
            assert_eq!(bytes[24 + i], expected_lock, "aLock[{i}]");
        }
        // nBackfillAttempted at relative offset 32.
        assert_eq!(decode_native_u32(read4(&bytes, 32)), 100);

        // In the full SHM header, ckpt starts at absolute offset 96.
        let mut full = [0u8; WAL_SHM_FIRST_HEADER_BYTES];
        let dummy_hdr = WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: 0,
            is_init: 1,
            big_end_cksum: 0,
            sz_page: 4096,
            mx_frame: 0,
            n_page: 0,
            a_frame_cksum: [0; 2],
            a_salt: [0; 2],
            a_cksum: [0; 2],
        };
        write_shm_header(&mut full, &dummy_hdr, &ckpt).expect("write");
        // nBackfill at absolute offset 96.
        assert_eq!(decode_native_u32(read4(&full, 96)), 42);
        // aReadMark[0] at absolute offset 100.
        assert_eq!(decode_native_u32(read4(&full, 100)), 10);
        // aLock[0] at absolute offset 120.
        assert_eq!(full[120], 1);
        // nBackfillAttempted at absolute offset 128.
        assert_eq!(decode_native_u32(read4(&full, 128)), 100);
    }

    #[test]
    fn test_reader_marks_prevent_checkpoint_overwrite() {
        // Reader mark set to frame N prevents checkpoint past that frame.
        let mut ckpt = WalCkptInfo {
            n_backfill: 0,
            a_read_mark: [0; WAL_READ_MARK_COUNT],
            a_lock: [0; WAL_LOCK_SLOT_COUNT],
            n_backfill_attempted: 0,
            not_used0: 0,
        };

        // Reader 0 is at frame 50.
        ckpt.a_read_mark[0] = 50;
        // Reader 1 is at frame 30 (oldest active reader).
        ckpt.a_read_mark[1] = 30;

        // Checkpoint should not overwrite frames <= min active reader mark.
        let min_mark = ckpt
            .a_read_mark
            .iter()
            .filter(|&&m| m > 0)
            .copied()
            .min()
            .unwrap_or(0);
        assert_eq!(
            min_mark, 30,
            "checkpoint limit should be oldest reader mark"
        );

        // After all readers close (marks zeroed), checkpoint can proceed fully.
        ckpt.a_read_mark = [0; WAL_READ_MARK_COUNT];
        let min_mark_after = ckpt
            .a_read_mark
            .iter()
            .filter(|&&m| m > 0)
            .copied()
            .min()
            .unwrap_or(0);
        assert_eq!(min_mark_after, 0, "no active readers = no checkpoint limit");
    }

    #[test]
    fn test_lock_slot_mapping() {
        // Verify lock slot constants match the spec layout.
        assert_eq!(WAL_WRITE_LOCK, 0, "aLock[0] = WAL_WRITE_LOCK");
        assert_eq!(WAL_CKPT_LOCK, 1, "aLock[1] = WAL_CKPT_LOCK");
        assert_eq!(WAL_RECOVER_LOCK, 2, "aLock[2] = WAL_RECOVER_LOCK");
        assert_eq!(WAL_READ_LOCK_BASE, 3, "aLock[3..7] = WAL_READ_LOCK(0..4)");

        // Verify 5 reader locks fit: indices 3, 4, 5, 6, 7.
        for i in 0..5_usize {
            let lock_idx = WAL_READ_LOCK_BASE + i;
            assert!(lock_idx < WAL_LOCK_SLOT_COUNT, "reader lock {i} in bounds");
        }
    }

    #[test]
    fn test_wal_index_header_round_trip() {
        let hdr = WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: 999,
            is_init: 1,
            big_end_cksum: 1,
            sz_page: 8192,
            mx_frame: 500,
            n_page: 200,
            a_frame_cksum: [0xDEAD_BEEF, 0xCAFE_BABE],
            a_salt: [0x1234_5678, 0x9ABC_DEF0],
            a_cksum: [0xFACE_FEED, 0xBEEF_DEAD],
        };
        let bytes = hdr.to_bytes();
        let parsed = WalIndexHdr::from_bytes(&bytes).expect("round-trip parse");
        assert_eq!(parsed, hdr);
    }

    #[test]
    fn test_wal_ckpt_info_round_trip() {
        let ckpt = WalCkptInfo {
            n_backfill: 77,
            a_read_mark: [10, 20, 30, 40, 50],
            a_lock: [0, 1, 0, 1, 1, 0, 0, 1],
            n_backfill_attempted: 80,
            not_used0: 0,
        };
        let bytes = ckpt.to_bytes();
        let parsed = WalCkptInfo::from_bytes(&bytes).expect("round-trip parse");
        assert_eq!(parsed, ckpt);
    }

    #[test]
    fn test_wal_index_iversion() {
        assert_eq!(WAL_INDEX_VERSION, 3_007_000);
        let hdr = WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: 0,
            is_init: 1,
            big_end_cksum: 0,
            sz_page: 4096,
            mx_frame: 0,
            n_page: 0,
            a_frame_cksum: [0; 2],
            a_salt: [0; 2],
            a_cksum: [0; 2],
        };
        let bytes = hdr.to_bytes();
        let parsed = WalIndexHdr::from_bytes(&bytes).expect("parse");
        assert_eq!(parsed.i_version, 3_007_000);
    }

    #[test]
    fn test_wal_index_native_byte_order_header() {
        // SHM fields are native byte order, not big-endian.
        let hdr = WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: 0x0102_0304,
            is_init: 1,
            big_end_cksum: 0,
            sz_page: 4096,
            mx_frame: 0,
            n_page: 0,
            a_frame_cksum: [0; 2],
            a_salt: [0; 2],
            a_cksum: [0; 2],
        };
        let bytes = hdr.to_bytes();
        // iChange at offset 8, native byte order.
        let raw = [bytes[8], bytes[9], bytes[10], bytes[11]];
        assert_eq!(u32::from_ne_bytes(raw), 0x0102_0304);
        // Contrast with big-endian: would be [0x01, 0x02, 0x03, 0x04].
        if cfg!(target_endian = "little") {
            assert_eq!(raw, 0x0102_0304_u32.to_le_bytes());
        }
    }

    #[test]
    fn test_wal_index_segment_physical_layout() {
        // Verify segment layout: page-number array at bytes 0..16384,
        // hash table at bytes 16384..32768 in a 32KB segment.
        assert_eq!(WAL_SHM_PAGE_ARRAY_BYTES, 16_384);
        assert_eq!(WAL_SHM_HASH_BYTES, 16_384);
        assert_eq!(
            WAL_SHM_PAGE_ARRAY_BYTES + WAL_SHM_HASH_BYTES,
            WAL_SHM_SEGMENT_BYTES,
            "page array + hash table = segment size"
        );
    }
}
