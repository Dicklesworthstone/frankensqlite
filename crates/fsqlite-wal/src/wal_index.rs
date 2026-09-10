//! WAL-index hash table primitives.
//!
//! This module implements the SQLite-compatible SHM hash function:
//! `slot = (page_number * 383) & 8191` with linear probing.
//!
//! The constants and layout mirror SQLite's WAL-index design:
//! - 32 KiB SHM segments
//! - 4096 page-number entries + 8192 hash slots
//! - first segment reserves 136 header bytes, leaving 4062 usable entries

use std::sync::atomic::{Ordering, fence};

use fsqlite_error::{FrankenError, Result};
use fsqlite_vfs::ShmRegion;

use crate::checksum::sqlite_wal_checksum;

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
/// Integer fields use **native** byte order except for the salts, whose
/// bytes are copied unchanged from the big-endian WAL header. SHM is not
/// portable across architectures; it is reconstructed from the WAL.
///
/// ```text
/// Offset  Size  Field
///   0       4   iVersion (3007000)
///   4       4   unused
///   8       4   iChange (commit counter)
///  12       1   isInit (1 if initialized)
///  13       1   bigEndCksum (1 if WAL uses big-endian checksums)
///  14       2   szPage (database page size; 1 means 65536)
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
    /// Commit counter, incremented when a write transaction is published.
    pub i_change: u32,
    /// 1 if this header has been initialized.
    pub is_init: u8,
    /// 1 if the WAL uses big-endian checksums.
    pub big_end_cksum: u8,
    /// Database page size, with 65536 encoded as 1.
    pub sz_page: u16,
    /// Highest valid frame index in the WAL (0 = empty WAL).
    pub mx_frame: u32,
    /// Database size in pages.
    pub n_page: u32,
    /// Running WAL frame checksum pair.
    pub a_frame_cksum: [u32; 2],
    /// WAL salt values, serialized in big-endian WAL byte order.
    pub a_salt: [u32; 2],
    /// Header checksum (covers bytes 0..40 of this struct).
    pub a_cksum: [u32; 2],
}

impl WalIndexHdr {
    /// Decode a `WalIndexHdr` without accepting it as a published snapshot.
    ///
    /// Call [`Self::validate`] and compare the two header copies before use.
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
                u32::from_be_bytes(read4(buf, 32)),
                u32::from_be_bytes(read4(buf, 36)),
            ],
            a_cksum: [
                decode_native_u32(read4(buf, 40)),
                decode_native_u32(read4(buf, 44)),
            ],
        })
    }

    /// Serialize the header, preserving the supplied checksum.
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
        buf[32..36].copy_from_slice(&self.a_salt[0].to_be_bytes());
        buf[36..40].copy_from_slice(&self.a_salt[1].to_be_bytes());
        write4(&mut buf, 40, self.a_cksum[0]);
        write4(&mut buf, 44, self.a_cksum[1]);
        buf
    }

    /// Recompute the native-order checksum after changing header fields.
    ///
    /// This does not publish either shared-memory copy or acquire a lock.
    pub fn update_checksum(&mut self) -> Result<()> {
        self.a_cksum = self.computed_checksum()?;
        Ok(())
    }

    fn computed_checksum(&self) -> Result<[u32; 2]> {
        let bytes = self.to_bytes();
        let checksum = sqlite_wal_checksum(&bytes[..40], 0, 0, cfg!(target_endian = "big"))?;
        Ok([checksum.s1, checksum.s2])
    }

    /// Decode the page-size field, including SQLite's 65536-byte sentinel.
    pub fn page_size(&self) -> Result<u32> {
        let size = if self.sz_page == 1 {
            65_536
        } else {
            u32::from(self.sz_page)
        };
        if !(512..=65_536).contains(&size) || !size.is_power_of_two() {
            return Err(FrankenError::WalCorrupt {
                detail: format!("invalid WAL-index page size {}", self.sz_page),
            });
        }
        Ok(size)
    }

    /// Check initialized state, format, page size and the header checksum.
    ///
    /// A valid single copy still needs the ordered dual-copy read protocol
    /// and validation against the WAL generation before it can be consumed.
    pub fn validate(&self) -> Result<()> {
        if self.is_init != 1 || self.i_version != WAL_INDEX_VERSION || self.big_end_cksum > 1 {
            return Err(FrankenError::WalCorrupt {
                detail: "uninitialized or unsupported WAL-index header".to_owned(),
            });
        }
        self.page_size()?;
        if self.a_cksum != self.computed_checksum()? {
            return Err(FrankenError::WalCorrupt {
                detail: "WAL-index header checksum mismatch".to_owned(),
            });
        }
        Ok(())
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

/// Compare two `WalIndexHdr` copies in an already captured buffer.
///
/// The SHM header stores two copies of `WalIndexHdr` at offsets 0..48 and
/// 48..96. Equality alone does not validate initialization or the checksum.
/// A live SHM reader must capture copy 1, issue its VFS barrier, then capture
/// copy 2 before calling this helper.
#[must_use]
pub fn wal_index_hdr_copies_match(buf: &[u8]) -> bool {
    if buf.len() < 2 * WAL_INDEX_HDR_BYTES {
        return false;
    }
    buf[..WAL_INDEX_HDR_BYTES] == buf[WAL_INDEX_HDR_BYTES..2 * WAL_INDEX_HDR_BYTES]
}

/// Parse the full 136-byte SHM header: dual `WalIndexHdr` copies + `WalCkptInfo`.
///
/// Returns `None` if the copies disagree, or if the matching header is not
/// initialized or fails validation. A short buffer is an error. This parses
/// captured bytes; it does not supply the barriers needed for a live read.
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
    if hdr.validate().is_err() {
        return Ok(None);
    }
    let ckpt = WalCkptInfo::from_bytes(&buf[2 * WAL_INDEX_HDR_BYTES..])?;
    Ok(Some((hdr, ckpt)))
}

/// Serialize a complete SHM header into a private buffer.
///
/// This is not a live publication operation: it overwrites checkpoint and
/// reader state and does not perform the VFS barriers or lock acquisition.
/// A live publisher writes only header copy 2, issues a barrier, then writes
/// copy 1, after making the committed hash entries visible.
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

/// Capture a validated live header using copy 1, barrier, then copy 2.
///
/// The caller must retain this fixed-size region's SHM attachment. Each header
/// word is accessed as an aligned native u32, including the packed flags/page
/// size word and the two words containing big-endian salt bytes. No byte slice
/// borrows live shared memory. The fence supplies the barrier used by the native
/// Unix and Windows VFS implementations.
///
/// `None` means recovery or a retry is required. An accepted header still needs
/// WAL-generation validation and revalidation after the reader lock is acquired;
/// this operation alone does not protect a snapshot from checkpoint/reset.
pub fn read_shared_wal_index_header(region: &ShmRegion) -> Result<Option<WalIndexHdr>> {
    validate_shared_segment(region)?;
    let first = read_shared_header_copy(region, 0)?;
    fence(Ordering::SeqCst);
    let second = read_shared_header_copy(region, WAL_INDEX_HDR_BYTES)?;
    if first != second {
        return Ok(None);
    }
    let header = WalIndexHdr::from_bytes(&first)?;
    Ok(header.validate().is_ok().then_some(header))
}

/// Publish only the two live header copies, in copy 2/barrier/copy 1 order.
///
/// The caller owns WAL_WRITE_LOCK, has installed every committed frame/hash
/// entry, and has established the commit's durability or deferred-sync authority.
/// A changed generation additionally requires the reader-reset/recovery gates.
/// Checkpoint watermarks, reader marks and lock bytes are never overwritten.
/// Retain the publication attempt and its locks if this returns an error.
pub fn publish_shared_wal_index_header(region: &ShmRegion, header: &WalIndexHdr) -> Result<()> {
    validate_shared_segment(region)?;
    header.validate()?;
    let bytes = header.to_bytes();
    fence(Ordering::SeqCst);
    write_shared_header_copy(region, WAL_INDEX_HDR_BYTES, &bytes)?;
    fence(Ordering::SeqCst);
    write_shared_header_copy(region, 0, &bytes)
}

fn validate_shared_segment(region: &ShmRegion) -> Result<()> {
    if region.len() != WAL_SHM_SEGMENT_BYTES {
        return Err(FrankenError::WalCorrupt {
            detail: "live WAL-index mappings must retain fixed 32 KiB regions".to_owned(),
        });
    }
    Ok(())
}

fn read_shared_header_copy(region: &ShmRegion, offset: usize) -> Result<[u8; WAL_INDEX_HDR_BYTES]> {
    let mut bytes = [0_u8; WAL_INDEX_HDR_BYTES];
    for (index, word) in bytes.as_chunks_mut::<4>().0.iter_mut().enumerate() {
        word.copy_from_slice(
            &region
                .atomic_load_u32_ne(offset + index * 4, Ordering::Acquire)?
                .to_ne_bytes(),
        );
    }
    Ok(bytes)
}

fn write_shared_header_copy(
    region: &ShmRegion,
    offset: usize,
    bytes: &[u8; WAL_INDEX_HDR_BYTES],
) -> Result<()> {
    for (index, word) in bytes.as_chunks::<4>().0.iter().enumerate() {
        region.atomic_store_u32_ne(
            offset + index * 4,
            u32::from_ne_bytes([word[0], word[1], word[2], word[3]]),
            Ordering::Release,
        )?;
    }
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

/// Location of a one-based WAL frame in the native 32 KiB SHM segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalIndexFrameLocation {
    /// Zero-based SHM region number.
    pub region: u32,
    /// One-based index stored in the region's hash table.
    pub entry: u16,
}

impl WalIndexFrameLocation {
    /// Locate a frame without overflowing at `u32::MAX`.
    pub fn new(frame: u32) -> Result<Self> {
        if frame == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "WAL-index frame numbers start at one".to_owned(),
            });
        }
        let first_capacity = u32::try_from(WAL_SHM_FIRST_USABLE_PAGE_ENTRIES)
            .map_err(|_| FrankenError::internal("WAL-index first capacity exceeds u32"))?;
        let capacity = u32::try_from(WAL_INDEX_PAGE_ARRAY_ENTRIES)
            .map_err(|_| FrankenError::internal("WAL-index capacity exceeds u32"))?;
        let (region, entry) = if frame <= first_capacity {
            (0, frame)
        } else {
            let remaining = frame - first_capacity - 1;
            (1 + remaining / capacity, 1 + remaining % capacity)
        };
        Ok(Self {
            region,
            entry: u16::try_from(entry)
                .map_err(|_| FrankenError::internal("WAL-index entry exceeds u16"))?,
        })
    }

    fn page_offset(self) -> usize {
        let header = if self.region == 0 {
            WAL_SHM_FIRST_HEADER_BYTES
        } else {
            0
        };
        header + (usize::from(self.entry) - 1) * 4
    }
}

fn native_region_base(region: u32) -> Result<u64> {
    if region == 0 {
        return Ok(0);
    }
    Ok(u64::try_from(WAL_SHM_FIRST_USABLE_PAGE_ENTRIES)
        .map_err(|_| FrankenError::internal("WAL-index capacity exceeds u64"))?
        + u64::from(region - 1)
            * u64::try_from(WAL_INDEX_PAGE_ARRAY_ENTRIES)
                .map_err(|_| FrankenError::internal("WAL-index capacity exceeds u64"))?)
}

/// Clear only entries beyond the published prefix in a captured segment.
///
/// The caller owns the write lock and binds `mx_frame` to the same generation.
/// Clearing a prior generation additionally requires the reader-reset gates.
/// Published hash entries and all first-segment header/reader bytes survive.
/// Perform this once before appending a batch, not between its frames.
pub fn clear_native_wal_index_tail(segment: &mut [u8], region: u32, mx_frame: u32) -> Result<()> {
    if segment.len() < WAL_SHM_SEGMENT_BYTES {
        return Err(FrankenError::WalCorrupt {
            detail: "short WAL-index segment during tail cleanup".to_owned(),
        });
    }
    let (offset, capacity) = if region == 0 {
        (
            WAL_SHM_FIRST_HEADER_BYTES,
            WAL_SHM_FIRST_USABLE_PAGE_ENTRIES,
        )
    } else {
        (0, WAL_INDEX_PAGE_ARRAY_ENTRIES)
    };
    let retained = u64::from(mx_frame).saturating_sub(native_region_base(region)?);
    let retained = usize::try_from(retained)
        .map_err(|_| FrankenError::internal("WAL-index prefix exceeds usize"))?
        .min(capacity);
    for slot in segment[WAL_SHM_PAGE_ARRAY_BYTES..WAL_SHM_SEGMENT_BYTES]
        .as_chunks_mut::<2>()
        .0
    {
        if usize::from(u16::from_ne_bytes([slot[0], slot[1]])) > retained {
            slot.fill(0);
        }
    }
    segment[offset + retained * 4..WAL_SHM_PAGE_ARRAY_BYTES].fill(0);
    Ok(())
}

/// Append a frame mapping to the supplied native segment bytes.
///
/// The caller owns the WAL write lock, supplies the region returned by
/// [`WalIndexFrameLocation::new`], and has cleared any unpublished tail before
/// the batch. Existing entries are never overwritten. This only writes the
/// mapping; the caller publishes the committed header after all mappings and
/// WAL writes are complete. It does not supply live-memory atomic access.
pub fn append_native_wal_index_entry(
    segment: &mut [u8],
    frame: u32,
    page_number: u32,
) -> Result<()> {
    if segment.len() < WAL_SHM_SEGMENT_BYTES || page_number == 0 {
        return Err(FrankenError::WalCorrupt {
            detail: "short WAL-index segment or zero page number".to_owned(),
        });
    }
    let location = WalIndexFrameLocation::new(frame)?;
    let page_offset = location.page_offset();
    if decode_native_u32(read4(segment, page_offset)) != 0 {
        return Err(FrankenError::WalCorrupt {
            detail: "WAL-index append would overwrite an existing mapping".to_owned(),
        });
    }
    if location.entry > 1 && decode_native_u32(read4(segment, page_offset - 4)) == 0 {
        return Err(FrankenError::WalCorrupt {
            detail: "WAL-index append would leave a frame gap".to_owned(),
        });
    }
    let mut slot = usize::try_from(wal_index_hash_slot(page_number))
        .map_err(|_| FrankenError::internal("WAL-index hash slot exceeds usize"))?;
    for _ in 0..WAL_INDEX_HASH_SLOTS {
        let offset = WAL_SHM_PAGE_ARRAY_BYTES + slot * 2;
        if u16::from_ne_bytes([segment[offset], segment[offset + 1]]) == 0 {
            write4(segment, page_offset, page_number);
            segment[offset..offset + 2].copy_from_slice(&location.entry.to_ne_bytes());
            return Ok(());
        }
        slot = (slot + 1) % WAL_INDEX_HASH_SLOTS;
    }
    Err(FrankenError::WalCorrupt {
        detail: "WAL-index hash table has no empty slot".to_owned(),
    })
}

/// Clear unpublished mappings in a live region using fixed-width atomics.
///
/// The write-lock and generation/reader-gate requirements are the same as
/// [`clear_native_wal_index_tail`]. Published page and hash entries are not
/// rewritten, so readers retaining that prefix continue to use it.
pub fn clear_shared_wal_index_tail(segment: &ShmRegion, region: u32, mx_frame: u32) -> Result<()> {
    validate_shared_segment(segment)?;
    let (offset, capacity) = if region == 0 {
        (
            WAL_SHM_FIRST_HEADER_BYTES,
            WAL_SHM_FIRST_USABLE_PAGE_ENTRIES,
        )
    } else {
        (0, WAL_INDEX_PAGE_ARRAY_ENTRIES)
    };
    let retained = u64::from(mx_frame).saturating_sub(native_region_base(region)?);
    let retained = usize::try_from(retained)
        .map_err(|_| FrankenError::internal("WAL-index prefix exceeds usize"))?
        .min(capacity);
    for slot in 0..WAL_INDEX_HASH_SLOTS {
        let hash_offset = WAL_SHM_PAGE_ARRAY_BYTES + slot * 2;
        if usize::from(segment.atomic_load_u16_ne(hash_offset, Ordering::Acquire)?) > retained {
            segment.atomic_store_u16_ne(hash_offset, 0, Ordering::Release)?;
        }
    }
    for entry in retained..capacity {
        segment.atomic_store_u32_ne(offset + entry * 4, 0, Ordering::Release)?;
    }
    Ok(())
}

/// Install one mapping in a live region without publishing a commit header.
///
/// The caller owns WAL_WRITE_LOCK and supplies the region selected by
/// [`WalIndexFrameLocation::new`]. Clear any unpublished tail once before the
/// batch. Each page entry is published before its hash slot; the eventual
/// committed header is published only after the entire batch is installed.
pub fn append_shared_wal_index_entry(
    segment: &ShmRegion,
    frame: u32,
    page_number: u32,
) -> Result<()> {
    validate_shared_segment(segment)?;
    if page_number == 0 {
        return Err(FrankenError::WalCorrupt {
            detail: "zero WAL-index page number".to_owned(),
        });
    }
    let location = WalIndexFrameLocation::new(frame)?;
    let page_offset = location.page_offset();
    if segment.atomic_load_u32_ne(page_offset, Ordering::Acquire)? != 0 {
        return Err(FrankenError::WalCorrupt {
            detail: "WAL-index append would overwrite an existing mapping".to_owned(),
        });
    }
    if location.entry > 1 && segment.atomic_load_u32_ne(page_offset - 4, Ordering::Acquire)? == 0 {
        return Err(FrankenError::WalCorrupt {
            detail: "WAL-index append would leave a frame gap".to_owned(),
        });
    }
    let mut slot = usize::try_from(wal_index_hash_slot(page_number))
        .map_err(|_| FrankenError::internal("WAL-index hash slot exceeds usize"))?;
    for _ in 0..WAL_INDEX_HASH_SLOTS {
        let hash_offset = WAL_SHM_PAGE_ARRAY_BYTES + slot * 2;
        if segment.atomic_load_u16_ne(hash_offset, Ordering::Acquire)? == 0 {
            segment.atomic_store_u32_ne(page_offset, page_number, Ordering::Release)?;
            segment.atomic_store_u16_ne(hash_offset, location.entry, Ordering::Release)?;
            return Ok(());
        }
        slot = (slot + 1) % WAL_INDEX_HASH_SLOTS;
    }
    Err(FrankenError::WalCorrupt {
        detail: "WAL-index hash table has no empty slot".to_owned(),
    })
}

/// Find the latest mapping at or before a reader's committed frame horizon.
///
/// The caller supplies a stable snapshot of one region and retains its reader
/// lock. Later entries, including out-of-range indices beyond the horizon,
/// cannot select a frame for the reader. Search regions newest to oldest.
pub fn lookup_native_wal_index_frame(
    segment: &[u8],
    region: u32,
    page_number: u32,
    mx_frame: u32,
) -> Result<Option<u32>> {
    if segment.len() < WAL_SHM_SEGMENT_BYTES || page_number == 0 {
        return Err(FrankenError::WalCorrupt {
            detail: "short WAL-index segment or zero lookup page".to_owned(),
        });
    }
    let base = native_region_base(region)?;
    if base >= u64::from(mx_frame) {
        return Ok(None);
    }
    let capacity = if region == 0 {
        WAL_SHM_FIRST_USABLE_PAGE_ENTRIES
    } else {
        WAL_INDEX_PAGE_ARRAY_ENTRIES
    };
    let mut found = None;
    let mut slot = usize::try_from(wal_index_hash_slot(page_number))
        .map_err(|_| FrankenError::internal("WAL-index hash slot exceeds usize"))?;
    for _ in 0..WAL_INDEX_HASH_SLOTS {
        let offset = WAL_SHM_PAGE_ARRAY_BYTES + slot * 2;
        let entry = u16::from_ne_bytes([segment[offset], segment[offset + 1]]);
        if entry == 0 {
            return Ok(found);
        }
        let frame = base + u64::from(entry);
        if frame <= u64::from(mx_frame) {
            if usize::from(entry) > capacity {
                return Err(FrankenError::WalCorrupt {
                    detail: "published WAL-index entry exceeds segment capacity".to_owned(),
                });
            }
            let location = WalIndexFrameLocation { region, entry };
            if decode_native_u32(read4(segment, location.page_offset())) == page_number {
                let frame = u32::try_from(frame)
                    .map_err(|_| FrankenError::internal("published WAL frame exceeds u32"))?;
                found = Some(found.map_or(frame, |previous: u32| previous.max(frame)));
            }
        }
        slot = (slot + 1) % WAL_INDEX_HASH_SLOTS;
    }
    Err(FrankenError::WalCorrupt {
        detail: "WAL-index lookup encountered a full hash table".to_owned(),
    })
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
    /// Every appended frame gets a unique slot in the hash table, even if the same
    /// page already exists. This allows readers to find the most recent version
    /// of a page that was committed prior to their read mark.
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
        let mut best: Option<WalHashLookup> = None;

        loop {
            let slot_usize = usize::try_from(slot).expect("hash slot must fit usize");
            let one_based = self.hash_slots[slot_usize];
            if one_based == 0 {
                break;
            }

            let idx = usize::from(one_based - 1);
            if self.page_numbers[idx] == page_number {
                if let Some(ref b) = best {
                    if one_based > b.one_based_index {
                        best = Some(WalHashLookup {
                            slot,
                            one_based_index: one_based,
                            page_number,
                        });
                    }
                } else {
                    best = Some(WalHashLookup {
                        slot,
                        one_based_index: one_based,
                        page_number,
                    });
                }
            }

            slot = (slot + 1) & WAL_INDEX_HASH_MASK;
            if slot == start_slot {
                break;
            }
        }

        best
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

    fn shared_header_fixture(page_size: u16, change: u32) -> WalIndexHdr {
        let mut header = WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: change,
            is_init: 1,
            big_end_cksum: 1,
            sz_page: page_size,
            mx_frame: 4063 + change,
            n_page: 19 + change,
            a_frame_cksum: [0x0123_4567, 0x89ab_cdef],
            a_salt: [0x1357_9bdf, 0x2468_ace0],
            a_cksum: [0; 2],
        };
        header.update_checksum().unwrap();
        header
    }

    #[test]
    fn test_shared_header_preserves_reader_state_and_refuses_torn_copies() {
        for page_size in [512, 4096, 1] {
            let region = ShmRegion::from_vec(vec![0xa7; WAL_SHM_SEGMENT_BYTES]);
            let header = shared_header_fixture(page_size, 7);
            publish_shared_wal_index_header(&region, &header).unwrap();
            assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(header));
            let captured = region.lock().to_vec();
            assert_eq!(&captured[..48], &header.to_bytes());
            assert_eq!(&captured[48..96], &header.to_bytes());
            assert!(captured[96..].iter().all(|byte| *byte == 0xa7));

            let mut invalid = header;
            invalid.mx_frame += 1;
            assert!(publish_shared_wal_index_header(&region, &invalid).is_err());
            assert_eq!(region.lock().to_vec(), captured);

            region
                .atomic_store_u32_ne(56, 8, Ordering::Release)
                .unwrap();
            assert!(read_shared_wal_index_header(&region).unwrap().is_none());
            region.atomic_store_u32_ne(8, 8, Ordering::Release).unwrap();
            assert!(read_shared_wal_index_header(&region).unwrap().is_none());
            publish_shared_wal_index_header(&region, &header).unwrap();
            assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(header));
        }
        for size in [0, 96, WAL_SHM_SEGMENT_BYTES - 1, WAL_SHM_SEGMENT_BYTES + 1] {
            let region = ShmRegion::new(size);
            assert!(read_shared_wal_index_header(&region).is_err());
            assert!(
                publish_shared_wal_index_header(&region, &shared_header_fixture(4096, 0)).is_err()
            );
            assert!(region.lock().iter().all(|byte| *byte == 0));
        }
    }

    #[test]
    fn test_shared_header_concurrent_capture_never_accepts_a_mixed_header() {
        let first = shared_header_fixture(4096, 1);
        let second = shared_header_fixture(1, 2);
        let region = ShmRegion::new(WAL_SHM_SEGMENT_BYTES);
        publish_shared_wal_index_header(&region, &first).unwrap();
        let writer_region = region.share();
        let start = std::sync::Arc::new(std::sync::Barrier::new(2));
        let writer_start = std::sync::Arc::clone(&start);
        let writer = std::thread::spawn(move || {
            writer_start.wait();
            for index in 0..1000 {
                let header = if index % 2 == 0 { &second } else { &first };
                publish_shared_wal_index_header(&writer_region, header).unwrap();
            }
        });
        start.wait();
        for _ in 0..2000 {
            if let Some(header) = read_shared_wal_index_header(&region).unwrap() {
                assert!(
                    header == first || header == second,
                    "mixed header: {header:?}"
                );
            }
        }
        writer.join().unwrap();
        assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(first));
    }

    #[test]
    fn test_shared_index_atomic_mapping_matches_captured_layout_and_tail_cleanup() {
        let mut captured = [
            vec![0; WAL_SHM_SEGMENT_BYTES],
            vec![0; WAL_SHM_SEGMENT_BYTES],
        ];
        captured[0][..WAL_SHM_FIRST_HEADER_BYTES].fill(0x5a);
        let shared = [
            ShmRegion::from_vec(captured[0].clone()),
            ShmRegion::new(WAL_SHM_SEGMENT_BYTES),
        ];
        for frame in 1..=4100 {
            let location = WalIndexFrameLocation::new(frame).unwrap();
            let region = usize::try_from(location.region).unwrap();
            let page = if frame <= 3 {
                7 + (frame - 1) * 8192
            } else {
                frame
            };
            append_native_wal_index_entry(&mut captured[region], frame, page).unwrap();
            append_shared_wal_index_entry(&shared[region], frame, page).unwrap();
        }
        for region in 0..2 {
            assert_eq!(shared[region].lock().to_vec(), captured[region]);
            clear_native_wal_index_tail(
                &mut captured[region],
                u32::try_from(region).unwrap(),
                4060,
            )
            .unwrap();
            clear_shared_wal_index_tail(&shared[region], u32::try_from(region).unwrap(), 4060)
                .unwrap();
            assert_eq!(shared[region].lock().to_vec(), captured[region]);
        }
        append_shared_wal_index_entry(&shared[0], 4061, 16391).unwrap();
        append_native_wal_index_entry(&mut captured[0], 4061, 16391).unwrap();
        assert_eq!(shared[0].lock().to_vec(), captured[0]);
        let before = shared[0].lock().to_vec();
        assert!(append_shared_wal_index_entry(&shared[0], 4061, 5).is_err());
        assert!(append_shared_wal_index_entry(&shared[0], 4062, 0).is_err());
        assert!(append_shared_wal_index_entry(&shared[1], 4064, 5).is_err());
        assert_eq!(shared[0].lock().to_vec(), before);
        assert!(shared[1].lock().iter().all(|byte| *byte == 0));
    }

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
    fn test_native_frame_location_boundaries() {
        for (frame, region, entry) in [
            (1, 0, 1),
            (4062, 0, 4062),
            (4063, 1, 1),
            (8158, 1, 4096),
            (8159, 2, 1),
            (u32::MAX, 1_048_576, 33),
        ] {
            assert_eq!(
                WalIndexFrameLocation::new(frame).expect("location"),
                WalIndexFrameLocation { region, entry }
            );
        }
        assert!(WalIndexFrameLocation::new(0).is_err());
    }

    #[test]
    fn test_native_hash_collision_preserves_reader_horizon() {
        let mut bytes = vec![0; WAL_SHM_SEGMENT_BYTES];
        bytes[..WAL_SHM_FIRST_HEADER_BYTES].fill(0xa5);
        append_native_wal_index_entry(&mut bytes, 1, 7).expect("first frame");
        append_native_wal_index_entry(&mut bytes, 2, 8199).expect("collision");
        append_native_wal_index_entry(&mut bytes, 3, 7).expect("new page version");
        assert_eq!(
            lookup_native_wal_index_frame(&bytes, 0, 7, 1).expect("old reader"),
            Some(1)
        );
        assert_eq!(
            lookup_native_wal_index_frame(&bytes, 0, 7, 2).expect("old reader"),
            Some(1)
        );
        assert_eq!(
            lookup_native_wal_index_frame(&bytes, 0, 7, 3).expect("new reader"),
            Some(3)
        );
        assert_eq!(
            lookup_native_wal_index_frame(&bytes, 0, 8199, 3).expect("colliding page"),
            Some(2)
        );
        assert_eq!(
            lookup_native_wal_index_frame(&bytes, 0, 16391, 3).expect("missing collision"),
            None
        );
        assert!(
            bytes[..WAL_SHM_FIRST_HEADER_BYTES]
                .iter()
                .all(|&b| b == 0xa5)
        );

        let before = bytes.clone();
        assert!(append_native_wal_index_entry(&mut bytes, 2, 8).is_err());
        assert!(append_native_wal_index_entry(&mut bytes, 5, 8).is_err());
        assert!(append_native_wal_index_entry(&mut bytes, 4, 0).is_err());
        assert_eq!(bytes, before, "refusals preserve published mappings");

        // A future writer's uncommitted hash entry cannot select a frame
        // for the old reader, even when its index would exceed the array.
        let slot = usize::try_from(wal_index_hash_slot(7)).expect("slot") + 3;
        let offset = WAL_SHM_PAGE_ARRAY_BYTES + slot * 2;
        bytes[offset..offset + 2].copy_from_slice(&u16::MAX.to_ne_bytes());
        assert_eq!(
            lookup_native_wal_index_frame(&bytes, 0, 7, 3).expect("ignore future entry"),
            Some(3)
        );
        assert!(lookup_native_wal_index_frame(&bytes, 0, 7, u32::MAX).is_err());
    }

    #[test]
    fn test_native_hash_full_table_and_short_segment_refuse_without_mutation() {
        let mut full = vec![0; WAL_SHM_SEGMENT_BYTES];
        full[WAL_SHM_PAGE_ARRAY_BYTES..].fill(0xff);
        let before = full.clone();
        assert!(append_native_wal_index_entry(&mut full, 1, 7).is_err());
        assert_eq!(full, before);
        assert!(lookup_native_wal_index_frame(&full, 0, 7, 1).is_err());
        let mut short = vec![0; WAL_SHM_SEGMENT_BYTES - 1];
        assert!(append_native_wal_index_entry(&mut short, 1, 7).is_err());
        assert!(short.iter().all(|&byte| byte == 0));
        assert!(lookup_native_wal_index_frame(&short, 0, 7, 1).is_err());
    }

    #[test]
    fn test_native_tail_cleanup_preserves_committed_collision_prefix() {
        let mut bytes = vec![0; WAL_SHM_SEGMENT_BYTES];
        bytes[..WAL_SHM_FIRST_HEADER_BYTES].fill(0xa5);
        append_native_wal_index_entry(&mut bytes, 1, 7).expect("first committed frame");
        append_native_wal_index_entry(&mut bytes, 2, 8199).expect("second committed frame");
        let committed = bytes.clone();
        append_native_wal_index_entry(&mut bytes, 3, 7).expect("unpublished tail");
        append_native_wal_index_entry(&mut bytes, 4, 16391).expect("unpublished collision");
        clear_native_wal_index_tail(&mut bytes, 0, 2).expect("remove only tail");
        assert_eq!(bytes, committed, "committed bytes must survive unchanged");
        clear_native_wal_index_tail(&mut bytes, 0, 2).expect("idempotent cleanup");
        assert_eq!(bytes, committed);
        append_native_wal_index_entry(&mut bytes, 3, 16391).expect("replacement tail");
        assert_eq!(
            lookup_native_wal_index_frame(&bytes, 0, 7, 3).expect("old page survives"),
            Some(1)
        );
        assert_eq!(
            lookup_native_wal_index_frame(&bytes, 0, 16391, 3).expect("replacement visible"),
            Some(3)
        );
        clear_native_wal_index_tail(&mut bytes, 0, 0).expect("empty generation");
        assert!(
            bytes[..WAL_SHM_FIRST_HEADER_BYTES]
                .iter()
                .all(|&b| b == 0xa5)
        );
        assert!(bytes[WAL_SHM_FIRST_HEADER_BYTES..].iter().all(|&b| b == 0));

        let mut next = vec![0; WAL_SHM_SEGMENT_BYTES];
        append_native_wal_index_entry(&mut next, 4063, 7).expect("second segment");
        clear_native_wal_index_tail(&mut next, 1, 4062).expect("tail-only segment");
        assert!(next.iter().all(|&byte| byte == 0));
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
        let mut hdr = WalIndexHdr {
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
        hdr.update_checksum().expect("header checksum");
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
        // The commit counter uses native byte order, unlike the WAL salts.
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
    fn test_wal_index_salts_keep_wal_byte_order() {
        let mut bytes = [0_u8; WAL_INDEX_HDR_BYTES];
        bytes[32..40].copy_from_slice(&[0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0]);
        let header = WalIndexHdr::from_bytes(&bytes).expect("decode WAL salts");
        assert_eq!(header.a_salt, [0x1234_5678, 0x9abc_def0]);
        assert_eq!(&header.to_bytes()[32..40], &bytes[32..40]);
    }

    #[test]
    fn test_shm_header_rejects_matching_corruption() {
        let mut header = WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: 7,
            is_init: 1,
            big_end_cksum: 0,
            sz_page: 4096,
            mx_frame: 9,
            n_page: 3,
            a_frame_cksum: [0x1234_5678, 0x8765_4321],
            a_salt: [0x1122_3344, 0x5566_7788],
            a_cksum: [0; 2],
        };
        header.update_checksum().expect("checksum");
        header.validate().expect("valid header");
        for wal_checksum_order in [0, 1] {
            let mut candidate = header;
            candidate.big_end_cksum = wal_checksum_order;
            candidate.update_checksum().expect("native header checksum");
            candidate
                .validate()
                .expect("either WAL checksum order is valid");
        }
        let ckpt = WalCkptInfo {
            n_backfill: 0,
            a_read_mark: [0, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            a_lock: [0; WAL_LOCK_SLOT_COUNT],
            n_backfill_attempted: 0,
            not_used0: 0,
        };
        let mut valid = [0; WAL_SHM_FIRST_HEADER_BYTES];
        write_shm_header(&mut valid, &header, &ckpt).expect("serialize");
        assert!(parse_shm_header(&valid).expect("parse").is_some());

        for offset in 0..WAL_INDEX_HDR_BYTES {
            let mut damaged = valid;
            damaged[offset] ^= 1;
            damaged[WAL_INDEX_HDR_BYTES + offset] ^= 1;
            assert!(wal_index_hdr_copies_match(&damaged));
            assert!(
                parse_shm_header(&damaged).expect("parse").is_none(),
                "matching copies with corrupted byte {offset} must be rejected"
            );
        }
        assert!(
            parse_shm_header(&[0; WAL_SHM_FIRST_HEADER_BYTES])
                .expect("parse zeroed header")
                .is_none()
        );

        // A matching checksum does not make an unsupported format usable.
        for (version, initialized, endian, page_size) in [
            (WAL_INDEX_VERSION + 1, 1, 0, 4096),
            (WAL_INDEX_VERSION, 0, 0, 4096),
            (WAL_INDEX_VERSION, 1, 2, 4096),
            (WAL_INDEX_VERSION, 1, 0, 0),
            (WAL_INDEX_VERSION, 1, 0, 513),
        ] {
            let mut invalid = header;
            invalid.i_version = version;
            invalid.is_init = initialized;
            invalid.big_end_cksum = endian;
            invalid.sz_page = page_size;
            invalid.update_checksum().expect("checksum");
            assert!(invalid.validate().is_err());
            write_shm_header(&mut valid, &invalid, &ckpt).expect("serialize");
            assert!(parse_shm_header(&valid).expect("parse").is_none());
        }
    }

    #[test]
    fn test_wal_index_page_size_65536_sentinel() {
        let mut header = WalIndexHdr::from_bytes(&[0; WAL_INDEX_HDR_BYTES]).expect("decode");
        header.sz_page = 1;
        assert_eq!(header.page_size().expect("sentinel"), 65_536);
        for shift in 9..=15 {
            header.sz_page = 1 << shift;
            assert_eq!(header.page_size().expect("power of two"), 1 << shift);
        }
        for invalid in [0, 2, 256, 513, u16::MAX] {
            header.sz_page = invalid;
            assert!(header.page_size().is_err(), "invalid page size {invalid}");
        }
    }

    #[test]
    fn test_wal_index_hdr_from_bytes_too_short() {
        let buf = [0u8; WAL_INDEX_HDR_BYTES - 1];
        let err = WalIndexHdr::from_bytes(&buf).unwrap_err();
        assert!(err.to_string().contains("too small"));
    }

    #[test]
    fn test_wal_ckpt_info_from_bytes_too_short() {
        let buf = [0u8; WAL_CKPT_INFO_BYTES - 1];
        let err = WalCkptInfo::from_bytes(&buf).unwrap_err();
        assert!(err.to_string().contains("too small"));
    }

    #[test]
    fn test_write_shm_header_too_short_buffer() {
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
        let ckpt = WalCkptInfo {
            n_backfill: 0,
            a_read_mark: [0; WAL_READ_MARK_COUNT],
            a_lock: [0; WAL_LOCK_SLOT_COUNT],
            n_backfill_attempted: 0,
            not_used0: 0,
        };
        let mut buf = [0u8; WAL_SHM_FIRST_HEADER_BYTES - 1];
        let err = write_shm_header(&mut buf, &hdr, &ckpt).unwrap_err();
        assert!(err.to_string().contains("too small"));
    }

    #[test]
    fn test_hash_segment_is_empty_and_len() {
        let mut seg = WalIndexHashSegment::new(WalIndexSegmentKind::Subsequent);
        assert!(seg.is_empty());
        assert_eq!(seg.len(), 0);
        seg.insert(1).unwrap();
        seg.insert(2).unwrap();
        assert!(!seg.is_empty());
        assert_eq!(seg.len(), 2);
    }

    #[test]
    fn test_lookup_missing_page_returns_none() {
        let mut seg = WalIndexHashSegment::new(WalIndexSegmentKind::Subsequent);
        seg.insert(10).unwrap();
        assert!(seg.lookup(10).is_some());
        assert!(seg.lookup(99).is_none());
    }

    #[test]
    fn test_duplicate_page_insert_returns_latest() {
        let mut seg = WalIndexHashSegment::new(WalIndexSegmentKind::Subsequent);
        seg.insert(42).unwrap();
        seg.insert(42).unwrap();
        let result = seg.lookup(42).expect("should find page");
        assert_eq!(result.one_based_index, 2, "lookup returns latest entry");
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

    #[test]
    fn test_wal_ckpt_info_to_bytes_roundtrip() {
        let ckpt = WalCkptInfo {
            n_backfill: 42,
            a_read_mark: [1, 2, 3, 4, 5],
            a_lock: [0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80],
            n_backfill_attempted: 99,
            not_used0: 0,
        };
        let bytes = ckpt.to_bytes();
        let parsed = WalCkptInfo::from_bytes(&bytes).unwrap();
        assert_eq!(parsed, ckpt);
    }

    #[test]
    fn test_wal_index_hdr_copies_match_mismatch() {
        let mut buf = [0u8; 2 * WAL_INDEX_HDR_BYTES];
        buf[..WAL_INDEX_HDR_BYTES].fill(0xAA);
        buf[WAL_INDEX_HDR_BYTES..].fill(0xBB);
        assert!(!wal_index_hdr_copies_match(&buf));

        let (first, second) = buf.split_at_mut(WAL_INDEX_HDR_BYTES);
        second.copy_from_slice(first);
        assert!(wal_index_hdr_copies_match(&buf));

        assert!(!wal_index_hdr_copies_match(&[0u8; WAL_INDEX_HDR_BYTES - 1]));
    }

    #[test]
    fn test_parse_write_shm_header_roundtrip() {
        let mut hdr = WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: 7,
            is_init: 1,
            big_end_cksum: 0,
            sz_page: 4096,
            mx_frame: 100,
            n_page: 50,
            a_frame_cksum: [0x1234, 0x5678],
            a_salt: [0xAAAA, 0xBBBB],
            a_cksum: [0xCCCC, 0xDDDD],
        };
        hdr.update_checksum().expect("header checksum");
        let ckpt = WalCkptInfo {
            n_backfill: 10,
            a_read_mark: [0, 5, 10, 15, 20],
            a_lock: [0; WAL_LOCK_SLOT_COUNT],
            n_backfill_attempted: 10,
            not_used0: 0,
        };
        let mut buf = [0u8; WAL_SHM_FIRST_HEADER_BYTES];
        write_shm_header(&mut buf, &hdr, &ckpt).unwrap();
        let (parsed_hdr, parsed_ckpt) = parse_shm_header(&buf).unwrap().unwrap();
        assert_eq!(parsed_hdr, hdr);
        assert_eq!(parsed_ckpt, ckpt);
    }

    #[test]
    fn test_first_segment_capacity_less_than_subsequent() {
        let first = WalIndexHashSegment::new(WalIndexSegmentKind::First);
        let sub = WalIndexHashSegment::new(WalIndexSegmentKind::Subsequent);
        assert!(first.capacity() < sub.capacity());
        assert_eq!(first.kind(), WalIndexSegmentKind::First);
        assert_eq!(sub.kind(), WalIndexSegmentKind::Subsequent);
        assert_eq!(sub.capacity(), WAL_SHM_SUBSEQUENT_USABLE_PAGE_ENTRIES);
        assert_eq!(first.capacity(), WAL_SHM_FIRST_USABLE_PAGE_ENTRIES);
    }

    #[test]
    fn test_hash_slots_accessor_reflects_inserts() {
        let mut seg = WalIndexHashSegment::new(WalIndexSegmentKind::Subsequent);
        let slots_before = seg.hash_slots();
        assert!(slots_before.iter().all(|&s| s == 0));

        seg.insert(7).unwrap();
        seg.insert(15).unwrap();
        let slots_after = seg.hash_slots();
        let non_zero: usize = slots_after.iter().filter(|&&s| s != 0).count();
        assert_eq!(non_zero, 2);

        let slot_7 = usize::try_from(wal_index_hash_slot(7)).unwrap();
        assert_eq!(
            slots_after[slot_7], 1,
            "page 7 is first entry → one-based 1"
        );
        let slot_15 = usize::try_from(wal_index_hash_slot(15)).unwrap();
        assert_eq!(
            slots_after[slot_15], 2,
            "page 15 is second entry → one-based 2"
        );
    }

    #[test]
    fn test_parse_shm_header_too_short_returns_error() {
        let buf = [0u8; WAL_SHM_FIRST_HEADER_BYTES - 1];
        let err = parse_shm_header(&buf).unwrap_err();
        assert!(err.to_string().contains("too small"));
    }

    #[test]
    fn test_from_bytes_accepts_oversized_buffers() {
        let hdr = WalIndexHdr {
            i_version: WAL_INDEX_VERSION,
            unused: 0,
            i_change: 55,
            is_init: 1,
            big_end_cksum: 0,
            sz_page: 4096,
            mx_frame: 10,
            n_page: 5,
            a_frame_cksum: [111, 222],
            a_salt: [333, 444],
            a_cksum: [555, 666],
        };
        let small = hdr.to_bytes();
        let mut big = [0xFFu8; 128];
        big[..WAL_INDEX_HDR_BYTES].copy_from_slice(&small);
        let parsed = WalIndexHdr::from_bytes(&big).unwrap();
        assert_eq!(parsed, hdr);

        let ckpt = WalCkptInfo {
            n_backfill: 9,
            a_read_mark: [1, 2, 3, 4, 5],
            a_lock: [0; WAL_LOCK_SLOT_COUNT],
            n_backfill_attempted: 12,
            not_used0: 0,
        };
        let small_ckpt = ckpt.to_bytes();
        let mut big_ckpt = [0xFFu8; 128];
        big_ckpt[..WAL_CKPT_INFO_BYTES].copy_from_slice(&small_ckpt);
        let parsed_ckpt = WalCkptInfo::from_bytes(&big_ckpt).unwrap();
        assert_eq!(parsed_ckpt, ckpt);
    }

    #[test]
    fn test_wal_hash_lookup_fields_and_derives() {
        let a = WalHashLookup {
            slot: 42,
            one_based_index: 7,
            page_number: 100,
        };
        let b = a;
        assert_eq!(a, b);

        let c = WalHashLookup {
            slot: 42,
            one_based_index: 8,
            page_number: 100,
        };
        assert_ne!(a, c);

        assert_eq!(a.slot, 42);
        assert_eq!(a.one_based_index, 7);
        assert_eq!(a.page_number, 100);

        let dbg = format!("{a:?}");
        assert!(dbg.contains("WalHashLookup"));
    }
}
