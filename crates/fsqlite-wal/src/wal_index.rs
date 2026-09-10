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
use crate::{WalFrameHeader, WalGenerationIdentity, WalHeader};

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

/// Read the durable backfill watermark for one exact accepted publication.
///
/// The caller retains its checkpoint/reader owner. A changed header or an
/// impossible attempted/completed interval requires recovery, not clamping.
pub fn read_shared_wal_index_backfill(
    region: &ShmRegion,
    expected_header: &WalIndexHdr,
) -> Result<u32> {
    read_shared_wal_index_backfill_state(region, expected_header).map(|(backfill, _)| backfill)
}

fn read_shared_wal_index_backfill_state(
    region: &ShmRegion,
    expected_header: &WalIndexHdr,
) -> Result<(u32, u32)> {
    validate_shared_segment(region)?;
    expected_header.validate()?;
    if read_shared_wal_index_header(region)? != Some(*expected_header) {
        return Err(FrankenError::BusyRecovery);
    }
    let backfill = region.atomic_load_u32_ne(96, Ordering::Acquire)?;
    let attempted = region.atomic_load_u32_ne(128, Ordering::Acquire)?;
    fence(Ordering::SeqCst);
    if read_shared_wal_index_header(region)? != Some(*expected_header)
        || backfill > attempted
        || attempted > expected_header.mx_frame
    {
        return Err(FrankenError::BusyRecovery);
    }
    Ok((backfill, attempted))
}

/// Publish cumulative backfill only after the caller has synced the database.
///
/// The caller retains WRITE/CKPT and the required backfill reader gate through
/// validation and publication. Attempted precedes completed; every header,
/// reader, lock, reserved and mapping byte remains untouched. On error retain
/// the owner and retry the same cumulative watermark after durability is known.
pub fn publish_shared_wal_index_backfill(
    region: &ShmRegion,
    expected_header: &WalIndexHdr,
    cumulative: u32,
) -> Result<()> {
    let (backfill, attempted) = read_shared_wal_index_backfill_state(region, expected_header)?;
    if cumulative < backfill || cumulative > expected_header.mx_frame {
        return Err(FrankenError::BusyRecovery);
    }
    region.atomic_store_u32_ne(128, attempted.max(cumulative), Ordering::Release)?;
    fence(Ordering::SeqCst);
    region.atomic_store_u32_ne(96, cumulative, Ordering::Release)
}

/// Bind a shared publication to the exact WAL header and committed marker.
///
/// The marker's frame number is one-based and must identify `mx_frame`.
/// Callers read that exact frame while retaining their external lock owner;
/// this validates metadata, not durability or lock ownership. An empty
/// publication has no terminal frame checksum to compare.
pub fn validate_shared_wal_index_wal_binding(
    header: &WalIndexHdr,
    wal_header: &WalHeader,
    terminal: Option<(u32, WalFrameHeader)>,
) -> Result<()> {
    header.validate()?;
    if wal_header.format_version != crate::WAL_FORMAT_VERSION
        || !matches!(wal_header.magic, crate::WAL_MAGIC_BE | crate::WAL_MAGIC_LE)
        || header.page_size()? != wal_header.page_size
        || header.big_end_cksum != u8::from(wal_header.big_endian_checksum())
        || header.a_salt != [wal_header.salts.salt1, wal_header.salts.salt2]
    {
        return Err(FrankenError::WalCorrupt {
            detail: "shared WAL-index header does not match the WAL generation format".to_owned(),
        });
    }
    match (header.mx_frame, terminal) {
        (0, None) => Ok(()),
        (frame, Some((number, marker)))
            if frame != 0
                && number == frame
                && marker.is_commit()
                && marker.salts == wal_header.salts
                && marker.db_size == header.n_page
                && [marker.checksum.s1, marker.checksum.s2] == header.a_frame_cksum =>
        {
            Ok(())
        }
        _ => Err(FrankenError::WalCorrupt {
            detail: "shared WAL-index terminal marker does not match its publication".to_owned(),
        }),
    }
}

/// Page-source bounds sampled while the caller owns a native reader slot.
///
/// The caller must separately validate the header against the WAL generation
/// and terminal committed frame. These bounds remain valid only while that
/// exact reader-slot claim is retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalIndexReadBoundary {
    /// Frames through this boundary may be read from the database file.
    pub backfilled_frames: u32,
    /// Last frame admitted by the validated shared publication header.
    pub maximum_wal_frame: u32,
    /// Slot zero protects a database-only image; every WAL lookup is bypassed.
    pub database_only: bool,
}

/// Read one native reader mark without borrowing live shared bytes.
pub fn read_shared_wal_index_read_mark(region: &ShmRegion, slot: u32) -> Result<u32> {
    validate_shared_segment(region)?;
    let slot = usize::try_from(slot).map_err(|_| FrankenError::BusyRecovery)?;
    if slot >= WAL_READ_MARK_COUNT {
        return Err(FrankenError::BusyRecovery);
    }
    region.atomic_load_u32_ne(2 * WAL_INDEX_HDR_BYTES + 4 + slot * 4, Ordering::Acquire)
}

/// Revalidate a captured publication after acquiring its native reader slot.
///
/// Sample `nBackfill` before the barrier and header reread. Otherwise a newer
/// checkpoint could make an older page appear safe in the database while its
/// replacement lies beyond this reader's publication. A changed header or
/// mark asks the caller to release the exact slot and retry before reading any
/// database or WAL page. Slot zero ignores the stored mark and requires a
/// completely backfilled image. This function never acquires or releases locks.
pub fn revalidate_shared_wal_index_reader(
    region: &ShmRegion,
    expected_header: &WalIndexHdr,
    slot: u32,
    expected_read_mark: u32,
) -> Result<Option<WalIndexReadBoundary>> {
    validate_shared_segment(region)?;
    expected_header.validate()?;
    if usize::try_from(slot).map_or(true, |slot| slot >= WAL_READ_MARK_COUNT) {
        return Err(FrankenError::BusyRecovery);
    }
    let backfilled_frames =
        region.atomic_load_u32_ne(2 * WAL_INDEX_HDR_BYTES, Ordering::Acquire)?;
    fence(Ordering::SeqCst);
    if slot != 0
        && (expected_read_mark == u32::MAX
            || expected_read_mark > expected_header.mx_frame
            || read_shared_wal_index_read_mark(region, slot)? != expected_read_mark)
    {
        return Ok(None);
    }
    if read_shared_wal_index_header(region)? != Some(*expected_header)
        || backfilled_frames > expected_header.mx_frame
        || (slot == 0 && backfilled_frames != expected_header.mx_frame)
    {
        return Ok(None);
    }
    Ok(Some(WalIndexReadBoundary {
        backfilled_frames,
        maximum_wal_frame: expected_header.mx_frame,
        database_only: slot == 0,
    }))
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

/// Leave the shared index explicitly unreadable before an exclusive rebuild.
///
/// The caller owns WRITE/CKPT/RECOVER and every reader slot. Invalidation
/// precedes all asynchronous work which can leave a partial rebuilt index.
/// A dropped/failed rebuild may release its locks with this invalid pair;
/// readers must recover again instead of consuming partial mappings.
pub fn invalidate_shared_wal_index_header(region: &ShmRegion) -> Result<()> {
    validate_shared_segment(region)?;
    region.atomic_store_u32_ne(12, 0, Ordering::Release)?;
    region.atomic_store_u32_ne(WAL_INDEX_HDR_BYTES + 12, 0, Ordering::Release)?;
    fence(Ordering::SeqCst);
    Ok(())
}

/// Install one privately prepared recovery segment while its header is invalid.
///
/// The caller retains the canonical recovery owner and never borrows live
/// bytes. Header/checkpoint/lock bytes are excluded; page words and hash slots
/// use their distinct protocol widths. Superseded tails in this segment are
/// replaced by the scratch's zeros, without unmapping or truncating SHM.
pub fn replace_shared_wal_index_region(
    region: &ShmRegion,
    number: u32,
    scratch: &[u8],
) -> Result<()> {
    validate_shared_segment(region)?;
    if scratch.len() != WAL_SHM_SEGMENT_BYTES {
        return Err(FrankenError::WalCorrupt {
            detail: "invalid private recovery segment size".to_owned(),
        });
    }
    if number == 0 && read_shared_wal_index_header(region)?.is_some() {
        return Err(FrankenError::BusyRecovery);
    }
    let page_start = if number == 0 {
        WAL_SHM_FIRST_HEADER_BYTES
    } else {
        0
    };
    for offset in (page_start..WAL_SHM_PAGE_ARRAY_BYTES).step_by(4) {
        region.atomic_store_u32_ne(
            offset,
            u32::from_ne_bytes(scratch[offset..offset + 4].try_into().expect("page word")),
            Ordering::Release,
        )?;
    }
    for offset in (WAL_SHM_PAGE_ARRAY_BYTES..WAL_SHM_SEGMENT_BYTES).step_by(2) {
        region.atomic_store_u16_ne(
            offset,
            u16::from_ne_bytes(scratch[offset..offset + 2].try_into().expect("hash slot")),
            Ordering::Release,
        )?;
    }
    Ok(())
}

/// Reset only checkpoint and reader metadata before recovery header publication.
///
/// Every reader slot, including zero, is exclusively held. Slot one remains
/// usable even for an empty WAL so readonly WAL-dependent consumers can pin
/// that generation without rewriting a mark. No backfill is claimed.
pub fn reset_shared_wal_index_recovery_marks(region: &ShmRegion, maximum_frame: u32) -> Result<()> {
    validate_shared_segment(region)?;
    if read_shared_wal_index_header(region)?.is_some() {
        return Err(FrankenError::BusyRecovery);
    }
    region.atomic_store_u32_ne(96, 0, Ordering::Release)?;
    region.atomic_store_u32_ne(128, 0, Ordering::Release)?;
    region.atomic_store_u32_ne(100, 0, Ordering::Release)?;
    region.atomic_store_u32_ne(104, maximum_frame, Ordering::Release)?;
    for offset in (108..120).step_by(4) {
        region.atomic_store_u32_ne(offset, u32::MAX, Ordering::Release)?;
    }
    fence(Ordering::SeqCst);
    Ok(())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedWalIndexResetPhase {
    Prepared,
    InvalidatingHeaders,
    ResettingMetadata,
    PublishingHeaders,
    HeadersPublished,
}

/// A fixed empty-generation publication owned by one checkpoint reset.
///
/// The caller retains WRITE/CKPT, the backfill reader-zero claim and reset
/// reader slots one through four until this plan completes. Preparation is
/// read-only. Publish only after the exact target WAL header and any truncate
/// are durable. Keep this plan and every gate on error; aliases retain the
/// mapping but do not acquire or represent those physical locks.
pub struct SharedWalIndexResetPlan {
    region: ShmRegion,
    baseline: WalIndexHdr,
    target: WalIndexHdr,
    phase: SharedWalIndexResetPhase,
}

impl SharedWalIndexResetPlan {
    /// Validate an exact fully backfilled baseline and a new empty generation.
    pub fn prepare(region: ShmRegion, baseline: WalIndexHdr, target: WalIndexHdr) -> Result<Self> {
        baseline.validate()?;
        target.validate()?;
        if target.mx_frame != 0
            || target.n_page != 0
            || target.a_frame_cksum != [0, 0]
            || target.i_version != baseline.i_version
            || target.unused != baseline.unused
            || target.i_change != baseline.i_change
            || target.is_init != baseline.is_init
            || target.big_end_cksum != baseline.big_end_cksum
            || target.sz_page != baseline.sz_page
            || target.a_salt == baseline.a_salt
            || read_shared_wal_index_backfill(&region, &baseline)? != baseline.mx_frame
        {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(Self {
            region,
            baseline,
            target,
            phase: SharedWalIndexResetPhase::Prepared,
        })
    }

    #[must_use]
    pub const fn baseline(&self) -> WalIndexHdr {
        self.baseline
    }

    fn invalidated_baseline(&self) -> [u8; WAL_INDEX_HDR_BYTES] {
        let mut bytes = self.baseline.to_bytes();
        bytes[12..16].fill(0);
        bytes
    }

    fn validate_owned_invalidation(&self, fully_invalid: bool) -> Result<()> {
        let first = read_shared_header_copy(&self.region, 0)?;
        fence(Ordering::SeqCst);
        let second = read_shared_header_copy(&self.region, WAL_INDEX_HDR_BYTES)?;
        let baseline = self.baseline.to_bytes();
        let invalid = self.invalidated_baseline();
        if (first == invalid && second == invalid)
            || (!fully_invalid && second == baseline && (first == baseline || first == invalid))
        {
            Ok(())
        } else {
            Err(FrankenError::BusyRecovery)
        }
    }

    fn validate_owned_header_publication(&self) -> Result<()> {
        let first = read_shared_header_copy(&self.region, 0)?;
        fence(Ordering::SeqCst);
        let second = read_shared_header_copy(&self.region, WAL_INDEX_HDR_BYTES)?;
        let invalid = self.invalidated_baseline();
        let target = self.target.to_bytes();
        // Header words are written in ascending order, copy two before one.
        // A torn pair belongs to this plan only if it is one such prefix.
        let is_prefix = |copy: &[u8; WAL_INDEX_HDR_BYTES]| {
            (0..=WAL_INDEX_HDR_BYTES)
                .step_by(4)
                .any(|split| copy[..split] == target[..split] && copy[split..] == invalid[split..])
        };
        if (first == invalid && is_prefix(&second)) || (second == target && is_prefix(&first)) {
            Ok(())
        } else {
            Err(FrankenError::BusyRecovery)
        }
    }

    /// Reset metadata and mappings, then publish both headers last.
    ///
    /// A retry admits only this plan's recorded partial phase or its exact
    /// completed target. An unrelated accepted or torn header is never repaired.
    /// Reset preserves the commit counter because it publishes no transaction.
    pub fn publish(&mut self) -> Result<()> {
        let live = read_shared_wal_index_header(&self.region)?;
        match self.phase {
            SharedWalIndexResetPhase::Prepared => {
                if live != Some(self.baseline)
                    || read_shared_wal_index_backfill(&self.region, &self.baseline)?
                        != self.baseline.mx_frame
                {
                    return Err(FrankenError::BusyRecovery);
                }
                // Retain ownership before the first destructive shared store.
                self.phase = SharedWalIndexResetPhase::InvalidatingHeaders;
            }
            SharedWalIndexResetPhase::InvalidatingHeaders => {
                self.validate_owned_invalidation(false)?;
            }
            SharedWalIndexResetPhase::ResettingMetadata => {
                self.validate_owned_invalidation(true)?;
            }
            SharedWalIndexResetPhase::PublishingHeaders => {
                if live == Some(self.target) {
                    self.phase = SharedWalIndexResetPhase::HeadersPublished;
                    return Ok(());
                }
                self.validate_owned_header_publication()?;
            }
            SharedWalIndexResetPhase::HeadersPublished => {
                return if live == Some(self.target) {
                    Ok(())
                } else {
                    Err(FrankenError::BusyRecovery)
                };
            }
        }
        if self.phase == SharedWalIndexResetPhase::InvalidatingHeaders {
            invalidate_shared_wal_index_header(&self.region)?;
            self.phase = SharedWalIndexResetPhase::ResettingMetadata;
        }
        if self.phase == SharedWalIndexResetPhase::ResettingMetadata {
            reset_shared_wal_index_recovery_marks(&self.region, 0)?;
            clear_shared_wal_index_tail(&self.region, 0, 0)?;
            self.phase = SharedWalIndexResetPhase::PublishingHeaders;
        }
        publish_shared_wal_index_header(&self.region, &self.target)?;
        self.phase = SharedWalIndexResetPhase::HeadersPublished;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedWalPublicationPhase {
    Prepared,
    InstallingMappings,
    PublishingHeaders,
    HeadersPublished,
}

/// Prepared append publication for an already initialized native WAL index.
///
/// The caller retains the same WRITE-lock owner from preparation through
/// publication or exact rejection. Aliases retain mappings; they do not own
/// that lock. Preparation never mutates live index bytes. The plan contains
/// only the unpublished suffix and affected region aliases.
pub struct SharedWalIndexAppendPlan {
    baseline: WalIndexHdr,
    generation: WalGenerationIdentity,
    regions: Vec<(u32, ShmRegion)>,
    entries: Vec<(u32, u32, bool)>,
    target: Option<WalIndexHdr>,
    phase: SharedWalPublicationPhase,
}

impl SharedWalIndexAppendPlan {
    /// Validate a contiguous append on bounded private segment scratch.
    ///
    /// Region zero is always required; every other supplied region must be
    /// ordered and unique. Live page words and hash slots are captured at
    /// their protocol widths, without borrowing concurrently shared bytes.
    pub fn prepare(
        baseline: WalIndexHdr,
        generation: WalGenerationIdentity,
        regions: Vec<(u32, ShmRegion)>,
        entries: Vec<(u32, u32, bool)>,
    ) -> Result<Self> {
        baseline.validate()?;
        if regions.first().is_none_or(|(region, _)| *region != 0)
            || regions.windows(2).any(|pair| pair[0].0 >= pair[1].0)
            || entries.is_empty()
            || baseline.a_salt != [generation.salts.salt1, generation.salts.salt2]
        {
            return Err(FrankenError::BusyRecovery);
        }
        let mut previous = baseline.mx_frame;
        for &(frame, page, _) in &entries {
            if previous.checked_add(1) != Some(frame) || page == 0 {
                return Err(FrankenError::WalCorrupt {
                    detail: "shared WAL-index append has an invalid page or frame gap".to_owned(),
                });
            }
            let region = WalIndexFrameLocation::new(frame)?.region;
            if regions
                .binary_search_by_key(&region, |(number, _)| *number)
                .is_err()
            {
                return Err(FrankenError::WalCorrupt {
                    detail: "shared WAL-index append is missing a mapped region".to_owned(),
                });
            }
            previous = frame;
        }
        if read_shared_wal_index_header(&regions[0].1)? != Some(baseline) {
            return Err(FrankenError::BusyRecovery);
        }
        let mut entry_cursor = 0;
        for (number, region) in &regions {
            validate_shared_segment(region)?;
            let mut scratch = vec![0_u8; WAL_SHM_SEGMENT_BYTES];
            let page_start = if *number == 0 {
                WAL_SHM_FIRST_HEADER_BYTES
            } else {
                0
            };
            for offset in (page_start..WAL_SHM_PAGE_ARRAY_BYTES).step_by(4) {
                scratch[offset..offset + 4].copy_from_slice(
                    &region
                        .atomic_load_u32_ne(offset, Ordering::Acquire)?
                        .to_ne_bytes(),
                );
            }
            for offset in (WAL_SHM_PAGE_ARRAY_BYTES..WAL_SHM_SEGMENT_BYTES).step_by(2) {
                scratch[offset..offset + 2].copy_from_slice(
                    &region
                        .atomic_load_u16_ne(offset, Ordering::Acquire)?
                        .to_ne_bytes(),
                );
            }
            clear_native_wal_index_tail(&mut scratch, *number, baseline.mx_frame)?;
            while let Some(&(frame, page, _)) = entries.get(entry_cursor) {
                if WalIndexFrameLocation::new(frame)?.region != *number {
                    break;
                }
                append_native_wal_index_entry(&mut scratch, frame, page)?;
                entry_cursor += 1;
            }
        }
        if read_shared_wal_index_header(&regions[0].1)? != Some(baseline) {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(Self {
            baseline,
            generation,
            regions,
            entries,
            target: None,
            phase: SharedWalPublicationPhase::Prepared,
        })
    }

    #[must_use]
    pub const fn baseline(&self) -> WalIndexHdr {
        self.baseline
    }

    #[must_use]
    pub const fn generation(&self) -> WalGenerationIdentity {
        self.generation
    }

    /// A partial shared publication must finish before another append starts.
    #[must_use]
    pub const fn can_extend(&self) -> bool {
        matches!(self.phase, SharedWalPublicationPhase::Prepared) && self.target.is_none()
    }

    /// Count newly published commit markers, excluding a later staged suffix.
    pub fn publication_change(&self, maximum_frame: u32) -> Result<u32> {
        let mut change = self.baseline.i_change;
        for &(frame, _, is_commit) in &self.entries {
            if frame > maximum_frame {
                break;
            }
            change = change.wrapping_add(u32::from(is_commit));
            if frame == maximum_frame {
                return if is_commit {
                    Ok(change)
                } else {
                    Err(FrankenError::BusyRecovery)
                };
            }
        }
        Err(FrankenError::BusyRecovery)
    }

    /// Publish entries then both header copies without async I/O or mapping.
    ///
    /// The caller has already established durability or deferred authority.
    /// On error retain this exact plan and its external writer owner. A retry
    /// after header publication never clears newly visible mappings.
    pub fn publish(&mut self, target: WalIndexHdr) -> Result<()> {
        target.validate()?;
        if target.mx_frame <= self.baseline.mx_frame
            || self
                .entries
                .last()
                .is_none_or(|(frame, _, _)| target.mx_frame > *frame)
            || target.i_version != self.baseline.i_version
            || target.unused != self.baseline.unused
            || target.is_init != self.baseline.is_init
            || target.big_end_cksum != self.baseline.big_end_cksum
            || target.sz_page != self.baseline.sz_page
            || target.a_salt != self.baseline.a_salt
            || target.i_change != self.publication_change(target.mx_frame)?
            || self.target.is_some_and(|previous| previous != target)
        {
            return Err(FrankenError::WalCorrupt {
                detail: "shared WAL-index publication does not match its retained target"
                    .to_owned(),
            });
        }
        self.target = Some(target);
        let live = read_shared_wal_index_header(&self.regions[0].1)?;
        match self.phase {
            SharedWalPublicationPhase::Prepared | SharedWalPublicationPhase::InstallingMappings => {
                if live != Some(self.baseline) {
                    return Err(FrankenError::BusyRecovery);
                }
                self.phase = SharedWalPublicationPhase::InstallingMappings;
                for (number, region) in &self.regions {
                    clear_shared_wal_index_tail(region, *number, self.baseline.mx_frame)?;
                }
                for &(frame, page, _) in &self.entries {
                    if frame > target.mx_frame {
                        break;
                    }
                    let number = WalIndexFrameLocation::new(frame)?.region;
                    let index = self
                        .regions
                        .binary_search_by_key(&number, |(region, _)| *region)
                        .map_err(|_| FrankenError::BusyRecovery)?;
                    append_shared_wal_index_entry(&self.regions[index].1, frame, page)?;
                }
                self.phase = SharedWalPublicationPhase::PublishingHeaders;
            }
            SharedWalPublicationPhase::PublishingHeaders => {
                if live == Some(target) {
                    self.phase = SharedWalPublicationPhase::HeadersPublished;
                    return Ok(());
                }
                if live.is_some_and(|header| header != self.baseline) {
                    return Err(FrankenError::BusyRecovery);
                }
                // A mismatched pair is admissible only here: this owner has
                // installed every mapping and already started these headers.
            }
            SharedWalPublicationPhase::HeadersPublished => {
                return if live == Some(target) {
                    Ok(())
                } else {
                    Err(FrankenError::BusyRecovery)
                };
            }
        }
        publish_shared_wal_index_header(&self.regions[0].1, &target)?;
        self.phase = SharedWalPublicationPhase::HeadersPublished;
        Ok(())
    }

    /// Retire a completed private publication while retaining its staged suffix.
    ///
    /// Call only after `publish` succeeded and private publication completed.
    /// Returns whether a later, still-uncommitted suffix remains staged.
    pub fn finish_private_publication(&mut self) -> bool {
        assert_eq!(self.phase, SharedWalPublicationPhase::HeadersPublished);
        let target = self.target.take().expect("published native header target");
        self.entries
            .retain(|(frame, _, _)| *frame > target.mx_frame);
        self.baseline = target;
        self.phase = SharedWalPublicationPhase::Prepared;
        !self.entries.is_empty()
    }
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
    fn test_shared_backfill_preserves_unrelated_bytes_and_refuses_regression() {
        let region = ShmRegion::from_vec(vec![0xa7; WAL_SHM_SEGMENT_BYTES]);
        let mut header = shared_header_fixture(4096, 7);
        header.mx_frame = 20;
        header.update_checksum().unwrap();
        publish_shared_wal_index_header(&region, &header).unwrap();
        region
            .atomic_store_u32_ne(96, 3, Ordering::Release)
            .unwrap();
        region
            .atomic_store_u32_ne(128, 8, Ordering::Release)
            .unwrap();
        let before = region.lock().to_vec();
        assert_eq!(read_shared_wal_index_backfill(&region, &header).unwrap(), 3);
        for (cumulative, attempted) in [(5, 8), (12, 12), (20, 20)] {
            publish_shared_wal_index_backfill(&region, &header, cumulative).unwrap();
            let mut expected = before.clone();
            write4(&mut expected, 96, cumulative);
            write4(&mut expected, 128, attempted);
            assert_eq!(
                region.lock().to_vec(),
                expected,
                "only the two watermarks change"
            );
            assert_eq!(
                read_shared_wal_index_backfill(&region, &header).unwrap(),
                cumulative
            );
            publish_shared_wal_index_backfill(&region, &header, cumulative).unwrap();
            assert_eq!(
                region.lock().to_vec(),
                expected,
                "same cumulative publication is idempotent"
            );
        }
        let full = region.lock().to_vec();
        for cumulative in [0, 19, 21, u32::MAX] {
            assert!(publish_shared_wal_index_backfill(&region, &header, cumulative).is_err());
            assert_eq!(region.lock().to_vec(), full);
        }
        let mut foreign = header;
        foreign.i_change += 1;
        foreign.update_checksum().unwrap();
        publish_shared_wal_index_header(&region, &foreign).unwrap();
        let changed = region.lock().to_vec();
        assert!(read_shared_wal_index_backfill(&region, &header).is_err());
        assert!(publish_shared_wal_index_backfill(&region, &header, 20).is_err());
        assert_eq!(region.lock().to_vec(), changed);
    }

    #[test]
    fn test_shared_backfill_refuses_invalid_intervals_and_mapping_sizes() {
        let mut header = shared_header_fixture(1, 0);
        header.mx_frame = 20;
        header.update_checksum().unwrap();
        for size in [0, 96, WAL_SHM_SEGMENT_BYTES - 1, WAL_SHM_SEGMENT_BYTES + 1] {
            let region = ShmRegion::from_vec(vec![0xa7; size]);
            let before = region.lock().to_vec();
            assert!(read_shared_wal_index_backfill(&region, &header).is_err());
            assert!(publish_shared_wal_index_backfill(&region, &header, 0).is_err());
            assert_eq!(region.lock().to_vec(), before);
        }
        let region = ShmRegion::new(WAL_SHM_SEGMENT_BYTES);
        publish_shared_wal_index_header(&region, &header).unwrap();
        for (backfill, attempted) in [(9, 8), (3, 21), (21, 21)] {
            region
                .atomic_store_u32_ne(96, backfill, Ordering::Release)
                .unwrap();
            region
                .atomic_store_u32_ne(128, attempted, Ordering::Release)
                .unwrap();
            let before = region.lock().to_vec();
            assert!(read_shared_wal_index_backfill(&region, &header).is_err());
            assert!(publish_shared_wal_index_backfill(&region, &header, 20).is_err());
            assert_eq!(region.lock().to_vec(), before);
        }
        for maximum in [0, u32::MAX] {
            header.mx_frame = maximum;
            header.update_checksum().unwrap();
            publish_shared_wal_index_header(&region, &header).unwrap();
            region
                .atomic_store_u32_ne(96, 0, Ordering::Release)
                .unwrap();
            region
                .atomic_store_u32_ne(128, 0, Ordering::Release)
                .unwrap();
            publish_shared_wal_index_backfill(&region, &header, maximum).unwrap();
            assert_eq!(
                read_shared_wal_index_backfill(&region, &header).unwrap(),
                maximum
            );
        }
    }

    fn shared_checkpoint_reset_fixture() -> (ShmRegion, WalIndexHdr, WalIndexHdr) {
        let region = ShmRegion::from_vec(vec![0xa7; WAL_SHM_SEGMENT_BYTES]);
        let mut baseline = shared_header_fixture(4096, 0);
        baseline.mx_frame = 20;
        baseline.i_change = u32::MAX;
        baseline.unused = 0x1234_5678;
        baseline.update_checksum().unwrap();
        publish_shared_wal_index_header(&region, &baseline).unwrap();
        region
            .atomic_store_u32_ne(96, 20, Ordering::Release)
            .unwrap();
        region
            .atomic_store_u32_ne(128, 20, Ordering::Release)
            .unwrap();
        let mut target = baseline;
        target.mx_frame = 0;
        target.n_page = 0;
        target.a_frame_cksum = [0, 0];
        target.a_salt[0] ^= 1;
        target.update_checksum().unwrap();
        (region, baseline, target)
    }

    #[test]
    fn test_shared_reset_prepare_refuses_short_torn_and_foreign_baselines() {
        let (region, baseline, target) = shared_checkpoint_reset_fixture();
        let before = region.lock().to_vec();
        for size in [0, WAL_SHM_SEGMENT_BYTES - 1, WAL_SHM_SEGMENT_BYTES + 1] {
            let short = ShmRegion::from_vec(vec![0xa7; size]);
            let untouched = short.lock().to_vec();
            assert!(SharedWalIndexResetPlan::prepare(short.share(), baseline, target).is_err());
            assert_eq!(short.lock().to_vec(), untouched);
        }
        let mut foreign_baseline = baseline;
        foreign_baseline.a_salt[1] ^= 2;
        foreign_baseline.update_checksum().unwrap();
        assert!(
            SharedWalIndexResetPlan::prepare(region.share(), foreign_baseline, target).is_err()
        );
        let mut bad_checksum = target;
        bad_checksum.a_cksum[0] ^= 1;
        assert!(SharedWalIndexResetPlan::prepare(region.share(), baseline, bad_checksum).is_err());
        assert_eq!(region.lock().to_vec(), before);
        region
            .atomic_store_u32_ne(WAL_INDEX_HDR_BYTES + 8, 0, Ordering::Release)
            .unwrap();
        let torn = region.lock().to_vec();
        assert!(SharedWalIndexResetPlan::prepare(region.share(), baseline, target).is_err());
        assert_eq!(region.lock().to_vec(), torn);
        write_shared_header_copy(&region, WAL_INDEX_HDR_BYTES, &baseline.to_bytes()).unwrap();
        assert_eq!(region.lock().to_vec(), before);
    }

    #[test]
    fn test_shared_reset_prepare_is_pure_and_publication_preserves_noncommit_state() {
        let (region, baseline, target) = shared_checkpoint_reset_fixture();
        let before = region.lock().to_vec();
        for case in 0..8 {
            let mut invalid = target;
            match case {
                0 => invalid.mx_frame = 1,
                1 => invalid.n_page = 1,
                2 => invalid.a_frame_cksum[0] = 1,
                3 => invalid.a_salt = baseline.a_salt,
                4 => invalid.i_change = 0,
                5 => invalid.unused ^= 1,
                6 => invalid.sz_page = 512,
                7 => invalid.big_end_cksum ^= 1,
                _ => unreachable!(),
            }
            invalid.update_checksum().unwrap();
            assert!(SharedWalIndexResetPlan::prepare(region.share(), baseline, invalid).is_err());
            assert_eq!(region.lock().to_vec(), before);
        }
        region
            .atomic_store_u32_ne(96, 19, Ordering::Release)
            .unwrap();
        let partial = region.lock().to_vec();
        assert!(SharedWalIndexResetPlan::prepare(region.share(), baseline, target).is_err());
        assert_eq!(region.lock().to_vec(), partial);
        region
            .atomic_store_u32_ne(96, 20, Ordering::Release)
            .unwrap();
        let mut plan = SharedWalIndexResetPlan::prepare(region.share(), baseline, target).unwrap();
        assert_eq!(plan.baseline(), baseline);
        assert_eq!(
            region.lock().to_vec(),
            before,
            "successful preparation is read-only"
        );
        plan.publish().unwrap();
        let after = region.lock().to_vec();
        assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(target));
        assert_eq!(
            target.i_change,
            u32::MAX,
            "reset publishes no commit and does not wrap iChange"
        );
        assert_eq!(
            &after[120..128],
            &before[120..128],
            "physical lock bytes remain untouched"
        );
        assert_eq!(
            &after[132..136],
            &before[132..136],
            "checkpoint reserved bytes remain untouched"
        );
        assert!(
            after[WAL_SHM_FIRST_HEADER_BYTES..]
                .iter()
                .all(|byte| *byte == 0)
        );
        assert_eq!(read_shared_wal_index_backfill(&region, &target).unwrap(), 0);
        for (slot, mark) in [0, 0, u32::MAX, u32::MAX, u32::MAX].into_iter().enumerate() {
            assert_eq!(
                read_shared_wal_index_read_mark(&region, u32::try_from(slot).unwrap()).unwrap(),
                mark
            );
        }
        plan.publish().unwrap();
        assert_eq!(region.lock().to_vec(), after);
    }

    #[test]
    fn test_shared_reset_retries_owned_invalidation_and_metadata_only() {
        for phase in [
            SharedWalIndexResetPhase::InvalidatingHeaders,
            SharedWalIndexResetPhase::ResettingMetadata,
        ] {
            let (region, baseline, target) = shared_checkpoint_reset_fixture();
            let mut plan =
                SharedWalIndexResetPlan::prepare(region.share(), baseline, target).unwrap();
            let mut unstarted =
                SharedWalIndexResetPlan::prepare(region.share(), baseline, target).unwrap();
            plan.phase = phase;
            region
                .atomic_store_u32_ne(12, 0, Ordering::Release)
                .unwrap();
            if phase == SharedWalIndexResetPhase::ResettingMetadata {
                region
                    .atomic_store_u32_ne(WAL_INDEX_HDR_BYTES + 12, 0, Ordering::Release)
                    .unwrap();
                region
                    .atomic_store_u32_ne(96, 0, Ordering::Release)
                    .unwrap();
                region
                    .atomic_store_u16_ne(WAL_SHM_PAGE_ARRAY_BYTES, 0, Ordering::Release)
                    .unwrap();
            }
            let interrupted = region.lock().to_vec();
            assert_eq!(read_shared_wal_index_header(&region).unwrap(), None);
            assert!(
                unstarted.publish().is_err(),
                "a fresh plan cannot adopt an invalid pair"
            );
            assert_eq!(region.lock().to_vec(), interrupted);
            plan.publish().unwrap();
            assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(target));
            assert_eq!(read_shared_wal_index_backfill(&region, &target).unwrap(), 0);
        }
    }

    #[test]
    fn test_shared_reset_header_last_retries_only_exact_owned_word_prefixes() {
        for (first_words, second_words) in [
            (0, 0),
            (0, 1),
            (0, 11),
            (0, 12),
            (1, 12),
            (11, 12),
            (12, 12),
        ] {
            let (region, baseline, target) = shared_checkpoint_reset_fixture();
            let mut plan =
                SharedWalIndexResetPlan::prepare(region.share(), baseline, target).unwrap();
            let mut unstarted =
                SharedWalIndexResetPlan::prepare(region.share(), baseline, target).unwrap();
            invalidate_shared_wal_index_header(&region).unwrap();
            reset_shared_wal_index_recovery_marks(&region, 0).unwrap();
            clear_shared_wal_index_tail(&region, 0, 0).unwrap();
            plan.phase = SharedWalIndexResetPhase::PublishingHeaders;
            let target_bytes = target.to_bytes();
            for (offset, words) in [(WAL_INDEX_HDR_BYTES, second_words), (0, first_words)] {
                let mut partial = plan.invalidated_baseline();
                partial[..words * 4].copy_from_slice(&target_bytes[..words * 4]);
                write_shared_header_copy(&region, offset, &partial).unwrap();
            }
            let interrupted = region.lock().to_vec();
            if first_words != 12 {
                assert_eq!(read_shared_wal_index_header(&region).unwrap(), None);
            }
            assert!(unstarted.publish().is_err());
            assert_eq!(region.lock().to_vec(), interrupted);
            plan.publish().unwrap();
            assert_eq!(read_shared_wal_index_header(&region).unwrap(), Some(target));
            // A completed retry must not clear any mapping bytes a second time.
            region
                .atomic_store_u32_ne(WAL_SHM_FIRST_HEADER_BYTES, 77, Ordering::Release)
                .unwrap();
            let completed = region.lock().to_vec();
            plan.publish().unwrap();
            assert_eq!(region.lock().to_vec(), completed);
        }
    }

    #[test]
    fn test_shared_reset_refuses_foreign_accepted_and_unrelated_invalid_headers() {
        for phase in [
            SharedWalIndexResetPhase::Prepared,
            SharedWalIndexResetPhase::InvalidatingHeaders,
            SharedWalIndexResetPhase::ResettingMetadata,
            SharedWalIndexResetPhase::PublishingHeaders,
            SharedWalIndexResetPhase::HeadersPublished,
        ] {
            let (region, baseline, target) = shared_checkpoint_reset_fixture();
            let mut plan =
                SharedWalIndexResetPlan::prepare(region.share(), baseline, target).unwrap();
            plan.phase = phase;
            let mut foreign = target;
            foreign.a_salt[1] ^= 1;
            foreign.update_checksum().unwrap();
            publish_shared_wal_index_header(&region, &foreign).unwrap();
            for invalidate in [false, true] {
                if invalidate {
                    invalidate_shared_wal_index_header(&region).unwrap();
                }
                let before = region.lock().to_vec();
                assert!(plan.publish().is_err());
                assert_eq!(region.lock().to_vec(), before);
                assert_eq!(
                    plan.phase, phase,
                    "refusal preserves the exact retained phase"
                );
                assert_eq!(plan.target, target);
                assert_eq!(plan.baseline(), baseline);
            }
        }
    }

    #[test]
    fn test_shared_recovery_replaces_cross_region_maps_with_header_last() {
        let regions = [
            ShmRegion::from_vec(vec![0xa7; WAL_SHM_SEGMENT_BYTES]),
            ShmRegion::from_vec(vec![0xb8; WAL_SHM_SEGMENT_BYTES]),
        ];
        let old_header = shared_header_fixture(4096, 7);
        publish_shared_wal_index_header(&regions[0], &old_header).unwrap();
        let before = regions[0].lock().to_vec();
        let mut scratch = vec![0; WAL_SHM_SEGMENT_BYTES];
        assert!(replace_shared_wal_index_region(&regions[0], 0, &scratch).is_err());
        assert!(reset_shared_wal_index_recovery_marks(&regions[0], 4063).is_err());
        assert_eq!(
            regions[0].lock().to_vec(),
            before,
            "accepted headers prohibit in-place rebuild"
        );
        invalidate_shared_wal_index_header(&regions[0]).unwrap();
        assert_eq!(read_shared_wal_index_header(&regions[0]).unwrap(), None);
        for frame in 1..=4062_u32 {
            append_native_wal_index_entry(&mut scratch, frame, 1 + (frame % 2) * 8192).unwrap();
        }
        replace_shared_wal_index_region(&regions[0], 0, &scratch).unwrap();
        scratch.fill(0);
        append_native_wal_index_entry(&mut scratch, 4063, 1).unwrap();
        replace_shared_wal_index_region(&regions[1], 1, &scratch).unwrap();
        assert_eq!(read_shared_wal_index_header(&regions[0]).unwrap(), None);
        reset_shared_wal_index_recovery_marks(&regions[0], 4063).unwrap();
        let rebuilt = regions[0].lock().to_vec();
        assert_eq!(
            &rebuilt[120..128],
            &before[120..128],
            "lock bytes are never recovery payload"
        );
        assert_eq!(
            &rebuilt[132..136],
            &before[132..136],
            "reserved bytes remain untouched"
        );
        assert_eq!(
            regions[0]
                .atomic_load_u32_ne(96, Ordering::Acquire)
                .unwrap(),
            0
        );
        assert_eq!(
            regions[0]
                .atomic_load_u32_ne(128, Ordering::Acquire)
                .unwrap(),
            0
        );
        for (slot, expected) in [0, 4063, u32::MAX, u32::MAX, u32::MAX]
            .into_iter()
            .enumerate()
        {
            assert_eq!(
                regions[0]
                    .atomic_load_u32_ne(100 + slot * 4, Ordering::Acquire)
                    .unwrap(),
                expected
            );
        }
        let mut target = old_header;
        target.mx_frame = 4063;
        target.i_change = 0;
        target.update_checksum().unwrap();
        publish_shared_wal_index_header(&regions[0], &target).unwrap();
        assert_eq!(
            read_shared_wal_index_header(&regions[0]).unwrap(),
            Some(target)
        );
        assert_eq!(
            lookup_native_wal_index_frame(&rebuilt, 0, 1, 4063).unwrap(),
            Some(4062)
        );
        assert_eq!(
            lookup_native_wal_index_frame(&regions[1].lock(), 1, 1, 4063).unwrap(),
            Some(4063)
        );
        assert_eq!(
            lookup_native_wal_index_frame(&regions[1].lock(), 1, 8193, 4063).unwrap(),
            None
        );
    }

    #[test]
    fn test_shared_recovery_refuses_short_inputs_and_admits_empty_nonzero_reader_mark() {
        for size in [0, 96, WAL_SHM_SEGMENT_BYTES - 1, WAL_SHM_SEGMENT_BYTES + 1] {
            let region = ShmRegion::from_vec(vec![0xa7; size]);
            let before = region.lock().to_vec();
            assert!(invalidate_shared_wal_index_header(&region).is_err());
            assert!(
                replace_shared_wal_index_region(&region, 0, &vec![0; WAL_SHM_SEGMENT_BYTES])
                    .is_err()
            );
            assert!(reset_shared_wal_index_recovery_marks(&region, 0).is_err());
            assert_eq!(region.lock().to_vec(), before);
        }
        let region = ShmRegion::new(WAL_SHM_SEGMENT_BYTES);
        let before = region.lock().to_vec();
        for size in [0, WAL_SHM_SEGMENT_BYTES - 1, WAL_SHM_SEGMENT_BYTES + 1] {
            assert!(replace_shared_wal_index_region(&region, 0, &vec![0; size]).is_err());
            assert_eq!(region.lock().to_vec(), before);
        }
        reset_shared_wal_index_recovery_marks(&region, 0).unwrap();
        assert_eq!(read_shared_wal_index_read_mark(&region, 1).unwrap(), 0);
        assert_eq!(read_shared_wal_index_header(&region).unwrap(), None);
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
    fn test_shared_reader_revalidation_preserves_bounds_and_rejects_stale_state() {
        let region = ShmRegion::new(WAL_SHM_SEGMENT_BYTES);
        let mut header = shared_header_fixture(4096, 7);
        header.mx_frame = 20;
        header.update_checksum().unwrap();
        publish_shared_wal_index_header(&region, &header).unwrap();
        region
            .atomic_store_u32_ne(96, 8, Ordering::Release)
            .unwrap();
        region
            .atomic_store_u32_ne(108, 12, Ordering::Release)
            .unwrap();
        let before = region.lock().to_vec();
        assert_eq!(read_shared_wal_index_read_mark(&region, 2).unwrap(), 12);
        assert_eq!(
            revalidate_shared_wal_index_reader(&region, &header, 2, 12).unwrap(),
            Some(WalIndexReadBoundary {
                backfilled_frames: 8,
                maximum_wal_frame: 20,
                database_only: false,
            })
        );
        for (slot, mark) in [(2, 11), (2, 21), (0, 0)] {
            assert!(
                revalidate_shared_wal_index_reader(&region, &header, slot, mark)
                    .unwrap()
                    .is_none()
            );
        }
        assert!(revalidate_shared_wal_index_reader(&region, &header, 5, 0).is_err());
        assert!(read_shared_wal_index_read_mark(&region, 5).is_err());
        assert_eq!(region.lock().to_vec(), before);

        let mut newer = header;
        newer.i_change += 1;
        newer.update_checksum().unwrap();
        publish_shared_wal_index_header(&region, &newer).unwrap();
        assert!(
            revalidate_shared_wal_index_reader(&region, &header, 2, 12)
                .unwrap()
                .is_none()
        );
        publish_shared_wal_index_header(&region, &header).unwrap();
        region
            .atomic_store_u32_ne(96, 21, Ordering::Release)
            .unwrap();
        assert!(
            revalidate_shared_wal_index_reader(&region, &header, 2, 12)
                .unwrap()
                .is_none()
        );
        region
            .atomic_store_u32_ne(96, 20, Ordering::Release)
            .unwrap();
        assert_eq!(
            revalidate_shared_wal_index_reader(&region, &header, 0, u32::MAX).unwrap(),
            Some(WalIndexReadBoundary {
                backfilled_frames: 20,
                maximum_wal_frame: 20,
                database_only: true,
            })
        );
    }

    #[test]
    fn test_shared_reader_revalidation_handles_empty_and_maximum_frame_boundaries() {
        for frame_count in [0, u32::MAX] {
            let region = ShmRegion::new(WAL_SHM_SEGMENT_BYTES);
            let mut header = shared_header_fixture(1, 0);
            header.mx_frame = frame_count;
            header.update_checksum().unwrap();
            publish_shared_wal_index_header(&region, &header).unwrap();
            region
                .atomic_store_u32_ne(96, frame_count, Ordering::Release)
                .unwrap();
            let boundary = revalidate_shared_wal_index_reader(&region, &header, 0, 0)
                .unwrap()
                .expect("fully backfilled database-only boundary");
            assert_eq!(boundary.backfilled_frames, frame_count);
            assert_eq!(boundary.maximum_wal_frame, frame_count);
            assert!(boundary.database_only);
            region
                .atomic_store_u32_ne(104, u32::MAX, Ordering::Release)
                .unwrap();
            assert!(
                revalidate_shared_wal_index_reader(&region, &header, 1, u32::MAX)
                    .unwrap()
                    .is_none(),
                "the unused-reader sentinel is not a claim, even at maximum mxFrame"
            );
        }
        let short = ShmRegion::new(136);
        let header = shared_header_fixture(4096, 0);
        assert!(revalidate_shared_wal_index_reader(&short, &header, 0, 0).is_err());
        assert!(read_shared_wal_index_read_mark(&short, 0).is_err());
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
    fn test_shared_publication_binding_rejects_format_and_terminal_mismatch() {
        for page_size in [512, 4096, 65536] {
            let mut header = shared_header_fixture(
                if page_size == 65536 {
                    1
                } else {
                    u16::try_from(page_size).unwrap()
                },
                9,
            );
            let wal = WalHeader {
                magic: crate::WAL_MAGIC_BE,
                format_version: crate::WAL_FORMAT_VERSION,
                page_size,
                checkpoint_seq: 17,
                salts: crate::WalSalts {
                    salt1: header.a_salt[0],
                    salt2: header.a_salt[1],
                },
                checksum: crate::SqliteWalChecksum { s1: 3, s2: 4 },
            };
            let marker = WalFrameHeader {
                page_number: 7,
                db_size: header.n_page,
                salts: wal.salts,
                checksum: crate::SqliteWalChecksum {
                    s1: header.a_frame_cksum[0],
                    s2: header.a_frame_cksum[1],
                },
            };
            validate_shared_wal_index_wal_binding(&header, &wal, Some((header.mx_frame, marker)))
                .unwrap();
            for mutation in 0..7 {
                let mut changed = header;
                match mutation {
                    0 => changed.a_salt[0] ^= 1,
                    1 => changed.big_end_cksum ^= 1,
                    2 => changed.sz_page = if header.sz_page == 512 { 4096 } else { 512 },
                    3 => changed.mx_frame -= 1,
                    4 => changed.n_page += 1,
                    5 => changed.a_frame_cksum[1] ^= 1,
                    _ => changed.mx_frame = 0,
                }
                changed.update_checksum().unwrap();
                assert!(
                    validate_shared_wal_index_wal_binding(
                        &changed,
                        &wal,
                        Some((header.mx_frame, marker)),
                    )
                    .is_err()
                );
            }
            let mut uncommitted = marker;
            uncommitted.db_size = 0;
            assert!(
                validate_shared_wal_index_wal_binding(
                    &header,
                    &wal,
                    Some((header.mx_frame, uncommitted)),
                )
                .is_err()
            );
            assert!(validate_shared_wal_index_wal_binding(&header, &wal, None).is_err());
            header.mx_frame = 0;
            header.update_checksum().unwrap();
            validate_shared_wal_index_wal_binding(&header, &wal, None).unwrap();
            let mut invalid_format = wal;
            invalid_format.format_version += 1;
            assert!(validate_shared_wal_index_wal_binding(&header, &invalid_format, None).is_err());
        }
    }

    #[test]
    fn test_shared_append_plan_counts_markers_crosses_region_and_preserves_prefix() {
        let regions = [
            ShmRegion::new(WAL_SHM_SEGMENT_BYTES),
            ShmRegion::new(WAL_SHM_SEGMENT_BYTES),
        ];
        let mut baseline = shared_header_fixture(4096, 0);
        baseline.mx_frame = 4061;
        baseline.i_change = u32::MAX;
        baseline.update_checksum().unwrap();
        publish_shared_wal_index_header(&regions[0], &baseline).unwrap();
        for offset in (96..WAL_SHM_FIRST_HEADER_BYTES).step_by(4) {
            regions[0]
                .atomic_store_u32_ne(offset, 0x6172_8394, Ordering::Release)
                .unwrap();
        }
        for frame in 1..=baseline.mx_frame {
            append_shared_wal_index_entry(&regions[0], frame, frame).unwrap();
        }
        let before = [regions[0].lock().to_vec(), regions[1].lock().to_vec()];
        let generation = WalGenerationIdentity {
            checkpoint_seq: 17,
            salts: crate::WalSalts {
                salt1: baseline.a_salt[0],
                salt2: baseline.a_salt[1],
            },
        };
        let mut plan = SharedWalIndexAppendPlan::prepare(
            baseline,
            generation,
            vec![(0, regions[0].share()), (1, regions[1].share())],
            vec![
                (4062, 7, true),
                (4063, 8199, false),
                (4064, 7, true),
                (4065, 9, false),
            ],
        )
        .unwrap();
        assert_eq!(regions[0].lock().to_vec(), before[0]);
        assert_eq!(regions[1].lock().to_vec(), before[1]);
        assert!(
            plan.publication_change(4063).is_err(),
            "noncommit cannot be a publication horizon"
        );
        let mut target = baseline;
        target.mx_frame = 4064;
        target.i_change = plan.publication_change(target.mx_frame).unwrap();
        assert_eq!(
            target.i_change, 1,
            "two markers wrap the previous counter once"
        );
        target.n_page = 47;
        target.a_frame_cksum = [77, 88];
        target.update_checksum().unwrap();
        let mut wrong_count = target;
        wrong_count.i_change = 0;
        wrong_count.update_checksum().unwrap();
        assert!(plan.publish(wrong_count).is_err());
        assert_eq!(regions[0].lock().to_vec(), before[0]);
        plan.publish(target).unwrap();
        assert_eq!(
            read_shared_wal_index_header(&regions[0]).unwrap(),
            Some(target)
        );
        let published = [regions[0].lock().to_vec(), regions[1].lock().to_vec()];
        assert_eq!(
            &published[0][96..WAL_SHM_FIRST_HEADER_BYTES],
            &before[0][96..WAL_SHM_FIRST_HEADER_BYTES]
        );
        assert_eq!(
            &published[0][WAL_SHM_FIRST_HEADER_BYTES..WAL_SHM_PAGE_ARRAY_BYTES - 4],
            &before[0][WAL_SHM_FIRST_HEADER_BYTES..WAL_SHM_PAGE_ARRAY_BYTES - 4],
            "every previously published page word remains byte-identical",
        );
        assert_eq!(
            lookup_native_wal_index_frame(&published[0], 0, 7, 4061).unwrap(),
            Some(7)
        );
        assert_eq!(
            lookup_native_wal_index_frame(&published[1], 1, 7, target.mx_frame).unwrap(),
            Some(4064)
        );
        assert_eq!(
            lookup_native_wal_index_frame(&published[1], 1, 9, 4065).unwrap(),
            None
        );
        plan.publish(target).unwrap();
        assert_eq!(regions[0].lock().to_vec(), published[0]);
        assert_eq!(regions[1].lock().to_vec(), published[1]);
        assert!(plan.finish_private_publication());
        assert_eq!(plan.baseline(), target);
        assert_eq!(plan.entries, vec![(4065, 9, false)]);
        assert!(plan.can_extend());
    }

    #[test]
    fn test_shared_append_plan_exact_partial_header_retry_and_foreign_header_refusal() {
        let region = ShmRegion::new(WAL_SHM_SEGMENT_BYTES);
        let mut baseline = shared_header_fixture(512, 0);
        baseline.mx_frame = 0;
        baseline.update_checksum().unwrap();
        publish_shared_wal_index_header(&region, &baseline).unwrap();
        let generation = WalGenerationIdentity {
            checkpoint_seq: 0,
            salts: crate::WalSalts {
                salt1: baseline.a_salt[0],
                salt2: baseline.a_salt[1],
            },
        };
        let mut plan = SharedWalIndexAppendPlan::prepare(
            baseline,
            generation,
            vec![(0, region.share())],
            vec![(1, 7, true), (2, 7, true)],
        )
        .unwrap();
        let mut target = baseline;
        target.mx_frame = 2;
        target.i_change = 2;
        target.update_checksum().unwrap();
        // Controlled interruption: replay an owned partial mapping installation.
        plan.phase = SharedWalPublicationPhase::InstallingMappings;
        append_shared_wal_index_entry(&region, 1, 7).unwrap();
        plan.publish(target).unwrap();
        let published = region.lock().to_vec();
        // Controlled interruption after copy two, with the exact retained plan.
        plan.phase = SharedWalPublicationPhase::PublishingHeaders;
        write_shared_header_copy(&region, 0, &baseline.to_bytes()).unwrap();
        assert_eq!(read_shared_wal_index_header(&region).unwrap(), None);
        plan.publish(target).unwrap();
        assert_eq!(region.lock().to_vec(), published);
        assert!(!plan.finish_private_publication());
        let mut stale = SharedWalIndexAppendPlan::prepare(
            target,
            generation,
            vec![(0, region.share())],
            vec![(3, 9, true)],
        )
        .unwrap();
        let mut foreign = target;
        foreign.i_change += 1;
        foreign.update_checksum().unwrap();
        publish_shared_wal_index_header(&region, &foreign).unwrap();
        let foreign_bytes = region.lock().to_vec();
        let mut next = target;
        next.mx_frame = 3;
        next.i_change += 1;
        next.update_checksum().unwrap();
        assert!(stale.publish(next).is_err());
        assert_eq!(region.lock().to_vec(), foreign_bytes);
    }

    #[test]
    fn test_shared_append_plan_preflight_refuses_missing_region_and_invalid_entries() {
        let region = ShmRegion::new(WAL_SHM_SEGMENT_BYTES);
        let mut baseline = shared_header_fixture(4096, 0);
        baseline.mx_frame = 4062;
        baseline.update_checksum().unwrap();
        publish_shared_wal_index_header(&region, &baseline).unwrap();
        let generation = WalGenerationIdentity {
            checkpoint_seq: 0,
            salts: crate::WalSalts {
                salt1: baseline.a_salt[0],
                salt2: baseline.a_salt[1],
            },
        };
        let before = region.lock().to_vec();
        for entries in [
            vec![(4063, 9, true)],
            vec![(4064, 9, true)],
            vec![(4063, 0, true)],
            vec![],
        ] {
            assert!(
                SharedWalIndexAppendPlan::prepare(
                    baseline,
                    generation,
                    vec![(0, region.share())],
                    entries,
                )
                .is_err()
            );
            assert_eq!(region.lock().to_vec(), before);
        }
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
