//! Strict, read-only byte witnesses for frame-free WAL recovery (GH #515).
//!
//! Unlike SQLite's reader treatment of a short WAL, recovery must not turn a
//! malformed header into permission to rewrite a protected file. Only an absent
//! WAL, a zero-byte WAL, or one complete, checksum-valid header is frame-free
//! here. Even a single byte after the header is refused.
//!
//! A witness describes the supplied bytes, NOT a live filesystem generation.
//! Callers must retain file identities, exclude writers, verify the complete
//! database family, and recheck the same bytes before publication. In particular,
//! a successful transition check does not authorize changing the main database,
//! journal, permissions, certificates, or WAL-index generation. It is not a lock
//! and must never be used to bypass BusyRecovery or WAL append admission.

use std::fmt;

const HEADER_BYTES: usize = 32;
const WAL_VERSION: u32 = 3_007_000;

/// Why a protected WAL does not qualify for the narrow recovery exception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameFreeWalError {
    /// The expected main-database page size is invalid.
    InvalidDatabasePageSize,
    /// A nonempty WAL is shorter than its complete header.
    PartialHeader,
    /// There are bytes after the header, including a partial or invalid frame.
    FrameBytesPresent,
    /// The magic, version, or encoded page size is invalid.
    InvalidHeader,
    /// The header's checksum does not validate in the indicated byte order.
    InvalidChecksum,
    /// The WAL and the independently verified main database use different sizes.
    PageSizeMismatch,
    /// A valid frame-free state changed in a way other than zero bytes to header.
    NotGenerationBinding,
}

impl fmt::Display for FrameFreeWalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidDatabasePageSize => "invalid main database page size",
            Self::PartialHeader => "partial WAL header is not recovery authority",
            Self::FrameBytesPresent => "WAL contains frame bytes; committed, uncheckpointed changes must be preserved",
            Self::InvalidHeader => "invalid WAL header",
            Self::InvalidChecksum => "invalid WAL header checksum",
            Self::PageSizeMismatch => "WAL and main database page sizes differ",
            Self::NotGenerationBinding => "WAL change is not a zero-byte to valid-header generation binding",
        })
    }
}

impl std::error::Error for FrameFreeWalError {}

/// A complete header whose structure, checksum, and database page size agree.
/// Fields are private so callers cannot manufacture an unvalidated witness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameFreeWalHeader {
    bytes: [u8; HEADER_BYTES],
}

impl FrameFreeWalHeader {
    /// The exact verified header, including the checkpoint sequence and salts.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; HEADER_BYTES] {
        &self.bytes
    }

    /// Main-database page size to which this header was checked.
    #[must_use]
    pub fn page_size(&self) -> u32 {
        word(&self.bytes, 8)
    }

    /// WAL checkpoint sequence; salts alone do not identify a generation.
    #[must_use]
    pub fn checkpoint_sequence(&self) -> u32 {
        word(&self.bytes, 12)
    }

    /// The raw big-endian WAL salt values, not native-endian SHM scalars.
    #[must_use]
    pub fn salts(&self) -> [u32; 2] {
        [word(&self.bytes, 16), word(&self.bytes, 20)]
    }
}

/// A validated observation; absence is intentionally distinct from zero bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameFreeWalState {
    /// No WAL existed in the observed family.
    Absent,
    /// A WAL existed and had exactly zero bytes.
    Empty,
    /// Exactly one verified header existed, with no frame or trailing bytes.
    Header(FrameFreeWalHeader),
}

/// A byte-preserving recovery or the one deliberately supported WAL change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameFreeWalTransition {
    /// Presence and every WAL byte were unchanged, and both states are valid.
    Unchanged,
    /// An existing zero-byte WAL gained exactly this verified header.
    HeaderBound(FrameFreeWalHeader),
}

const fn valid_page_size(size: u32) -> bool {
    size >= 512 && size <= 65_536 && size.is_power_of_two()
}

fn word(bytes: &[u8; HEADER_BYTES], offset: usize) -> u32 {
    u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

/// Verify a bounded WAL observation against the main database's page size.
///
/// `None` means absent, not unreadable. An I/O error must be propagated by the
/// caller rather than converted to `None`. Supply all bytes (or first establish
/// the exact length under exclusion); supplying just the header of a framed WAL
/// is not a frame-free proof.
///
/// # Errors
/// Rejects malformed or partial headers, every trailing byte, checksum errors,
/// invalid page sizes, and page-size disagreement with the main database.
pub fn inspect_frame_free_wal(
    bytes: Option<&[u8]>,
    database_page_size: u32,
) -> Result<FrameFreeWalState, FrameFreeWalError> {
    if !valid_page_size(database_page_size) {
        return Err(FrameFreeWalError::InvalidDatabasePageSize);
    }
    let Some(bytes) = bytes else {
        return Ok(FrameFreeWalState::Absent);
    };
    match bytes.len() {
        0 => return Ok(FrameFreeWalState::Empty),
        1..HEADER_BYTES => return Err(FrameFreeWalError::PartialHeader),
        HEADER_BYTES => {}
        _ => return Err(FrameFreeWalError::FrameBytesPresent),
    }
    let mut header = [0; HEADER_BYTES];
    header.copy_from_slice(bytes);
    let magic = word(&header, 0);
    let page_size = word(&header, 8);
    if !matches!(magic, 0x377f_0682 | 0x377f_0683)
        || word(&header, 4) != WAL_VERSION
        || !valid_page_size(page_size)
    {
        return Err(FrameFreeWalError::InvalidHeader);
    }
    if page_size != database_page_size {
        return Err(FrameFreeWalError::PageSizeMismatch);
    }
    let big_endian = magic & 1 != 0;
    let mut checksum = [0_u32; 2];
    for pair in header[..24].chunks_exact(8) {
        let decode = |at: usize| {
            let value = [pair[at], pair[at + 1], pair[at + 2], pair[at + 3]];
            if big_endian {
                u32::from_be_bytes(value)
            } else {
                u32::from_le_bytes(value)
            }
        };
        checksum[0] = checksum[0].wrapping_add(decode(0)).wrapping_add(checksum[1]);
        checksum[1] = checksum[1].wrapping_add(decode(4)).wrapping_add(checksum[0]);
    }
    if checksum != [word(&header, 24), word(&header, 28)] {
        return Err(FrameFreeWalError::InvalidChecksum);
    }
    Ok(FrameFreeWalState::Header(FrameFreeWalHeader { bytes: header }))
}

/// Verify only an unchanged WAL or an existing empty WAL gaining one header.
///
/// Both sides are validated before equality is considered: unchanged corrupt
/// headers or unchanged frame-bearing WALs never enter the exception. Resetting
/// a valid header, removing a WAL, or creating a previously absent WAL is not
/// admitted, even when neither observation contains frames.
///
/// This is only the WAL-byte portion of a recovery proof. Main/journal content,
/// file identity, metadata, ownership, concurrent access and index publication
/// must be checked separately under the owner's exclusion protocol.
///
/// # Errors
/// Returns a validation error for either observation or rejects a transition
/// other than exact preservation or zero bytes to one valid header.
pub fn verify_frame_free_wal_transition(
    before: Option<&[u8]>,
    after: Option<&[u8]>,
    database_page_size: u32,
) -> Result<FrameFreeWalTransition, FrameFreeWalError> {
    let before = inspect_frame_free_wal(before, database_page_size)?;
    let after = inspect_frame_free_wal(after, database_page_size)?;
    if before == after {
        return Ok(FrameFreeWalTransition::Unchanged);
    }
    match (before, after) {
        (FrameFreeWalState::Empty, FrameFreeWalState::Header(header)) => {
            Ok(FrameFreeWalTransition::HeaderBound(header))
        }
        _ => Err(FrameFreeWalError::NotGenerationBinding),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Independent fixture construction follows SQLite's documented recurrence.
    fn header(page_size: u32, big_endian: bool, sequence: u32) -> [u8; 32] {
        let fields = [
            if big_endian { 0x377f_0683 } else { 0x377f_0682 },
            WAL_VERSION,
            page_size,
            sequence,
            0x1234_5678,
            0x9abc_def0,
        ];
        let mut bytes = [0; 32];
        for (field, slot) in fields.iter().zip(bytes.chunks_exact_mut(4)) {
            slot.copy_from_slice(&field.to_be_bytes());
        }
        let mut a = 0_u32;
        let mut b = 0_u32;
        for pair in bytes[..24].chunks_exact(8) {
            let decode = |slice: &[u8]| {
                let array: [u8; 4] = slice.try_into().unwrap();
                if big_endian { u32::from_be_bytes(array) } else { u32::from_le_bytes(array) }
            };
            a = a.wrapping_add(decode(&pair[..4])).wrapping_add(b);
            b = b.wrapping_add(decode(&pair[4..])).wrapping_add(a);
        }
        bytes[24..28].copy_from_slice(&a.to_be_bytes());
        bytes[28..32].copy_from_slice(&b.to_be_bytes());
        bytes
    }

    #[test]
    fn accepts_only_valid_frame_free_observations_in_both_orders() {
        for size in [512, 1024, 2048, 4096, 8192, 16_384, 32_768, 65_536] {
            assert_eq!(inspect_frame_free_wal(None, size), Ok(FrameFreeWalState::Absent));
            assert_eq!(inspect_frame_free_wal(Some(&[]), size), Ok(FrameFreeWalState::Empty));
            for big_endian in [false, true] {
                let bytes = header(size, big_endian, 7);
                let FrameFreeWalState::Header(proof) = inspect_frame_free_wal(Some(&bytes), size).unwrap() else {
                    panic!("expected a verified header");
                };
                assert_eq!(proof.as_bytes(), &bytes);
                assert_eq!(proof.page_size(), size);
                assert_eq!(proof.checkpoint_sequence(), 7);
                assert_eq!(proof.salts(), [0x1234_5678, 0x9abc_def0]);
            }
        }
    }

    #[test]
    fn rejects_every_partial_header_and_any_trailing_bytes() {
        let bytes = header(4096, false, 0);
        for length in 1..32 {
            assert_eq!(inspect_frame_free_wal(Some(&bytes[..length]), 4096), Err(FrameFreeWalError::PartialHeader));
        }
        for extra in [1, 23, 24, 24 + 4096, 2 * (24 + 4096)] {
            let mut framed = bytes.to_vec();
            framed.resize(32 + extra, 0);
            assert_eq!(inspect_frame_free_wal(Some(&framed), 4096), Err(FrameFreeWalError::FrameBytesPresent));
            assert!(verify_frame_free_wal_transition(Some(&framed), Some(&framed), 4096).is_err());
            assert!(verify_frame_free_wal_transition(Some(&framed), Some(&bytes), 4096).is_err());
        }
    }

    #[test]
    fn rejects_every_single_bit_header_corruption() {
        for big_endian in [false, true] {
            let original = header(4096, big_endian, 3);
            for byte in 0..32 {
                for bit in 0..8 {
                    let mut corrupt = original;
                    corrupt[byte] ^= 1 << bit;
                    assert!(inspect_frame_free_wal(Some(&corrupt), 4096).is_err(), "byte={byte}, bit={bit}, big_endian={big_endian}");
                    assert!(verify_frame_free_wal_transition(Some(&corrupt), Some(&corrupt), 4096).is_err());
                }
            }
        }
    }

    #[test]
    fn rejects_invalid_or_mismatched_page_sizes_and_versions() {
        for size in [0, 1, 256, 513, 4095, 65_537, 131_072, u32::MAX] {
            assert_eq!(inspect_frame_free_wal(None, size), Err(FrameFreeWalError::InvalidDatabasePageSize));
            assert_eq!(inspect_frame_free_wal(Some(&header(size, false, 0)), 4096), Err(FrameFreeWalError::InvalidHeader));
        }
        assert_eq!(inspect_frame_free_wal(Some(&header(8192, false, 0)), 4096), Err(FrameFreeWalError::PageSizeMismatch));
        let mut version = header(4096, false, 0);
        version[4..8].copy_from_slice(&3_007_001_u32.to_be_bytes());
        assert_eq!(inspect_frame_free_wal(Some(&version), 4096), Err(FrameFreeWalError::InvalidHeader));
    }

    #[test]
    fn binding_is_one_way_and_never_an_absence_or_reset_exception() {
        for big_endian in [false, true] {
            let first = header(4096, big_endian, 1);
            let next = header(4096, big_endian, 2);
            let states: [Option<&[u8]>; 4] = [None, Some(&[]), Some(&first), Some(&next)];
            for (i, before) in states.iter().enumerate() {
                for (j, after) in states.iter().enumerate() {
                    let result = verify_frame_free_wal_transition(*before, *after, 4096);
                    if i == j {
                        assert_eq!(result, Ok(FrameFreeWalTransition::Unchanged));
                    } else if i == 1 && j >= 2 {
                        assert!(matches!(result, Ok(FrameFreeWalTransition::HeaderBound(_))));
                    } else {
                        assert_eq!(result, Err(FrameFreeWalError::NotGenerationBinding));
                    }
                }
            }
        }
    }
}
