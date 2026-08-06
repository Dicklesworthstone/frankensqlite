//! Corruption injection framework for resilience and recovery testing.
//!
//! Provides precise, deterministic corruption at byte, page, header, WAL
//! frame, and FEC sidecar granularity.  Every injection produces a
//! [`CorruptionReport`] capturing exactly what was changed so recovery can
//! be verified.
//!
//! # Safety
//!
//! [`CorruptionInjector::new`] refuses paths that resolve into a `golden/`
//! directory to prevent accidental modification of reference copies.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use sha2::{Digest, Sha256};

use crate::{E2eError, E2eResult};

/// Default SQLite page size (bytes).
pub const DEFAULT_PAGE_SIZE: u32 = 4096;

/// WAL file header size (bytes).
const WAL_HEADER_SIZE: u64 = 32;

/// WAL frame header size (bytes).
const WAL_FRAME_HEADER_SIZE: u64 = 24;

/// SQLite database header size (bytes).
const DB_HEADER_SIZE: usize = 100;

// ── CorruptionPattern ───────────────────────────────────────────────────

/// A description of corruption to inject into a database file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CorruptionPattern {
    /// Flip a single bit at a specific byte offset.
    BitFlip { byte_offset: u64, bit_position: u8 },
    /// Flip N unique bits within a region `[offset..offset+length)`.
    ///
    /// Deterministic: the same `(offset, length, count, seed)` always flips
    /// the same set of bits (order-independent).
    BitFlipMany {
        offset: u64,
        length: u64,
        count: u32,
        seed: u64,
    },
    /// Zero out an entire page (SQLite page numbers are 1-indexed).
    PageZero { page_number: u32 },
    /// Overwrite N bytes at offset with seeded random data.
    RandomOverwrite {
        offset: u64,
        length: usize,
        seed: u64,
    },
    /// Overwrite N bytes within a specific page with seeded random data.
    PagePartialCorrupt {
        page_number: u32,
        offset_within_page: u16,
        length: u16,
        seed: u64,
    },
    /// Truncate the target file to `new_len` bytes.
    TruncateTo { new_len: u64 },
    /// Zero out the 100-byte database header (page 1, offset 0..100).
    HeaderZero,
    /// Corrupt specific WAL frames with seeded random data.
    ///
    /// Note: `frame_numbers` are 0-indexed (first frame starts at offset 32).
    WalFrameCorrupt { frame_numbers: Vec<u32>, seed: u64 },
    /// Truncate the WAL to only the first N frames.
    ///
    /// The resulting WAL length is `WAL_HEADER_SIZE + frames*(24 + page_size)`.
    WalTruncate { frames: u32 },
    /// Flip a single bit within a WAL frame's payload.
    ///
    /// `frame_index` is 0-indexed. `byte_offset_within_payload` must be
    /// `< page_size`.
    WalFrameBitFlip {
        frame_index: u32,
        byte_offset_within_payload: u32,
        bit_position: u8,
    },
    /// Flip N unique bits across WAL frames `frame_start..=frame_end`.
    WalBitRot {
        frame_start: u32,
        frame_end: u32,
        flips: u32,
        seed: u64,
    },
    /// Simulate a torn write by truncating the WAL within a frame payload.
    ///
    /// `frame_index` is 0-indexed. The WAL will be truncated to the end of the
    /// frame header plus `bytes_into_payload`.
    WalTornTruncate {
        frame_index: u32,
        bytes_into_payload: u32,
    },
    /// Corrupt a region of an FEC sidecar file with seeded random data.
    SidecarCorrupt {
        offset: u64,
        length: usize,
        seed: u64,
    },
}

impl fmt::Display for CorruptionPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BitFlip {
                byte_offset,
                bit_position,
            } => write!(f, "BitFlip(byte={byte_offset}, bit={bit_position})"),
            Self::BitFlipMany {
                offset,
                length,
                count,
                seed,
            } => write!(
                f,
                "BitFlipMany(off={offset}, len={length}, count={count}, seed={seed})"
            ),
            Self::PageZero { page_number } => write!(f, "PageZero(page={page_number})"),
            Self::RandomOverwrite {
                offset,
                length,
                seed,
            } => write!(
                f,
                "RandomOverwrite(off={offset}, len={length}, seed={seed})"
            ),
            Self::PagePartialCorrupt {
                page_number,
                offset_within_page,
                length,
                seed,
            } => write!(
                f,
                "PagePartialCorrupt(page={page_number}, off={offset_within_page}, len={length}, seed={seed})"
            ),
            Self::TruncateTo { new_len } => write!(f, "TruncateTo(new_len={new_len})"),
            Self::HeaderZero => write!(f, "HeaderZero"),
            Self::WalFrameCorrupt {
                frame_numbers,
                seed,
            } => write!(f, "WalFrameCorrupt(frames={frame_numbers:?}, seed={seed})"),
            Self::WalTruncate { frames } => write!(f, "WalTruncate(frames={frames})"),
            Self::WalFrameBitFlip {
                frame_index,
                byte_offset_within_payload,
                bit_position,
            } => write!(
                f,
                "WalFrameBitFlip(frame={frame_index}, off={byte_offset_within_payload}, bit={bit_position})"
            ),
            Self::WalBitRot {
                frame_start,
                frame_end,
                flips,
                seed,
            } => write!(
                f,
                "WalBitRot(frames={frame_start}..={frame_end}, flips={flips}, seed={seed})"
            ),
            Self::WalTornTruncate {
                frame_index,
                bytes_into_payload,
            } => write!(
                f,
                "WalTornTruncate(frame={frame_index}, bytes_into_payload={bytes_into_payload})"
            ),
            Self::SidecarCorrupt {
                offset,
                length,
                seed,
            } => write!(f, "SidecarCorrupt(off={offset}, len={length}, seed={seed})"),
        }
    }
}

// ── CorruptionReport ────────────────────────────────────────────────────

/// A precise byte-range modification produced by an injection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CorruptionModification {
    /// File byte offset where the modification begins.
    pub offset: u64,
    /// Number of bytes modified (or removed for truncation).
    pub length: u64,
    /// First SQLite database page affected (1-indexed), if applicable.
    pub page_first: Option<u32>,
    /// Last SQLite database page affected (1-indexed), if applicable.
    pub page_last: Option<u32>,
    /// First WAL frame index affected (0-indexed), if applicable.
    pub wal_frame_first: Option<u32>,
    /// Last WAL frame index affected (0-indexed), if applicable.
    pub wal_frame_last: Option<u32>,
    /// SHA-256 of the exact byte range before mutation (or removed bytes for truncation).
    pub sha256_before: String,
    /// SHA-256 of the exact byte range after mutation (None for truncation).
    pub sha256_after: Option<String>,
}

/// Report documenting exactly what a corruption injection changed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CorruptionReport {
    /// Stable scenario id string derived from the pattern parameters.
    pub scenario_id: String,
    /// The pattern that was applied.
    pub pattern: CorruptionPattern,
    /// Precise modified ranges.
    pub modifications: Vec<CorruptionModification>,
    /// Number of bytes actually modified.
    pub affected_bytes: u64,
    /// SQLite page numbers that were affected (1-indexed).
    pub affected_pages: Vec<u32>,
    /// SHA-256 of the affected region *before* corruption.
    pub original_sha256: String,
}

// ── CorruptionInjector ──────────────────────────────────────────────────

/// Precise, deterministic corruption injector for database files.
///
/// Operates on a working copy and refuses paths inside `golden/`.
#[derive(Debug)]
pub struct CorruptionInjector {
    path: PathBuf,
    page_size: u32,
}

impl CorruptionInjector {
    /// Create a new injector targeting `path`.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::Io` if the path resolves into a `golden/` directory
    /// or the file does not exist.
    pub fn new(path: PathBuf) -> E2eResult<Self> {
        Self::with_page_size(path, DEFAULT_PAGE_SIZE)
    }

    /// Create a new injector with a custom page size.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::Io` if safety checks fail.
    pub fn with_page_size(path: PathBuf, page_size: u32) -> E2eResult<Self> {
        // Safety: refuse to operate on golden copies.
        let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
        let path_str = canonical.to_string_lossy();
        if path_str.contains("/golden/") || path_str.ends_with("/golden") {
            return Err(E2eError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("refusing to corrupt golden copy: {}", path.display()),
            )));
        }

        if !path.exists() {
            return Err(E2eError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("file not found: {}", path.display()),
            )));
        }

        Ok(Self { path, page_size })
    }

    /// Apply a single corruption pattern to the file.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::Io` on file I/O failure.
    #[allow(
        clippy::too_many_lines,
        clippy::cast_possible_truncation,
        clippy::match_same_arms
    )]
    pub fn inject(&self, pattern: &CorruptionPattern) -> E2eResult<CorruptionReport> {
        let mut data = std::fs::read(&self.path)?;
        if data.is_empty() {
            return Err(E2eError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "empty file",
            )));
        }

        let ps = self.page_size as usize;
        let scenario_id = pattern.scenario_id();

        let (affected_bytes, affected_pages, original_region, modifications) = match pattern {
            CorruptionPattern::BitFlip {
                byte_offset,
                bit_position,
            } => {
                if *bit_position >= 8 {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("bit_position {bit_position} must be in 0..=7"),
                    )));
                }

                let off = *byte_offset as usize;
                if off >= data.len() {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("byte_offset {off} exceeds file size {}", data.len()),
                    )));
                }
                let original = [data[off]];
                data[off] ^= 1 << bit_position;
                let page = (off / ps) + 1;
                let modification = CorruptionModification {
                    offset: *byte_offset,
                    length: 1,
                    page_first: Some(page as u32),
                    page_last: Some(page as u32),
                    wal_frame_first: None,
                    wal_frame_last: None,
                    sha256_before: sha256_hex(&original),
                    sha256_after: Some(sha256_hex(&[data[off]])),
                };
                (1, vec![page as u32], original.to_vec(), vec![modification])
            }

            CorruptionPattern::BitFlipMany {
                offset,
                length,
                count,
                seed,
            } => {
                let off = usize::try_from(*offset).map_err(|_| {
                    E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "offset overflow",
                    ))
                })?;
                let len = usize::try_from(*length).map_err(|_| {
                    E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "length overflow",
                    ))
                })?;
                if len == 0 {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "length must be > 0",
                    )));
                }
                let end = off.checked_add(len).ok_or_else(|| {
                    E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "offset+length overflow",
                    ))
                })?;
                if end > data.len() {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("range {off}..{end} exceeds file size {}", data.len()),
                    )));
                }

                if *count == 0 {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "count must be > 0",
                    )));
                }
                let max_unique_bits = u64::try_from(len).unwrap_or(u64::MAX).saturating_mul(8);
                if u64::from(*count) > max_unique_bits {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "count {count} exceeds max unique bits in region ({max_unique_bits})"
                        ),
                    )));
                }

                let mut rng = StdRng::seed_from_u64(*seed);
                let mut flips = BTreeSet::<(usize, u8)>::new();
                let target = usize::try_from(*count).map_err(|_| {
                    E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "count overflow",
                    ))
                })?;
                while flips.len() < target {
                    let byte_idx = off + rng.random_range(0..len);
                    let bit_idx = rng.random_range(0..8u8);
                    flips.insert((byte_idx, bit_idx));
                }

                let mut byte_indices: Vec<usize> = flips.iter().map(|(b, _)| *b).collect();
                byte_indices.sort_unstable();
                byte_indices.dedup();

                // Capture originals per contiguous range.
                let mut original_region = Vec::new();
                let mut ranges: Vec<(usize, usize, Vec<u8>)> = Vec::new(); // (start, end_exclusive, original)
                let mut i = 0usize;
                while i < byte_indices.len() {
                    let start = byte_indices[i];
                    let mut end_inclusive = start;
                    i += 1;
                    while i < byte_indices.len() && byte_indices[i] == end_inclusive + 1 {
                        end_inclusive += 1;
                        i += 1;
                    }
                    let end_exclusive = end_inclusive
                        .checked_add(1)
                        .expect("end_inclusive derived from valid index");
                    let original = data[start..end_exclusive].to_vec();
                    original_region.extend_from_slice(&original);
                    ranges.push((start, end_exclusive, original));
                }

                for (byte_idx, bit_idx) in flips {
                    data[byte_idx] ^= 1 << bit_idx;
                }

                let mut affected_pages = Vec::new();
                let mut modifications = Vec::new();
                for (start, end_exclusive, original) in ranges {
                    affected_pages.extend(pages_in_range(start, end_exclusive, ps));
                    let (page_first, page_last) = page_span_for_range(start, end_exclusive, ps);
                    modifications.push(CorruptionModification {
                        offset: u64::try_from(start).unwrap_or(u64::MAX),
                        length: u64::try_from(end_exclusive - start).unwrap_or(u64::MAX),
                        page_first,
                        page_last,
                        wal_frame_first: None,
                        wal_frame_last: None,
                        sha256_before: sha256_hex(&original),
                        sha256_after: Some(sha256_hex(&data[start..end_exclusive])),
                    });
                }
                affected_pages.sort_unstable();
                affected_pages.dedup();

                let affected_bytes = modifications.iter().map(|m| m.length).sum::<u64>();
                (
                    affected_bytes,
                    affected_pages,
                    original_region,
                    modifications,
                )
            }

            CorruptionPattern::PageZero { page_number } => {
                let Some(page_index) = page_number.checked_sub(1) else {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "page_number must be >= 1",
                    )));
                };

                let Some(start) = (page_index as usize).checked_mul(ps) else {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "page offset overflow",
                    )));
                };
                let Some(end) = start.checked_add(ps) else {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "page end overflow",
                    )));
                };
                if end > data.len() {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("page {page_number} beyond file end"),
                    )));
                }
                let original = data[start..end].to_vec();
                data[start..end].fill(0);
                let modification = CorruptionModification {
                    offset: u64::try_from(start).unwrap_or(u64::MAX),
                    length: u64::try_from(ps).unwrap_or(u64::MAX),
                    page_first: Some(*page_number),
                    page_last: Some(*page_number),
                    wal_frame_first: None,
                    wal_frame_last: None,
                    sha256_before: sha256_hex(&original),
                    sha256_after: Some(sha256_hex(&data[start..end])),
                };
                (ps as u64, vec![*page_number], original, vec![modification])
            }

            CorruptionPattern::RandomOverwrite {
                offset,
                length,
                seed,
            }
            | CorruptionPattern::SidecarCorrupt {
                offset,
                length,
                seed,
            } => {
                let off = *offset as usize;
                let Some(end) = off.checked_add(*length) else {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "offset+length overflow",
                    )));
                };
                if end > data.len() {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("range {off}..{end} exceeds file size {}", data.len()),
                    )));
                }
                let original = data[off..end].to_vec();
                let mut rng = StdRng::seed_from_u64(*seed);
                for b in &mut data[off..end] {
                    *b = rng.random();
                }
                let is_sidecar = matches!(pattern, CorruptionPattern::SidecarCorrupt { .. });
                let pages = if is_sidecar {
                    Vec::new()
                } else {
                    pages_in_range(off, end, ps)
                };
                let (page_first, page_last) = if is_sidecar {
                    (None, None)
                } else {
                    page_span_for_range(off, end, ps)
                };
                let modification = CorruptionModification {
                    offset: *offset,
                    length: u64::try_from(*length).unwrap_or(u64::MAX),
                    page_first,
                    page_last,
                    wal_frame_first: None,
                    wal_frame_last: None,
                    sha256_before: sha256_hex(&original),
                    sha256_after: Some(sha256_hex(&data[off..end])),
                };
                ((*length) as u64, pages, original, vec![modification])
            }

            CorruptionPattern::PagePartialCorrupt {
                page_number,
                offset_within_page,
                length,
                seed,
            } => {
                let Some(page_index) = page_number.checked_sub(1) else {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "page_number must be >= 1",
                    )));
                };

                let offset_within_page = usize::from(*offset_within_page);
                let length = usize::from(*length);

                if offset_within_page >= ps {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "offset_within_page exceeds page size",
                    )));
                }
                if offset_within_page.saturating_add(length) > ps {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "page partial corruption crosses page boundary",
                    )));
                }

                let Some(page_start) = (page_index as usize).checked_mul(ps) else {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "page offset overflow",
                    )));
                };
                let Some(start) = page_start.checked_add(offset_within_page) else {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "page offset overflow",
                    )));
                };
                let Some(end) = start.checked_add(length) else {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "page end overflow",
                    )));
                };
                if end > data.len() {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "page partial offset exceeds file size".to_owned(),
                    )));
                }

                let original = data[start..end].to_vec();
                let mut rng = StdRng::seed_from_u64(*seed);
                for b in &mut data[start..end] {
                    *b = rng.random();
                }
                let modification = CorruptionModification {
                    offset: u64::try_from(start).unwrap_or(u64::MAX),
                    length: u64::try_from(length).unwrap_or(u64::MAX),
                    page_first: Some(*page_number),
                    page_last: Some(*page_number),
                    wal_frame_first: None,
                    wal_frame_last: None,
                    sha256_before: sha256_hex(&original),
                    sha256_after: Some(sha256_hex(&data[start..end])),
                };
                (
                    length as u64,
                    vec![*page_number],
                    original,
                    vec![modification],
                )
            }

            CorruptionPattern::TruncateTo { new_len } => {
                let new_len_usize = usize::try_from(*new_len).map_err(|_| {
                    E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "new_len overflow",
                    ))
                })?;
                if new_len_usize >= data.len() {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("new_len {new_len} must be < file size {}", data.len()),
                    )));
                }

                let original = data[new_len_usize..].to_vec();
                let end = data.len();
                data.truncate(new_len_usize);

                let pages = pages_in_range(new_len_usize, end, ps);
                let (page_first, page_last) = page_span_for_range(new_len_usize, end, ps);
                let modification = CorruptionModification {
                    offset: *new_len,
                    length: u64::try_from(end - new_len_usize).unwrap_or(u64::MAX),
                    page_first,
                    page_last,
                    wal_frame_first: None,
                    wal_frame_last: None,
                    sha256_before: sha256_hex(&original),
                    sha256_after: None,
                };
                (
                    u64::try_from(end - new_len_usize).unwrap_or(u64::MAX),
                    pages,
                    original,
                    vec![modification],
                )
            }

            CorruptionPattern::HeaderZero => {
                let end = DB_HEADER_SIZE.min(data.len());
                let original = data[..end].to_vec();
                data[..end].fill(0);
                let (page_first, page_last) = page_span_for_range(0, end, ps);
                let modification = CorruptionModification {
                    offset: 0,
                    length: u64::try_from(end).unwrap_or(u64::MAX),
                    page_first,
                    page_last,
                    wal_frame_first: None,
                    wal_frame_last: None,
                    sha256_before: sha256_hex(&original),
                    sha256_after: Some(sha256_hex(&data[..end])),
                };
                (end as u64, vec![1], original, vec![modification])
            }

            CorruptionPattern::WalFrameCorrupt {
                frame_numbers,
                seed,
            } => {
                let mut rng = StdRng::seed_from_u64(*seed);
                let frame_size = WAL_FRAME_HEADER_SIZE + u64::from(self.page_size);
                let mut total_bytes = 0u64;
                let mut all_original = Vec::new();
                let mut affected_pages = Vec::new();
                let mut modifications = Vec::new();

                for &frame_num in frame_numbers {
                    let frame_start = WAL_HEADER_SIZE + u64::from(frame_num) * frame_size;

                    let hdr_start = usize::try_from(frame_start).map_err(|_| {
                        E2eError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "frame offset overflow",
                        ))
                    })?;
                    let hdr_end = hdr_start
                        .checked_add(WAL_FRAME_HEADER_SIZE as usize)
                        .ok_or_else(|| {
                            E2eError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "frame header end overflow",
                            ))
                        })?;
                    let data_start = hdr_end;
                    let data_end =
                        data_start
                            .checked_add(self.page_size as usize)
                            .ok_or_else(|| {
                                E2eError::Io(std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "frame data end overflow",
                                ))
                            })?;

                    if data_end > data.len() {
                        return Err(E2eError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("wal frame {frame_num} beyond file end"),
                        )));
                    }

                    // WAL frame header begins with big-endian pgno.
                    let pgno_bytes: [u8; 4] =
                        data[hdr_start..hdr_start + 4].try_into().map_err(|_| {
                            E2eError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "short wal frame header",
                            ))
                        })?;
                    let pgno = u32::from_be_bytes(pgno_bytes);
                    affected_pages.push(pgno);

                    let original = data[data_start..data_end].to_vec();
                    all_original.extend_from_slice(&original);
                    for b in &mut data[data_start..data_end] {
                        *b = rng.random();
                    }
                    modifications.push(CorruptionModification {
                        offset: u64::try_from(data_start).unwrap_or(u64::MAX),
                        length: u64::from(self.page_size),
                        page_first: Some(pgno),
                        page_last: Some(pgno),
                        wal_frame_first: Some(frame_num),
                        wal_frame_last: Some(frame_num),
                        sha256_before: sha256_hex(&original),
                        sha256_after: Some(sha256_hex(&data[data_start..data_end])),
                    });
                    total_bytes += u64::from(self.page_size);
                }

                affected_pages.sort_unstable();
                affected_pages.dedup();

                (total_bytes, affected_pages, all_original, modifications)
            }

            CorruptionPattern::WalTruncate { frames } => {
                if data.len() < usize::try_from(WAL_HEADER_SIZE).unwrap_or(usize::MAX) {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "wal file too small",
                    )));
                }

                let frame_size = WAL_FRAME_HEADER_SIZE + u64::from(self.page_size);
                let new_len = WAL_HEADER_SIZE + u64::from(*frames) * frame_size;
                let new_len_usize = usize::try_from(new_len).map_err(|_| {
                    E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "wal truncate length overflow",
                    ))
                })?;
                if new_len_usize >= data.len() {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "wal length {} already <= truncate target {new_len}",
                            data.len()
                        ),
                    )));
                }

                let original = data[new_len_usize..].to_vec();
                let original_len = data.len();

                // Determine which full frames are removed and report their db page numbers.
                let remaining = (u64::try_from(original_len).unwrap_or(u64::MAX))
                    .saturating_sub(WAL_HEADER_SIZE);
                let full_frames = remaining.checked_div(frame_size).unwrap_or(0);

                let start_frame = *frames;
                let end_frame_inclusive = if full_frames == 0 {
                    None
                } else {
                    u32::try_from(full_frames.saturating_sub(1)).ok()
                };

                let mut affected_pages = Vec::new();
                let mut page_min: Option<u32> = None;
                let mut page_max: Option<u32> = None;

                if let Some(last_full_frame) = end_frame_inclusive
                    && start_frame <= last_full_frame
                {
                    for frame_idx in start_frame..=last_full_frame {
                        let frame_start = WAL_HEADER_SIZE + u64::from(frame_idx) * frame_size;
                        let hdr_start = usize::try_from(frame_start).map_err(|_| {
                            E2eError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "frame offset overflow",
                            ))
                        })?;
                        if hdr_start + 4 <= original_len {
                            let pgno = u32::from_be_bytes(
                                data[hdr_start..hdr_start + 4]
                                    .try_into()
                                    .unwrap_or([0_u8; 4]),
                            );
                            if pgno != 0 {
                                affected_pages.push(pgno);
                                page_min = Some(page_min.map_or(pgno, |p| p.min(pgno)));
                                page_max = Some(page_max.map_or(pgno, |p| p.max(pgno)));
                            }
                        }
                    }
                }

                affected_pages.sort_unstable();
                affected_pages.dedup();

                data.truncate(new_len_usize);

                let modification = CorruptionModification {
                    offset: new_len,
                    length: u64::try_from(original_len - new_len_usize).unwrap_or(u64::MAX),
                    page_first: page_min,
                    page_last: page_max,
                    wal_frame_first: Some(*frames),
                    wal_frame_last: end_frame_inclusive,
                    sha256_before: sha256_hex(&original),
                    sha256_after: None,
                };
                (
                    u64::try_from(original_len - new_len_usize).unwrap_or(u64::MAX),
                    affected_pages,
                    original,
                    vec![modification],
                )
            }

            CorruptionPattern::WalFrameBitFlip {
                frame_index,
                byte_offset_within_payload,
                bit_position,
            } => {
                if *bit_position >= 8 {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("bit_position {bit_position} must be in 0..=7"),
                    )));
                }
                if *byte_offset_within_payload >= self.page_size {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "byte_offset_within_payload exceeds page size",
                    )));
                }

                let frame_size = WAL_FRAME_HEADER_SIZE + u64::from(self.page_size);
                let frame_start = WAL_HEADER_SIZE + u64::from(*frame_index) * frame_size;
                let hdr_start = usize::try_from(frame_start).map_err(|_| {
                    E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "frame offset overflow",
                    ))
                })?;
                let hdr_end = hdr_start
                    .checked_add(WAL_FRAME_HEADER_SIZE as usize)
                    .ok_or_else(|| {
                        E2eError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "frame header end overflow",
                        ))
                    })?;
                let payload_start = hdr_end;
                let payload_end = payload_start
                    .checked_add(self.page_size as usize)
                    .ok_or_else(|| {
                        E2eError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "frame payload end overflow",
                        ))
                    })?;
                if payload_end > data.len() {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("wal frame {frame_index} beyond file end"),
                    )));
                }

                let pgno = if hdr_start + 4 <= data.len() {
                    u32::from_be_bytes(
                        data[hdr_start..hdr_start + 4]
                            .try_into()
                            .unwrap_or([0_u8; 4]),
                    )
                } else {
                    0
                };

                let payload_off = usize::try_from(*byte_offset_within_payload).map_err(|_| {
                    E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "payload offset overflow",
                    ))
                })?;
                let byte_idx = payload_start + payload_off;
                if byte_idx >= payload_end {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "payload byte index out of range",
                    )));
                }

                let original = [data[byte_idx]];
                data[byte_idx] ^= 1 << *bit_position;

                let modification = CorruptionModification {
                    offset: u64::try_from(byte_idx).unwrap_or(u64::MAX),
                    length: 1,
                    page_first: if pgno == 0 { None } else { Some(pgno) },
                    page_last: if pgno == 0 { None } else { Some(pgno) },
                    wal_frame_first: Some(*frame_index),
                    wal_frame_last: Some(*frame_index),
                    sha256_before: sha256_hex(&original),
                    sha256_after: Some(sha256_hex(&[data[byte_idx]])),
                };
                (
                    1,
                    if pgno == 0 { Vec::new() } else { vec![pgno] },
                    original.to_vec(),
                    vec![modification],
                )
            }

            CorruptionPattern::WalBitRot {
                frame_start,
                frame_end,
                flips,
                seed,
            } => {
                if frame_start > frame_end {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "frame_start must be <= frame_end",
                    )));
                }
                if *flips == 0 {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "flips must be > 0",
                    )));
                }

                let frame_size = WAL_FRAME_HEADER_SIZE + u64::from(self.page_size);
                let total_len = u64::try_from(data.len()).unwrap_or(u64::MAX);
                let remaining = total_len.saturating_sub(WAL_HEADER_SIZE);
                let full_frames = remaining.checked_div(frame_size).unwrap_or(0);
                if full_frames == 0 {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "wal contains no full frames",
                    )));
                }
                let last_full_frame =
                    u32::try_from(full_frames.saturating_sub(1)).map_err(|_| {
                        E2eError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "frame index overflow",
                        ))
                    })?;
                if *frame_end > last_full_frame {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("frame_end {frame_end} exceeds last full frame {last_full_frame}"),
                    )));
                }

                let frames_count = u64::from(*frame_end - *frame_start + 1);
                let bits_per_frame = u64::from(self.page_size).saturating_mul(8);
                let max_bits = frames_count.saturating_mul(bits_per_frame);
                if u64::from(*flips) > max_bits {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("flips {flips} exceeds max unique bits in range ({max_bits})"),
                    )));
                }

                let mut rng = StdRng::seed_from_u64(*seed);
                let mut flip_map: BTreeMap<u32, BTreeSet<(usize, u8)>> = BTreeMap::new();
                let target = usize::try_from(*flips).map_err(|_| {
                    E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "flips overflow",
                    ))
                })?;
                while flip_map.values().map(BTreeSet::len).sum::<usize>() < target {
                    let frame_idx = rng.random_range(*frame_start..=*frame_end);
                    let byte_off = rng.random_range(0..(self.page_size as usize));
                    let bit_idx = rng.random_range(0..8u8);
                    flip_map
                        .entry(frame_idx)
                        .or_default()
                        .insert((byte_off, bit_idx));
                }

                let mut modifications = Vec::new();
                let mut all_original = Vec::new();
                let mut affected_pages = Vec::new();

                for (&frame_idx, flips) in &flip_map {
                    let frame_start_off = WAL_HEADER_SIZE + u64::from(frame_idx) * frame_size;
                    let hdr_start = usize::try_from(frame_start_off).map_err(|_| {
                        E2eError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "frame offset overflow",
                        ))
                    })?;
                    let hdr_end = hdr_start + WAL_FRAME_HEADER_SIZE as usize;
                    let payload_start = hdr_end;
                    let payload_end = payload_start + self.page_size as usize;
                    if payload_end > data.len() {
                        return Err(E2eError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "wal frame beyond file end",
                        )));
                    }

                    let pgno = if hdr_start + 4 <= data.len() {
                        u32::from_be_bytes(
                            data[hdr_start..hdr_start + 4]
                                .try_into()
                                .unwrap_or([0_u8; 4]),
                        )
                    } else {
                        0
                    };
                    if pgno != 0 {
                        affected_pages.push(pgno);
                    }

                    let mut byte_indices: Vec<usize> = flips.iter().map(|(b, _)| *b).collect();
                    byte_indices.sort_unstable();
                    byte_indices.dedup();

                    // Capture originals per contiguous range before mutation.
                    let mut ranges: Vec<(usize, usize, Vec<u8>)> = Vec::new(); // (abs_start, abs_end_exclusive, original)
                    let mut i = 0usize;
                    while i < byte_indices.len() {
                        let start = byte_indices[i];
                        let mut end_inclusive = start;
                        i += 1;
                        while i < byte_indices.len() && byte_indices[i] == end_inclusive + 1 {
                            end_inclusive += 1;
                            i += 1;
                        }
                        let abs_start = payload_start + start;
                        let abs_end_exclusive = payload_start + end_inclusive + 1;
                        let original = data[abs_start..abs_end_exclusive].to_vec();
                        all_original.extend_from_slice(&original);
                        ranges.push((abs_start, abs_end_exclusive, original));
                    }

                    for (byte_off, bit_idx) in flips {
                        let idx = payload_start + *byte_off;
                        data[idx] ^= 1 << *bit_idx;
                    }

                    for (abs_start, abs_end_exclusive, original) in ranges {
                        modifications.push(CorruptionModification {
                            offset: u64::try_from(abs_start).unwrap_or(u64::MAX),
                            length: u64::try_from(abs_end_exclusive - abs_start)
                                .unwrap_or(u64::MAX),
                            page_first: if pgno == 0 { None } else { Some(pgno) },
                            page_last: if pgno == 0 { None } else { Some(pgno) },
                            wal_frame_first: Some(frame_idx),
                            wal_frame_last: Some(frame_idx),
                            sha256_before: sha256_hex(&original),
                            sha256_after: Some(sha256_hex(&data[abs_start..abs_end_exclusive])),
                        });
                    }
                }

                affected_pages.sort_unstable();
                affected_pages.dedup();

                let affected_bytes = modifications.iter().map(|m| m.length).sum::<u64>();
                (affected_bytes, affected_pages, all_original, modifications)
            }

            CorruptionPattern::WalTornTruncate {
                frame_index,
                bytes_into_payload,
            } => {
                if *bytes_into_payload >= self.page_size {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "bytes_into_payload exceeds page size",
                    )));
                }

                let frame_size = WAL_FRAME_HEADER_SIZE + u64::from(self.page_size);
                let new_len = WAL_HEADER_SIZE
                    + u64::from(*frame_index) * frame_size
                    + WAL_FRAME_HEADER_SIZE
                    + u64::from(*bytes_into_payload);
                let new_len_usize = usize::try_from(new_len).map_err(|_| {
                    E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "wal torn truncate length overflow",
                    ))
                })?;
                if new_len_usize >= data.len() {
                    return Err(E2eError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("wal length {} already <= new_len {new_len}", data.len()),
                    )));
                }

                let original = data[new_len_usize..].to_vec();
                let original_len = data.len();
                data.truncate(new_len_usize);

                let modification = CorruptionModification {
                    offset: new_len,
                    length: u64::try_from(original_len - new_len_usize).unwrap_or(u64::MAX),
                    page_first: None,
                    page_last: None,
                    wal_frame_first: Some(*frame_index),
                    wal_frame_last: None,
                    sha256_before: sha256_hex(&original),
                    sha256_after: None,
                };
                (
                    u64::try_from(original_len - new_len_usize).unwrap_or(u64::MAX),
                    Vec::new(),
                    original,
                    vec![modification],
                )
            }
        };

        std::fs::write(&self.path, &data)?;

        Ok(CorruptionReport {
            scenario_id,
            pattern: pattern.clone(),
            modifications,
            affected_bytes,
            affected_pages,
            original_sha256: sha256_hex(&original_region),
        })
    }

    /// Apply multiple corruption patterns sequentially.
    ///
    /// # Errors
    ///
    /// Returns the first error encountered; earlier patterns may have already
    /// been applied.
    pub fn inject_many(&self, patterns: &[CorruptionPattern]) -> E2eResult<Vec<CorruptionReport>> {
        let mut reports = Vec::with_capacity(patterns.len());
        for p in patterns {
            reports.push(self.inject(p)?);
        }
        Ok(reports)
    }

    /// Path this injector operates on.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Page size used for page-level calculations.
    #[must_use]
    pub const fn page_size(&self) -> u32 {
        self.page_size
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Compute which pages a byte range `[start..end)` spans.
#[allow(clippy::cast_possible_truncation)]
fn pages_in_range(start: usize, end: usize, page_size: usize) -> Vec<u32> {
    if start >= end || page_size == 0 {
        return Vec::new();
    }
    let first_page = (start / page_size) + 1;
    let last_page = ((end - 1) / page_size) + 1;
    (first_page..=last_page).map(|p| p as u32).collect()
}

fn page_span_for_range(start: usize, end: usize, page_size: usize) -> (Option<u32>, Option<u32>) {
    if start >= end || page_size == 0 {
        return (None, None);
    }
    let first_page = (start / page_size) + 1;
    let last_page = ((end - 1) / page_size) + 1;
    let first = u32::try_from(first_page).ok();
    let last = u32::try_from(last_page).ok();
    (first, last)
}

/// SHA-256 hex digest of a byte slice.
fn sha256_hex(data: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(data);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn sanitize_scenario_id(raw: &str) -> String {
    // ASCII-only, stable, filesystem-safe.
    let mut out = String::with_capacity(raw.len().min(80));
    let mut prev_sep = false;

    for ch in raw.chars() {
        let lc = ch.to_ascii_lowercase();
        let keep = match lc {
            'a'..='z' | '0'..='9' | '-' | '_' => Some(lc),
            _ => None,
        };

        if let Some(c) = keep {
            if (c == '-' || c == '_') && (out.is_empty() || prev_sep) {
                continue;
            }
            out.push(c);
            prev_sep = c == '-' || c == '_';
        } else if !out.is_empty() && !prev_sep {
            out.push('_');
            prev_sep = true;
        }

        if out.len() >= 80 {
            break;
        }
    }

    while out.ends_with('_') || out.ends_with('-') {
        out.pop();
    }
    out
}

fn format_u32_ranges(values: &[u32]) -> String {
    use std::fmt::Write as _;
    if values.is_empty() {
        return String::new();
    }

    let mut v = values.to_vec();
    v.sort_unstable();
    v.dedup();

    let mut out = String::new();
    let mut i = 0usize;
    while i < v.len() {
        let start = v[i];
        let mut end = start;
        i += 1;
        while i < v.len() && v[i] == end.saturating_add(1) {
            end = v[i];
            i += 1;
        }

        if !out.is_empty() {
            out.push(',');
        }
        if start == end {
            let _ = write!(out, "{start}");
        } else {
            let _ = write!(out, "{start}-{end}");
        }
    }
    out
}

impl CorruptionPattern {
    /// Stable, filesystem-safe scenario id string derived from this pattern.
    #[must_use]
    pub fn scenario_id(&self) -> String {
        let raw = match self {
            Self::BitFlip {
                byte_offset,
                bit_position,
            } => format!("bitflip_byte_{byte_offset}_bit_{bit_position}"),
            Self::BitFlipMany {
                offset,
                length,
                count,
                seed,
            } => format!("bitflip_off_{offset}_len_{length}_count_{count}_seed_{seed}"),
            Self::PageZero { page_number } => format!("page_zero_pg_{page_number}"),
            Self::RandomOverwrite {
                offset,
                length,
                seed,
            } => format!("rand_overwrite_off_{offset}_len_{length}_seed_{seed}"),
            Self::PagePartialCorrupt {
                page_number,
                offset_within_page,
                length,
                seed,
            } => format!(
                "page_partial_pg_{page_number}_off_{offset_within_page}_len_{length}_seed_{seed}"
            ),
            Self::TruncateTo { new_len } => format!("truncate_to_{new_len}"),
            Self::HeaderZero => "header_zero".to_owned(),
            Self::WalFrameCorrupt {
                frame_numbers,
                seed,
            } => {
                let frames = format_u32_ranges(frame_numbers);
                format!("wal_frame_corrupt_frames_{frames}_seed_{seed}")
            }
            Self::WalTruncate { frames } => format!("wal_truncate_frames_{frames}"),
            Self::WalFrameBitFlip {
                frame_index,
                byte_offset_within_payload,
                bit_position,
            } => format!(
                "wal_bitflip_frame_{frame_index}_off_{byte_offset_within_payload}_bit_{bit_position}"
            ),
            Self::WalBitRot {
                frame_start,
                frame_end,
                flips,
                seed,
            } => format!("wal_bitrot_{frame_start}_{frame_end}_flips_{flips}_seed_{seed}"),
            Self::WalTornTruncate {
                frame_index,
                bytes_into_payload,
            } => format!("wal_torn_truncate_frame_{frame_index}_bytes_{bytes_into_payload}"),
            Self::SidecarCorrupt {
                offset,
                length,
                seed,
            } => format!("sidecar_corrupt_off_{offset}_len_{length}_seed_{seed}"),
        };

        sanitize_scenario_id(&raw)
    }
}

// ── Corruption Strategy Catalog (bd-2als.4.1) ───────────────────────────

/// Category of file targeted by a corruption strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CorruptionCategory {
    /// The main `.db` database file.
    DatabaseFile,
    /// The WAL journal (`.db-wal`).
    Wal,
    /// The WAL-FEC sidecar (`.db-wal-fec`).
    Sidecar,
}

impl fmt::Display for CorruptionCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DatabaseFile => f.write_str("database_file"),
            Self::Wal => f.write_str("wal"),
            Self::Sidecar => f.write_str("sidecar"),
        }
    }
}

/// Impact severity of a corruption strategy.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum CorruptionSeverity {
    /// Subtle — may evade detection without checksums (e.g., single bit flip).
    Subtle,
    /// Moderate — detectable and potentially recoverable with FEC.
    Moderate,
    /// Severe — major structural damage; recovery unlikely without redundancy.
    Severe,
}

impl fmt::Display for CorruptionSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Subtle => f.write_str("subtle"),
            Self::Moderate => f.write_str("moderate"),
            Self::Severe => f.write_str("severe"),
        }
    }
}

/// A single entry in the corruption strategy catalog.
///
/// Each entry describes one deterministic corruption technique with enough
/// metadata for downstream runners (bd-2als.4.2, bd-2als.4.3) to discover,
/// filter, and instantiate strategies.
///
/// The catalog is the "menu" from which the corruption demo runner and CI
/// smoke select strategies to apply.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CatalogEntry {
    /// Stable, filesystem-safe identifier (e.g., `"header_zero"`).
    pub strategy_id: String,
    /// Human-readable name for reports.
    pub name: String,
    /// Narrative description of what this strategy does.
    pub description: String,
    /// Which file type this strategy targets.
    pub category: CorruptionCategory,
    /// Expected impact severity.
    pub severity: CorruptionSeverity,
    /// Default seed for reproducible corruption.
    pub default_seed: u64,
    /// The concrete pattern with default parameterization.
    pub pattern: CorruptionPattern,
}

impl CatalogEntry {
    /// Apply this catalog entry to a file, returning a structured report.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::Io` if the file cannot be read, written, or fails
    /// safety checks (golden path).
    pub fn inject(&self, path: &Path, page_size: u32) -> E2eResult<CorruptionReport> {
        let injector = CorruptionInjector::with_page_size(path.to_path_buf(), page_size)?;
        injector.inject(&self.pattern)
    }
}

/// Return the complete catalog of corruption strategies.
///
/// This is the canonical "menu" for the demo runner (bd-2als.4.4) and CI
/// smoke (bd-2als.6.4).  Each entry carries default parameters and a seed;
/// callers can override the seed or adjust parameters as needed.
///
/// Strategies are organized by category:
/// - **Database file**: header, page, bitflip, truncation
/// - **WAL**: frame corruption, truncation, bitrot, torn writes
/// - **Sidecar**: FEC repair symbol corruption
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn corruption_strategy_catalog() -> Vec<CatalogEntry> {
    vec![
        // ── Database file strategies ────────────────────────────────────
        CatalogEntry {
            strategy_id: "header_zero".to_owned(),
            name: "Header Zero".to_owned(),
            description: "Zero the 100-byte SQLite database header. \
                Prevents file format recognition; sqlite3 cannot open the file."
                .to_owned(),
            category: CorruptionCategory::DatabaseFile,
            severity: CorruptionSeverity::Severe,
            default_seed: 0,
            pattern: CorruptionPattern::HeaderZero,
        },
        CatalogEntry {
            strategy_id: "page_zero_pg2".to_owned(),
            name: "Page Zero (page 2)".to_owned(),
            description: "Zero an entire database page (default: page 2, first data page). \
                Destroys B-tree structure for that page; sqlite3 integrity_check fails."
                .to_owned(),
            category: CorruptionCategory::DatabaseFile,
            severity: CorruptionSeverity::Severe,
            default_seed: 0,
            pattern: CorruptionPattern::PageZero { page_number: 2 },
        },
        CatalogEntry {
            strategy_id: "bitflip_db_single".to_owned(),
            name: "Single Bit Flip (DB)".to_owned(),
            description: "Flip one bit in the database file (byte 200, bit 3). \
                Subtle corruption that may go undetected without page checksums."
                .to_owned(),
            category: CorruptionCategory::DatabaseFile,
            severity: CorruptionSeverity::Subtle,
            default_seed: 0,
            pattern: CorruptionPattern::BitFlip {
                byte_offset: 200,
                bit_position: 3,
            },
        },
        CatalogEntry {
            strategy_id: "bitflip_db_region".to_owned(),
            name: "Regional Bit Flips (DB)".to_owned(),
            description: "Flip 8 random bits within a 512-byte region starting at byte 4096 \
                (page 2). Simulates localized bitrot affecting multiple cells on one page."
                .to_owned(),
            category: CorruptionCategory::DatabaseFile,
            severity: CorruptionSeverity::Moderate,
            default_seed: 42,
            pattern: CorruptionPattern::BitFlipMany {
                offset: 4096,
                length: 512,
                count: 8,
                seed: 42,
            },
        },
        CatalogEntry {
            strategy_id: "truncate_db_half".to_owned(),
            name: "Truncate DB (50%)".to_owned(),
            description: "Truncate the database file to half its expected size. \
                Simulates catastrophic storage failure; multiple pages lost."
                .to_owned(),
            category: CorruptionCategory::DatabaseFile,
            severity: CorruptionSeverity::Severe,
            default_seed: 0,
            // The actual new_len must be computed at runtime from file size.
            // This default targets a 2-page (8192-byte) DB truncated to 1 page.
            pattern: CorruptionPattern::TruncateTo { new_len: 4096 },
        },
        CatalogEntry {
            strategy_id: "page_partial_pg2".to_owned(),
            name: "Partial Page Corrupt (page 2)".to_owned(),
            description: "Overwrite 128 bytes within page 2 with random data. \
                Corrupts B-tree cell content while leaving the page header intact."
                .to_owned(),
            category: CorruptionCategory::DatabaseFile,
            severity: CorruptionSeverity::Moderate,
            default_seed: 77,
            pattern: CorruptionPattern::PagePartialCorrupt {
                page_number: 2,
                offset_within_page: 64,
                length: 128,
                seed: 77,
            },
        },
        // ── WAL strategies ──────────────────────────────────────────────
        CatalogEntry {
            strategy_id: "wal_truncate_0".to_owned(),
            name: "WAL Truncate (0 frames)".to_owned(),
            description: "Truncate the WAL to just its header (0 frames kept). \
                All committed-but-uncheckpointed data is lost."
                .to_owned(),
            category: CorruptionCategory::Wal,
            severity: CorruptionSeverity::Severe,
            default_seed: 0,
            pattern: CorruptionPattern::WalTruncate { frames: 0 },
        },
        CatalogEntry {
            strategy_id: "wal_frame_corrupt_0_1".to_owned(),
            name: "WAL Frame Corrupt (frames 0-1)".to_owned(),
            description: "Overwrite the payload of the first two WAL frames with random data. \
                The cumulative checksum chain breaks; sqlite3 discards from frame 0."
                .to_owned(),
            category: CorruptionCategory::Wal,
            severity: CorruptionSeverity::Moderate,
            default_seed: 42,
            pattern: CorruptionPattern::WalFrameCorrupt {
                frame_numbers: vec![0, 1],
                seed: 42,
            },
        },
        CatalogEntry {
            strategy_id: "wal_bitrot_0_3".to_owned(),
            name: "WAL Bitrot (frames 0-3)".to_owned(),
            description: "Flip 5 random bits across WAL frames 0 through 3. \
                Simulates gradual storage degradation in the WAL region."
                .to_owned(),
            category: CorruptionCategory::Wal,
            severity: CorruptionSeverity::Moderate,
            default_seed: 99,
            pattern: CorruptionPattern::WalBitRot {
                frame_start: 0,
                frame_end: 3,
                flips: 5,
                seed: 99,
            },
        },
        CatalogEntry {
            strategy_id: "wal_bitflip_frame0".to_owned(),
            name: "WAL Single Bit Flip (frame 0)".to_owned(),
            description: "Flip one bit in the first WAL frame's payload. \
                Minimal corruption that breaks the checksum chain at the earliest point."
                .to_owned(),
            category: CorruptionCategory::Wal,
            severity: CorruptionSeverity::Subtle,
            default_seed: 0,
            pattern: CorruptionPattern::WalFrameBitFlip {
                frame_index: 0,
                byte_offset_within_payload: 100,
                bit_position: 3,
            },
        },
        CatalogEntry {
            strategy_id: "wal_torn_write_frame1".to_owned(),
            name: "WAL Torn Write (frame 1)".to_owned(),
            description: "Simulate a torn write by truncating the WAL mid-frame (1024 bytes \
                into frame 1's payload). Represents power loss during a write."
                .to_owned(),
            category: CorruptionCategory::Wal,
            severity: CorruptionSeverity::Moderate,
            default_seed: 0,
            pattern: CorruptionPattern::WalTornTruncate {
                frame_index: 1,
                bytes_into_payload: 1024,
            },
        },
        // ── Sidecar strategies ──────────────────────────────────────────
        CatalogEntry {
            strategy_id: "sidecar_corrupt_symbols".to_owned(),
            name: "FEC Sidecar Corrupt".to_owned(),
            description: "Overwrite 512 bytes of the WAL-FEC sidecar starting at offset 64. \
                Destroys repair symbols; FrankenSQLite falls back to WAL truncation."
                .to_owned(),
            category: CorruptionCategory::Sidecar,
            severity: CorruptionSeverity::Moderate,
            default_seed: 55,
            pattern: CorruptionPattern::SidecarCorrupt {
                offset: 64,
                length: 512,
                seed: 55,
            },
        },
    ]
}

/// Filter the catalog to only database file strategies.
#[must_use]
pub fn db_file_strategies() -> Vec<CatalogEntry> {
    corruption_strategy_catalog()
        .into_iter()
        .filter(|e| e.category == CorruptionCategory::DatabaseFile)
        .collect()
}

/// Filter the catalog to only WAL strategies.
#[must_use]
pub fn wal_strategies() -> Vec<CatalogEntry> {
    corruption_strategy_catalog()
        .into_iter()
        .filter(|e| e.category == CorruptionCategory::Wal)
        .collect()
}

/// Filter the catalog to only sidecar strategies.
#[must_use]
pub fn sidecar_strategies() -> Vec<CatalogEntry> {
    corruption_strategy_catalog()
        .into_iter()
        .filter(|e| e.category == CorruptionCategory::Sidecar)
        .collect()
}

/// Filter the catalog to strategies at or above a severity threshold.
#[must_use]
pub fn strategies_by_severity(min_severity: CorruptionSeverity) -> Vec<CatalogEntry> {
    corruption_strategy_catalog()
        .into_iter()
        .filter(|e| e.severity >= min_severity)
        .collect()
}

/// Look up a single catalog entry by its `strategy_id`.
#[must_use]
pub fn catalog_entry_by_id(strategy_id: &str) -> Option<CatalogEntry> {
    corruption_strategy_catalog()
        .into_iter()
        .find(|e| e.strategy_id == strategy_id)
}

// ── Legacy API (preserved for backward compat) ──────────────────────────

/// Legacy corruption strategy enum.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum CorruptionStrategy {
    /// Flip random bits in the file.
    RandomBitFlip { count: usize },
    /// Zero out a range of bytes at the given offset.
    ZeroRange { offset: usize, length: usize },
    /// Corrupt an entire page (4096-byte aligned, 1-indexed page number).
    PageCorrupt { page_number: u32 },
}

/// Apply a legacy corruption strategy to a database file.
///
/// # Errors
///
/// Returns `E2eError::Io` if the file cannot be read or written.
#[allow(clippy::cast_possible_truncation)]
pub fn inject_corruption(path: &Path, strategy: CorruptionStrategy, seed: u64) -> E2eResult<()> {
    let mut data = std::fs::read(path)?;
    if data.is_empty() {
        return Err(E2eError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "empty file",
        )));
    }

    let mut rng = StdRng::seed_from_u64(seed);

    match strategy {
        CorruptionStrategy::RandomBitFlip { count } => {
            for _ in 0..count {
                let byte_idx = rng.random_range(0..data.len());
                let bit_idx = rng.random_range(0..8u8);
                data[byte_idx] ^= 1 << bit_idx;
            }
        }
        CorruptionStrategy::ZeroRange { offset, length } => {
            let end = (offset + length).min(data.len());
            let start = offset.min(data.len());
            for byte in &mut data[start..end] {
                *byte = 0;
            }
        }
        CorruptionStrategy::PageCorrupt { page_number } => {
            let page_size = 4096usize;

            let Some(page_index) = page_number.checked_sub(1) else {
                return Err(E2eError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "page_number must be >= 1",
                )));
            };

            let Some(start) = (page_index as usize).checked_mul(page_size) else {
                return Err(E2eError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "page offset overflow",
                )));
            };
            let Some(end) = start.checked_add(page_size) else {
                return Err(E2eError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "page end overflow",
                )));
            };
            if end > data.len() {
                return Err(E2eError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("page {page_number} beyond file end"),
                )));
            }

            for byte in &mut data[start..end] {
                *byte = rng.random();
            }
        }
    }

    std::fs::write(path, &data)?;
    Ok(())
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db(size: usize) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("work").join("test.db");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, vec![0xAA_u8; size]).unwrap();
        (dir, path)
    }

    fn temp_db_filled(size: usize, fill: u8) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("work").join("test.db");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, vec![fill; size]).unwrap();
        (dir, path)
    }

    // -- CorruptionInjector tests --

    #[test]
    fn test_golden_path_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden").join("test.db");
        std::fs::create_dir_all(golden.parent().unwrap()).unwrap();
        std::fs::write(&golden, [0u8; 4096]).unwrap();

        let result = CorruptionInjector::new(golden);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("golden"), "expected golden rejection: {err}");
    }

    #[test]
    fn test_nonexistent_file_rejected() {
        let result = CorruptionInjector::new(PathBuf::from("/tmp/nonexistent_corruption_test.db"));
        assert!(result.is_err());
    }

    #[test]
    fn test_bit_flip_single() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let report = injector
            .inject(&CorruptionPattern::BitFlip {
                byte_offset: 100,
                bit_position: 3,
            })
            .unwrap();

        assert_eq!(report.affected_bytes, 1);
        assert_eq!(report.affected_pages, vec![1]);

        let data = std::fs::read(&path).unwrap();
        // 0xAA = 0b10101010, flipping bit 3 → 0b10100010 = 0xA2
        assert_eq!(data[100], 0xA2);
    }

    #[test]
    fn test_bit_flip_all_positions() {
        let (_dir, path) = temp_db_filled(4096, 0);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        for bit_position in 0u8..8 {
            std::fs::write(&path, vec![0u8; 4096]).unwrap();
            injector
                .inject(&CorruptionPattern::BitFlip {
                    byte_offset: 10,
                    bit_position,
                })
                .unwrap();
            let data = std::fs::read(&path).unwrap();
            assert_eq!(data[10], 1u8 << bit_position, "bit_position={bit_position}");
        }
    }

    #[test]
    fn test_bit_flip_preserves_surrounding_bytes() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let original = std::fs::read(&path).unwrap();
        injector
            .inject(&CorruptionPattern::BitFlip {
                byte_offset: 100,
                bit_position: 0,
            })
            .unwrap();
        let mutated = std::fs::read(&path).unwrap();

        assert_ne!(mutated[100], original[100]);
        let mut original2 = original;
        original2[100] = mutated[100];
        assert_eq!(mutated, original2, "only the targeted byte should differ");
    }

    #[test]
    fn test_bit_flip_idempotent_when_applied_twice() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let original = std::fs::read(&path).unwrap();
        let pattern = CorruptionPattern::BitFlip {
            byte_offset: 123,
            bit_position: 4,
        };

        injector.inject(&pattern).unwrap();
        injector.inject(&pattern).unwrap();

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data, original, "double-flip should restore original");
    }

    #[test]
    fn test_bit_flip_at_file_boundary() {
        let size = 8192;
        let (_dir, path) = temp_db(size);
        let injector = CorruptionInjector::new(path).unwrap();

        let last = u64::try_from(size - 1).unwrap();
        let report = injector
            .inject(&CorruptionPattern::BitFlip {
                byte_offset: last,
                bit_position: 0,
            })
            .unwrap();

        assert_eq!(report.affected_bytes, 1);
        assert_eq!(report.affected_pages, vec![2]);
    }

    #[test]
    fn test_bit_flip_rejects_invalid_bit_position() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path).unwrap();
        let err = injector
            .inject(&CorruptionPattern::BitFlip {
                byte_offset: 0,
                bit_position: 8,
            })
            .unwrap_err();
        assert!(err.to_string().contains("bit_position"));
    }

    #[test]
    fn test_bit_flip_rejects_out_of_bounds_offset() {
        let size = 4096;
        let (_dir, path) = temp_db(size);
        let injector = CorruptionInjector::new(path).unwrap();
        let err = injector
            .inject(&CorruptionPattern::BitFlip {
                byte_offset: u64::try_from(size).unwrap(),
                bit_position: 0,
            })
            .unwrap_err();
        assert!(err.to_string().contains("exceeds file size"));
    }

    #[test]
    fn test_page_zero() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let report = injector
            .inject(&CorruptionPattern::PageZero { page_number: 2 })
            .unwrap();

        assert_eq!(report.affected_bytes, 4096);
        assert_eq!(report.affected_pages, vec![2]);

        let data = std::fs::read(&path).unwrap();
        assert!(data[4096..8192].iter().all(|&b| b == 0));
        assert!(data[0..4096].iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn test_page_zero_first_page() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        injector
            .inject(&CorruptionPattern::PageZero { page_number: 1 })
            .unwrap();

        let data = std::fs::read(&path).unwrap();
        assert!(data[0..4096].iter().all(|&b| b == 0));
        assert!(data[4096..8192].iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn test_page_zero_last_page() {
        let (_dir, path) = temp_db(3 * 4096);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        injector
            .inject(&CorruptionPattern::PageZero { page_number: 3 })
            .unwrap();

        let data = std::fs::read(&path).unwrap();
        assert!(data[0..8192].iter().all(|&b| b == 0xAA));
        assert!(data[8192..].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_page_zero_out_of_range_rejected() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path).unwrap();
        let err = injector
            .inject(&CorruptionPattern::PageZero { page_number: 3 })
            .unwrap_err();
        assert!(err.to_string().contains("beyond file end"));
    }

    #[test]
    fn test_random_overwrite_deterministic() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        injector
            .inject(&CorruptionPattern::RandomOverwrite {
                offset: 200,
                length: 50,
                seed: 77,
            })
            .unwrap();
        let c1 = std::fs::read(&path).unwrap();

        // Reset and re-corrupt
        std::fs::write(&path, vec![0xAA_u8; 8192]).unwrap();
        injector
            .inject(&CorruptionPattern::RandomOverwrite {
                offset: 200,
                length: 50,
                seed: 77,
            })
            .unwrap();
        let c2 = std::fs::read(&path).unwrap();

        assert_eq!(c1, c2, "same seed must produce identical corruption");
    }

    #[test]
    fn test_random_overwrite_different_seeds_produce_different_bytes() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        injector
            .inject(&CorruptionPattern::RandomOverwrite {
                offset: 200,
                length: 64,
                seed: 1,
            })
            .unwrap();
        let a = std::fs::read(&path).unwrap();

        std::fs::write(&path, vec![0xAA_u8; 8192]).unwrap();
        injector
            .inject(&CorruptionPattern::RandomOverwrite {
                offset: 200,
                length: 64,
                seed: 2,
            })
            .unwrap();
        let b = std::fs::read(&path).unwrap();

        assert_ne!(a[200..264], b[200..264]);
    }

    #[test]
    fn test_random_overwrite_out_of_range_rejected() {
        let (_dir, path) = temp_db(1024);
        let injector = CorruptionInjector::new(path).unwrap();

        let err = injector
            .inject(&CorruptionPattern::RandomOverwrite {
                offset: 900,
                length: 200,
                seed: 0,
            })
            .unwrap_err();
        assert!(err.to_string().contains("exceeds file size"));
    }

    #[test]
    fn test_random_overwrite_reports_affected_pages_span() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path).unwrap();

        let report = injector
            .inject(&CorruptionPattern::RandomOverwrite {
                offset: 4090,
                length: 20,
                seed: 123,
            })
            .unwrap();

        assert_eq!(report.affected_pages, vec![1, 2]);
    }

    #[test]
    fn test_page_partial_corrupt() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let report = injector
            .inject(&CorruptionPattern::PagePartialCorrupt {
                page_number: 1,
                offset_within_page: 10,
                length: 20,
                seed: 42,
            })
            .unwrap();

        assert_eq!(report.affected_bytes, 20);
        assert_eq!(report.affected_pages, vec![1]);

        let data = std::fs::read(&path).unwrap();
        // Bytes 0..10 should be untouched
        assert!(data[0..10].iter().all(|&b| b == 0xAA));
        // Bytes 10..30 should be different from original 0xAA
        assert!(data[10..30].iter().any(|&b| b != 0xAA));
        // Bytes 30..4096 should be untouched
        assert!(data[30..4096].iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn test_page_partial_corrupt_at_page_boundary() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let report = injector
            .inject(&CorruptionPattern::PagePartialCorrupt {
                page_number: 1,
                offset_within_page: 4096 - 20,
                length: 20,
                seed: 7,
            })
            .unwrap();

        assert_eq!(report.affected_bytes, 20);
        assert_eq!(report.affected_pages, vec![1]);
        let data = std::fs::read(&path).unwrap();
        assert!(data[..(4096 - 20)].iter().all(|&b| b == 0xAA));
        assert!(data[(4096 - 20)..].iter().any(|&b| b != 0xAA));
    }

    #[test]
    fn test_page_partial_corrupt_deterministic() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        injector
            .inject(&CorruptionPattern::PagePartialCorrupt {
                page_number: 1,
                offset_within_page: 10,
                length: 20,
                seed: 42,
            })
            .unwrap();
        let a = std::fs::read(&path).unwrap();

        std::fs::write(&path, vec![0xAA_u8; 4096]).unwrap();
        injector
            .inject(&CorruptionPattern::PagePartialCorrupt {
                page_number: 1,
                offset_within_page: 10,
                length: 20,
                seed: 42,
            })
            .unwrap();
        let b = std::fs::read(&path).unwrap();

        assert_eq!(a, b);
    }

    #[test]
    fn test_page_partial_corrupt_different_seeds_produce_different_bytes() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        injector
            .inject(&CorruptionPattern::PagePartialCorrupt {
                page_number: 1,
                offset_within_page: 100,
                length: 32,
                seed: 1,
            })
            .unwrap();
        let a = std::fs::read(&path).unwrap();

        std::fs::write(&path, vec![0xAA_u8; 4096]).unwrap();
        injector
            .inject(&CorruptionPattern::PagePartialCorrupt {
                page_number: 1,
                offset_within_page: 100,
                length: 32,
                seed: 2,
            })
            .unwrap();
        let b = std::fs::read(&path).unwrap();

        assert_ne!(a[100..132], b[100..132]);
    }

    #[test]
    fn test_page_partial_corrupt_rejects_cross_page_boundary() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path).unwrap();

        let err = injector
            .inject(&CorruptionPattern::PagePartialCorrupt {
                page_number: 1,
                offset_within_page: 4090,
                length: 10,
                seed: 0,
            })
            .unwrap_err();
        assert!(err.to_string().contains("crosses page boundary"));
    }

    #[test]
    fn test_page_partial_corrupt_rejects_offset_out_of_range() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path).unwrap();

        let err = injector
            .inject(&CorruptionPattern::PagePartialCorrupt {
                page_number: 1,
                offset_within_page: 4096,
                length: 1,
                seed: 0,
            })
            .unwrap_err();
        assert!(err.to_string().contains("offset_within_page"));
    }

    #[test]
    fn test_header_zero() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let report = injector.inject(&CorruptionPattern::HeaderZero).unwrap();

        assert_eq!(report.affected_bytes, 100);
        assert_eq!(report.affected_pages, vec![1]);

        let data = std::fs::read(&path).unwrap();
        assert!(data[..100].iter().all(|&b| b == 0));
        assert!(data[100..].iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn test_header_zero_sqlite_magic_gone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("work").join("header.db");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();
        drop(conn);

        let injector = CorruptionInjector::new(path.clone()).unwrap();
        injector.inject(&CorruptionPattern::HeaderZero).unwrap();

        let data = std::fs::read(&path).unwrap();
        assert_ne!(&data[..16], b"SQLite format 3\0");
        assert!(data[..16].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_header_zero_makes_database_unopenable_by_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("work").join("broken.db");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();
        drop(conn);

        let injector = CorruptionInjector::new(path.clone()).unwrap();
        injector.inject(&CorruptionPattern::HeaderZero).unwrap();

        let flags =
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let reopened = rusqlite::Connection::open_with_flags(&path, flags).unwrap();
        let res: Result<String, _> = reopened.query_row("PRAGMA integrity_check", [], |r| r.get(0));
        assert!(
            res.is_err(),
            "expected integrity_check to fail on header-zero DB"
        );
    }

    #[test]
    fn test_wal_frame_corrupt() {
        // Simulate a WAL file: 32-byte header + 2 frames of (24 + 4096) bytes each
        let frame_size = 24 + 4096;
        let wal_size = 32 + 2 * frame_size;
        let (_dir, path) = temp_db(wal_size);
        // Write recognizable pgno values into each frame header (big-endian).
        let mut wal = std::fs::read(&path).unwrap();
        // Frame 0 header starts at 32
        wal[32..36].copy_from_slice(&1u32.to_be_bytes());
        // Frame 1 header starts at 32 + frame_size
        let frame1 = 32 + frame_size;
        wal[frame1..frame1 + 4].copy_from_slice(&2u32.to_be_bytes());
        std::fs::write(&path, &wal).unwrap();

        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let report = injector
            .inject(&CorruptionPattern::WalFrameCorrupt {
                frame_numbers: vec![0, 1],
                seed: 99,
            })
            .unwrap();

        assert_eq!(report.affected_pages, vec![1, 2]);
        assert_eq!(report.affected_bytes, 2 * 4096);

        let data = std::fs::read(&path).unwrap();
        // WAL header (first 32 bytes) should be untouched
        assert!(data[..32].iter().all(|&b| b == 0xAA));
        // Frame header pgno should remain intact (we only corrupt frame data).
        assert_eq!(&data[32..36], &1u32.to_be_bytes());
        assert_eq!(&data[frame1..frame1 + 4], &2u32.to_be_bytes());
    }

    #[test]
    fn test_wal_frame_corrupt_single_frame_only() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + 2 * frame_size;
        let (_dir, path) = temp_db(wal_size);

        let mut wal = std::fs::read(&path).unwrap();
        wal[32..36].copy_from_slice(&1u32.to_be_bytes());
        let frame1 = 32 + frame_size;
        wal[frame1..frame1 + 4].copy_from_slice(&2u32.to_be_bytes());
        std::fs::write(&path, &wal).unwrap();

        let injector = CorruptionInjector::new(path.clone()).unwrap();

        injector
            .inject(&CorruptionPattern::WalFrameCorrupt {
                frame_numbers: vec![0],
                seed: 77,
            })
            .unwrap();

        let data = std::fs::read(&path).unwrap();
        let frame0_data_start = 32 + 24;
        let frame0_data_end = frame0_data_start + 4096;
        assert!(
            data[frame0_data_start..frame0_data_end]
                .iter()
                .any(|&b| b != 0xAA)
        );

        let frame1_data_start = frame1 + 24;
        let frame1_data_end = frame1_data_start + 4096;
        assert!(
            data[frame1_data_start..frame1_data_end]
                .iter()
                .all(|&b| b == 0xAA)
        );
    }

    #[test]
    fn test_wal_frame_corrupt_deterministic() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + 2 * frame_size;
        let (_dir, path) = temp_db(wal_size);

        let mut wal = std::fs::read(&path).unwrap();
        wal[32..36].copy_from_slice(&1u32.to_be_bytes());
        let frame1 = 32 + frame_size;
        wal[frame1..frame1 + 4].copy_from_slice(&2u32.to_be_bytes());
        std::fs::write(&path, &wal).unwrap();

        let injector = CorruptionInjector::new(path.clone()).unwrap();
        let pattern = CorruptionPattern::WalFrameCorrupt {
            frame_numbers: vec![0, 1],
            seed: 99,
        };

        injector.inject(&pattern).unwrap();
        let a = std::fs::read(&path).unwrap();

        std::fs::write(&path, &wal).unwrap();
        injector.inject(&pattern).unwrap();
        let b = std::fs::read(&path).unwrap();

        assert_eq!(a, b);
    }

    #[test]
    fn test_wal_frame_corrupt_out_of_range_rejected() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + frame_size;
        let (_dir, path) = temp_db(wal_size);

        let mut wal = std::fs::read(&path).unwrap();
        wal[32..36].copy_from_slice(&1u32.to_be_bytes());
        std::fs::write(&path, &wal).unwrap();

        let injector = CorruptionInjector::new(path).unwrap();
        let err = injector
            .inject(&CorruptionPattern::WalFrameCorrupt {
                frame_numbers: vec![1],
                seed: 0,
            })
            .unwrap_err();
        assert!(err.to_string().contains("beyond file end"));
    }

    #[test]
    fn test_sidecar_corrupt() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let report = injector
            .inject(&CorruptionPattern::SidecarCorrupt {
                offset: 1000,
                length: 200,
                seed: 33,
            })
            .unwrap();

        assert_eq!(report.affected_bytes, 200);
        let data = std::fs::read(&path).unwrap();
        assert!(data[1000..1200].iter().any(|&b| b != 0xAA));
    }

    #[test]
    fn test_inject_many() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path).unwrap();

        let patterns = vec![
            CorruptionPattern::BitFlip {
                byte_offset: 0,
                bit_position: 0,
            },
            CorruptionPattern::PageZero { page_number: 2 },
        ];

        let reports = injector.inject_many(&patterns).unwrap();
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].affected_bytes, 1);
        assert_eq!(reports[1].affected_bytes, 4096);
    }

    #[test]
    fn test_report_captures_original_sha256() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path).unwrap();

        let report = injector.inject(&CorruptionPattern::HeaderZero).unwrap();

        // Original was 100 bytes of 0xAA — verify the hash is non-empty
        assert!(!report.original_sha256.is_empty());
        assert_eq!(report.original_sha256.len(), 64); // SHA-256 hex = 64 chars
    }

    #[test]
    fn test_report_original_sha256_matches_expected() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path).unwrap();

        let report = injector.inject(&CorruptionPattern::HeaderZero).unwrap();

        let expected = sha256_hex(&[0xAA_u8; 100]);
        assert_eq!(report.original_sha256, expected);
    }

    #[test]
    fn test_inject_many_applies_patterns_sequentially() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let patterns = vec![
            CorruptionPattern::BitFlip {
                byte_offset: 0,
                bit_position: 0,
            },
            CorruptionPattern::PageZero { page_number: 2 },
        ];

        injector.inject_many(&patterns).unwrap();
        let data = std::fs::read(&path).unwrap();

        assert_eq!(data[0], 0xAB, "0xAA ^ 1 = 0xAB");
        assert!(data[4096..8192].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_pages_in_range() {
        assert_eq!(pages_in_range(0, 4096, 4096), vec![1]);
        assert_eq!(pages_in_range(0, 4097, 4096), vec![1, 2]);
        assert_eq!(pages_in_range(4096, 8192, 4096), vec![2]);
        assert_eq!(pages_in_range(100, 100, 4096), Vec::<u32>::new());
    }

    // -- TruncateTo tests --

    #[test]
    fn test_truncate_to() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let report = injector
            .inject(&CorruptionPattern::TruncateTo { new_len: 4096 })
            .unwrap();

        assert_eq!(report.affected_bytes, 4096);
        assert_eq!(report.affected_pages, vec![2]);

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 4096);
        assert!(data.iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn test_truncate_to_partial_page() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let report = injector
            .inject(&CorruptionPattern::TruncateTo { new_len: 2048 })
            .unwrap();

        assert_eq!(report.affected_bytes, 6144);
        assert!(report.affected_pages.contains(&1));
        assert!(report.affected_pages.contains(&2));

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 2048);
    }

    #[test]
    fn test_truncate_to_rejects_no_op() {
        let size = 4096;
        let (_dir, path) = temp_db(size);
        let injector = CorruptionInjector::new(path).unwrap();

        let err = injector
            .inject(&CorruptionPattern::TruncateTo {
                new_len: u64::try_from(size).unwrap(),
            })
            .unwrap_err();
        assert!(err.to_string().contains("must be <"));
    }

    // -- BitFlipMany tests --

    #[test]
    fn test_bit_flip_many_deterministic() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let pattern = CorruptionPattern::BitFlipMany {
            offset: 100,
            length: 200,
            count: 10,
            seed: 42,
        };

        injector.inject(&pattern).unwrap();
        let a = std::fs::read(&path).unwrap();

        std::fs::write(&path, vec![0xAA_u8; 8192]).unwrap();
        injector.inject(&pattern).unwrap();
        let b = std::fs::read(&path).unwrap();

        assert_eq!(a, b, "same seed must produce identical corruption");
    }

    #[test]
    fn test_bit_flip_many_modifies_correct_region() {
        let (_dir, path) = temp_db(8192);
        let injector = CorruptionInjector::new(path.clone()).unwrap();

        let report = injector
            .inject(&CorruptionPattern::BitFlipMany {
                offset: 100,
                length: 50,
                count: 5,
                seed: 77,
            })
            .unwrap();

        let data = std::fs::read(&path).unwrap();

        // Region before should be untouched
        assert!(data[..100].iter().all(|&b| b == 0xAA));
        // Region after should be untouched
        assert!(data[150..].iter().all(|&b| b == 0xAA));
        // Some bytes in region should be modified
        assert!(data[100..150].iter().any(|&b| b != 0xAA));
        // Report should be non-empty
        assert!(!report.modifications.is_empty());
    }

    #[test]
    fn test_bit_flip_many_rejects_zero_count() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path).unwrap();

        let err = injector
            .inject(&CorruptionPattern::BitFlipMany {
                offset: 0,
                length: 100,
                count: 0,
                seed: 0,
            })
            .unwrap_err();
        assert!(err.to_string().contains("count must be > 0"));
    }

    #[test]
    fn test_bit_flip_many_rejects_out_of_range() {
        let (_dir, path) = temp_db(4096);
        let injector = CorruptionInjector::new(path).unwrap();

        let err = injector
            .inject(&CorruptionPattern::BitFlipMany {
                offset: 4000,
                length: 200,
                count: 5,
                seed: 0,
            })
            .unwrap_err();
        assert!(err.to_string().contains("exceeds file size"));
    }

    // -- WalTruncate tests --

    #[test]
    fn test_wal_truncate() {
        // WAL: 32-byte header + 3 frames of (24 + 4096)
        let frame_size = 24 + 4096;
        let wal_size = 32 + 3 * frame_size;
        let (_dir, path) = temp_db(wal_size);

        // Write recognizable pgno values
        let mut wal = std::fs::read(&path).unwrap();
        for i in 0u32..3 {
            let frame_start = 32 + (i as usize) * frame_size;
            wal[frame_start..frame_start + 4].copy_from_slice(&(i + 1).to_be_bytes());
        }
        std::fs::write(&path, &wal).unwrap();

        let injector = CorruptionInjector::new(path.clone()).unwrap();
        let report = injector
            .inject(&CorruptionPattern::WalTruncate { frames: 1 })
            .unwrap();

        // Should have truncated 2 frames
        assert_eq!(
            report.affected_bytes,
            u64::try_from(2 * frame_size).unwrap()
        );
        assert!(report.affected_pages.contains(&2));
        assert!(report.affected_pages.contains(&3));

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 32 + frame_size);
    }

    #[test]
    fn test_wal_truncate_to_zero_frames() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + 2 * frame_size;
        let (_dir, path) = temp_db(wal_size);

        let mut wal = std::fs::read(&path).unwrap();
        wal[32..36].copy_from_slice(&1u32.to_be_bytes());
        let f1 = 32 + frame_size;
        wal[f1..f1 + 4].copy_from_slice(&2u32.to_be_bytes());
        std::fs::write(&path, &wal).unwrap();

        let injector = CorruptionInjector::new(path.clone()).unwrap();
        injector
            .inject(&CorruptionPattern::WalTruncate { frames: 0 })
            .unwrap();

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 32, "should only keep WAL header");
    }

    #[test]
    fn test_wal_truncate_rejects_no_op() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + 2 * frame_size;
        let (_dir, path) = temp_db(wal_size);

        let injector = CorruptionInjector::new(path).unwrap();
        let err = injector
            .inject(&CorruptionPattern::WalTruncate { frames: 2 })
            .unwrap_err();
        assert!(err.to_string().contains("already <="));
    }

    // -- WalFrameBitFlip tests --

    #[test]
    fn test_wal_frame_bit_flip() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + 2 * frame_size;
        let (_dir, path) = temp_db_filled(wal_size, 0);

        // Write recognizable pgno for frame 0
        let mut wal = std::fs::read(&path).unwrap();
        wal[32..36].copy_from_slice(&5u32.to_be_bytes());
        std::fs::write(&path, &wal).unwrap();

        let injector = CorruptionInjector::new(path.clone()).unwrap();
        let report = injector
            .inject(&CorruptionPattern::WalFrameBitFlip {
                frame_index: 0,
                byte_offset_within_payload: 100,
                bit_position: 3,
            })
            .unwrap();

        assert_eq!(report.affected_bytes, 1);
        assert_eq!(report.affected_pages, vec![5]);

        // Payload byte at offset 100 within frame 0 should have bit 3 flipped
        let data = std::fs::read(&path).unwrap();
        let payload_start = 32 + 24; // WAL header + frame header
        assert_eq!(data[payload_start + 100], 1 << 3);
    }

    #[test]
    fn test_wal_frame_bit_flip_rejects_bad_bit() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + frame_size;
        let (_dir, path) = temp_db(wal_size);

        let injector = CorruptionInjector::new(path).unwrap();
        let err = injector
            .inject(&CorruptionPattern::WalFrameBitFlip {
                frame_index: 0,
                byte_offset_within_payload: 0,
                bit_position: 8,
            })
            .unwrap_err();
        assert!(err.to_string().contains("bit_position"));
    }

    #[test]
    fn test_wal_frame_bit_flip_rejects_bad_offset() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + frame_size;
        let (_dir, path) = temp_db(wal_size);

        let injector = CorruptionInjector::new(path).unwrap();
        let err = injector
            .inject(&CorruptionPattern::WalFrameBitFlip {
                frame_index: 0,
                byte_offset_within_payload: 4096,
                bit_position: 0,
            })
            .unwrap_err();
        assert!(err.to_string().contains("exceeds page size"));
    }

    // -- WalBitRot tests --

    #[test]
    fn test_wal_bit_rot_deterministic() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + 4 * frame_size;
        let (_dir, path) = temp_db(wal_size);

        // Write pgno values
        let mut wal = std::fs::read(&path).unwrap();
        for i in 0u32..4 {
            let fs = 32 + (i as usize) * frame_size;
            wal[fs..fs + 4].copy_from_slice(&(i + 1).to_be_bytes());
        }
        std::fs::write(&path, &wal).unwrap();

        let pattern = CorruptionPattern::WalBitRot {
            frame_start: 0,
            frame_end: 3,
            flips: 10,
            seed: 42,
        };

        let injector = CorruptionInjector::new(path.clone()).unwrap();
        injector.inject(&pattern).unwrap();
        let a = std::fs::read(&path).unwrap();

        // Reset and re-corrupt
        std::fs::write(&path, &wal).unwrap();
        injector.inject(&pattern).unwrap();
        let b = std::fs::read(&path).unwrap();

        assert_eq!(a, b, "same seed must produce identical corruption");
    }

    #[test]
    fn test_wal_bit_rot_reports_affected_pages() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + 2 * frame_size;
        let (_dir, path) = temp_db(wal_size);

        let mut wal = std::fs::read(&path).unwrap();
        wal[32..36].copy_from_slice(&7u32.to_be_bytes());
        let f1 = 32 + frame_size;
        wal[f1..f1 + 4].copy_from_slice(&8u32.to_be_bytes());
        std::fs::write(&path, &wal).unwrap();

        let injector = CorruptionInjector::new(path).unwrap();
        let report = injector
            .inject(&CorruptionPattern::WalBitRot {
                frame_start: 0,
                frame_end: 1,
                flips: 5,
                seed: 99,
            })
            .unwrap();

        // Should report the db page numbers from the WAL frame headers
        for page in &report.affected_pages {
            assert!(*page == 7 || *page == 8);
        }
    }

    #[test]
    fn test_wal_bit_rot_rejects_zero_flips() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + frame_size;
        let (_dir, path) = temp_db(wal_size);

        let injector = CorruptionInjector::new(path).unwrap();
        let err = injector
            .inject(&CorruptionPattern::WalBitRot {
                frame_start: 0,
                frame_end: 0,
                flips: 0,
                seed: 0,
            })
            .unwrap_err();
        assert!(err.to_string().contains("flips must be > 0"));
    }

    #[test]
    fn test_wal_bit_rot_rejects_inverted_range() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + 2 * frame_size;
        let (_dir, path) = temp_db(wal_size);

        let injector = CorruptionInjector::new(path).unwrap();
        let err = injector
            .inject(&CorruptionPattern::WalBitRot {
                frame_start: 1,
                frame_end: 0,
                flips: 1,
                seed: 0,
            })
            .unwrap_err();
        assert!(err.to_string().contains("frame_start must be <= frame_end"));
    }

    // -- WalTornTruncate tests --

    #[test]
    fn test_wal_torn_truncate() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + 2 * frame_size;
        let (_dir, path) = temp_db(wal_size);

        let injector = CorruptionInjector::new(path.clone()).unwrap();
        let report = injector
            .inject(&CorruptionPattern::WalTornTruncate {
                frame_index: 1,
                bytes_into_payload: 512,
            })
            .unwrap();

        // Should truncate from the middle of frame 1's payload
        let expected_len = 32 + frame_size + 24 + 512;
        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), expected_len);
        assert!(report.affected_bytes > 0);
    }

    #[test]
    fn test_wal_torn_truncate_at_payload_start() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + 2 * frame_size;
        let (_dir, path) = temp_db(wal_size);

        let injector = CorruptionInjector::new(path.clone()).unwrap();
        injector
            .inject(&CorruptionPattern::WalTornTruncate {
                frame_index: 0,
                bytes_into_payload: 0,
            })
            .unwrap();

        // Should truncate right after frame 0's header (no payload written)
        let expected_len = 32 + 24; // WAL header + frame header
        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), expected_len);
    }

    #[test]
    fn test_wal_torn_truncate_rejects_payload_overflow() {
        let frame_size = 24 + 4096;
        let wal_size = 32 + frame_size;
        let (_dir, path) = temp_db(wal_size);

        let injector = CorruptionInjector::new(path).unwrap();
        let err = injector
            .inject(&CorruptionPattern::WalTornTruncate {
                frame_index: 0,
                bytes_into_payload: 4096, // == page_size, must be < page_size
            })
            .unwrap_err();
        assert!(err.to_string().contains("exceeds page size"));
    }

    // -- Scenario id tests --

    #[test]
    fn test_scenario_id_is_filesystem_safe() {
        let patterns = [
            CorruptionPattern::BitFlip {
                byte_offset: 42,
                bit_position: 3,
            },
            CorruptionPattern::BitFlipMany {
                offset: 0,
                length: 100,
                count: 5,
                seed: 99,
            },
            CorruptionPattern::PageZero { page_number: 2 },
            CorruptionPattern::TruncateTo { new_len: 1024 },
            CorruptionPattern::HeaderZero,
            CorruptionPattern::WalTruncate { frames: 3 },
            CorruptionPattern::WalBitRot {
                frame_start: 0,
                frame_end: 2,
                flips: 8,
                seed: 42,
            },
            CorruptionPattern::WalTornTruncate {
                frame_index: 1,
                bytes_into_payload: 512,
            },
        ];

        for p in &patterns {
            let id = p.scenario_id();
            assert!(!id.is_empty(), "scenario_id must not be empty for {p}");
            assert!(
                id.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
                "scenario_id must be filesystem-safe: {id}"
            );
            assert!(
                !id.starts_with('_') && !id.starts_with('-'),
                "scenario_id must not start with separator: {id}"
            );
            assert!(
                !id.ends_with('_') && !id.ends_with('-'),
                "scenario_id must not end with separator: {id}"
            );
        }
    }

    // -- Legacy tests --

    #[test]
    fn test_random_bit_flip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let original = vec![0u8; 4096];
        std::fs::write(&path, &original).unwrap();

        inject_corruption(&path, CorruptionStrategy::RandomBitFlip { count: 10 }, 42).unwrap();

        let corrupted = std::fs::read(&path).unwrap();
        assert_ne!(original, corrupted, "corruption should modify the file");
        assert_eq!(original.len(), corrupted.len(), "size should be unchanged");
    }

    #[test]
    fn test_zero_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, [0xFF_u8; 1024]).unwrap();

        inject_corruption(
            &path,
            CorruptionStrategy::ZeroRange {
                offset: 100,
                length: 50,
            },
            0,
        )
        .unwrap();

        let data = std::fs::read(&path).unwrap();
        assert!(data[100..150].iter().all(|&b| b == 0));
        assert!(data[0..100].iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn test_page_corrupt_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, [0u8; 8192]).unwrap();

        inject_corruption(
            &path,
            CorruptionStrategy::PageCorrupt { page_number: 1 },
            99,
        )
        .unwrap();
        let c1 = std::fs::read(&path).unwrap();

        std::fs::write(&path, [0u8; 8192]).unwrap();
        inject_corruption(
            &path,
            CorruptionStrategy::PageCorrupt { page_number: 1 },
            99,
        )
        .unwrap();
        let c2 = std::fs::read(&path).unwrap();

        assert_eq!(c1, c2, "same seed must produce identical corruption");
    }

    // ── Catalog tests (bd-2als.4.1) ─────────────────────────────────

    #[test]
    fn test_catalog_nonempty() {
        let catalog = corruption_strategy_catalog();
        assert!(
            catalog.len() >= 12,
            "expected at least 12 catalog entries, got {}",
            catalog.len()
        );
    }

    #[test]
    fn test_catalog_unique_strategy_ids() {
        let catalog = corruption_strategy_catalog();
        let mut ids: Vec<&str> = catalog.iter().map(|e| e.strategy_id.as_str()).collect();
        let original_len = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(
            ids.len(),
            original_len,
            "catalog has duplicate strategy_ids"
        );
    }

    #[test]
    fn test_catalog_scenario_ids_filesystem_safe() {
        let catalog = corruption_strategy_catalog();
        for entry in &catalog {
            let sid = entry.pattern.scenario_id();
            assert!(
                !sid.is_empty(),
                "empty scenario_id for {}",
                entry.strategy_id
            );
            assert!(
                sid.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
                "non-filesystem-safe scenario_id for {}: {sid}",
                entry.strategy_id
            );
        }
    }

    #[test]
    fn test_catalog_covers_all_categories() {
        let catalog = corruption_strategy_catalog();
        let has_db = catalog
            .iter()
            .any(|e| e.category == CorruptionCategory::DatabaseFile);
        let has_wal = catalog
            .iter()
            .any(|e| e.category == CorruptionCategory::Wal);
        let has_sidecar = catalog
            .iter()
            .any(|e| e.category == CorruptionCategory::Sidecar);
        assert!(has_db, "catalog missing DatabaseFile category");
        assert!(has_wal, "catalog missing Wal category");
        assert!(has_sidecar, "catalog missing Sidecar category");
    }

    #[test]
    fn test_catalog_covers_all_severities() {
        let catalog = corruption_strategy_catalog();
        let has_subtle = catalog
            .iter()
            .any(|e| e.severity == CorruptionSeverity::Subtle);
        let has_moderate = catalog
            .iter()
            .any(|e| e.severity == CorruptionSeverity::Moderate);
        let has_severe = catalog
            .iter()
            .any(|e| e.severity == CorruptionSeverity::Severe);
        assert!(has_subtle, "catalog missing Subtle severity");
        assert!(has_moderate, "catalog missing Moderate severity");
        assert!(has_severe, "catalog missing Severe severity");
    }

    #[test]
    fn test_catalog_filter_by_category() {
        let db = db_file_strategies();
        assert!(
            db.iter()
                .all(|e| e.category == CorruptionCategory::DatabaseFile)
        );
        assert!(!db.is_empty());

        let wal = wal_strategies();
        assert!(wal.iter().all(|e| e.category == CorruptionCategory::Wal));
        assert!(!wal.is_empty());

        let sc = sidecar_strategies();
        assert!(sc.iter().all(|e| e.category == CorruptionCategory::Sidecar));
        assert!(!sc.is_empty());
    }

    #[test]
    fn test_catalog_filter_by_severity() {
        let severe = strategies_by_severity(CorruptionSeverity::Severe);
        assert!(
            severe
                .iter()
                .all(|e| e.severity >= CorruptionSeverity::Severe)
        );
        assert!(!severe.is_empty());

        let moderate_plus = strategies_by_severity(CorruptionSeverity::Moderate);
        assert!(moderate_plus.len() >= severe.len());
    }

    #[test]
    fn test_catalog_lookup_by_id() {
        let entry = catalog_entry_by_id("header_zero");
        assert!(entry.is_some(), "expected to find header_zero in catalog");
        let entry = entry.unwrap();
        assert_eq!(entry.strategy_id, "header_zero");
        assert_eq!(entry.category, CorruptionCategory::DatabaseFile);

        let missing = catalog_entry_by_id("nonexistent_strategy");
        assert!(missing.is_none());
    }

    #[test]
    fn test_catalog_entry_inject_header_zero() {
        let (_dir, path) = temp_db(8192);
        let entry = catalog_entry_by_id("header_zero").unwrap();
        let report = entry.inject(&path, DEFAULT_PAGE_SIZE).unwrap();
        assert_eq!(report.affected_bytes, 100);
        assert_eq!(report.affected_pages, vec![1]);

        let data = std::fs::read(&path).unwrap();
        assert!(data[..100].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_catalog_entry_inject_rejects_golden() {
        let dir = tempfile::tempdir().unwrap();
        let golden = dir.path().join("golden").join("test.db");
        std::fs::create_dir_all(golden.parent().unwrap()).unwrap();
        std::fs::write(&golden, [0u8; 4096]).unwrap();

        let entry = catalog_entry_by_id("header_zero").unwrap();
        assert!(entry.inject(&golden, DEFAULT_PAGE_SIZE).is_err());
    }

    #[test]
    fn test_catalog_deterministic() {
        let (_dir, path) = temp_db(8192);
        let entry = catalog_entry_by_id("bitflip_db_region").unwrap();

        let r1 = entry.inject(&path, DEFAULT_PAGE_SIZE).unwrap();
        let d1 = std::fs::read(&path).unwrap();

        // Reset and re-inject
        std::fs::write(&path, vec![0xAA_u8; 8192]).unwrap();
        let r2 = entry.inject(&path, DEFAULT_PAGE_SIZE).unwrap();
        let d2 = std::fs::read(&path).unwrap();

        assert_eq!(
            d1, d2,
            "same catalog entry must produce identical corruption"
        );
        assert_eq!(r1.scenario_id, r2.scenario_id);
    }
}
