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

use std::fmt;
use std::path::{Path, PathBuf};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
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
    /// Zero out the 100-byte database header (page 1, offset 0..100).
    HeaderZero,
    /// Corrupt specific WAL frames with seeded random data.
    ///
    /// Note: `frame_numbers` are 0-indexed (first frame starts at offset 32).
    WalFrameCorrupt { frame_numbers: Vec<u32>, seed: u64 },
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
            Self::HeaderZero => write!(f, "HeaderZero"),
            Self::WalFrameCorrupt {
                frame_numbers,
                seed,
            } => write!(f, "WalFrameCorrupt(frames={frame_numbers:?}, seed={seed})"),
            Self::SidecarCorrupt {
                offset,
                length,
                seed,
            } => write!(f, "SidecarCorrupt(off={offset}, len={length}, seed={seed})"),
        }
    }
}

// ── CorruptionReport ────────────────────────────────────────────────────

/// Report documenting exactly what a corruption injection changed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CorruptionReport {
    /// The pattern that was applied.
    pub pattern: CorruptionPattern,
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

        let (affected_bytes, affected_pages, original_region) = match pattern {
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
                let original = vec![data[off]];
                data[off] ^= 1 << bit_position;
                let page = (off / ps) + 1;
                (1, vec![page as u32], original)
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
                (ps as u64, vec![*page_number], original)
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
                    *b = rng.r#gen();
                }
                let pages = pages_in_range(off, end, ps);
                ((*length) as u64, pages, original)
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
                    *b = rng.r#gen();
                }
                (length as u64, vec![*page_number], original)
            }

            CorruptionPattern::HeaderZero => {
                let end = DB_HEADER_SIZE.min(data.len());
                let original = data[..end].to_vec();
                data[..end].fill(0);
                (end as u64, vec![1], original)
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

                    all_original.extend_from_slice(&data[data_start..data_end]);
                    for b in &mut data[data_start..data_end] {
                        *b = rng.r#gen();
                    }
                    total_bytes += u64::from(self.page_size);
                }

                affected_pages.sort_unstable();
                affected_pages.dedup();

                (total_bytes, affected_pages, all_original)
            }
        };

        std::fs::write(&self.path, &data)?;

        Ok(CorruptionReport {
            pattern: pattern.clone(),
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

// ── Legacy API (preserved for backward compat) ──────────────────────────

/// Legacy corruption strategy enum.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum CorruptionStrategy {
    /// Flip random bits in the file.
    RandomBitFlip { count: usize },
    /// Zero out a range of bytes at the given offset.
    ZeroRange { offset: usize, length: usize },
    /// Corrupt an entire page (4096-byte aligned).
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
                let byte_idx = rng.gen_range(0..data.len());
                let bit_idx = rng.gen_range(0..8u8);
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
            let start = page_number as usize * page_size;
            let end = (start + page_size).min(data.len());
            if start < data.len() {
                for byte in &mut data[start..end] {
                    *byte = rng.r#gen();
                }
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
    fn test_pages_in_range() {
        assert_eq!(pages_in_range(0, 4096, 4096), vec![1]);
        assert_eq!(pages_in_range(0, 4097, 4096), vec![1, 2]);
        assert_eq!(pages_in_range(4096, 8192, 4096), vec![2]);
        assert_eq!(pages_in_range(100, 100, 4096), Vec::<u32>::new());
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
}
