//! Core WAL file I/O layer.
//!
//! Provides [`WalFile`], a VFS-backed abstraction over the SQLite WAL file format.
//! Handles WAL creation, frame append with rolling checksum chain, frame reads,
//! validation, and reset for checkpoint.
//!
//! The on-disk layout is:
//! ```text
//! [WAL Header: 32 bytes]
//! [Frame 0: 24-byte header + page_size bytes]
//! [Frame 1: 24-byte header + page_size bytes]
//! ...
//! [Frame N: 24-byte header + page_size bytes]
//! ```

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::SyncFlags;
use fsqlite_vfs::VfsFile;
use tracing::{debug, error};

use crate::checksum::{
    SqliteWalChecksum, WAL_FORMAT_VERSION, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WAL_MAGIC_LE,
    WalFrameHeader, WalHeader, WalSalts, compute_wal_frame_checksum, read_wal_header_checksum,
    write_wal_frame_checksum, write_wal_frame_salts,
};

/// A WAL file backed by a VFS file handle.
///
/// Manages the write-ahead log: creation, sequential frame append with
/// checksum chain integrity, frame reads, and reset after checkpoint.
pub struct WalFile<F: VfsFile> {
    file: F,
    page_size: usize,
    big_endian_checksum: bool,
    header: WalHeader,
    /// Rolling checksum from the last written/validated frame (or header if empty).
    running_checksum: SqliteWalChecksum,
    /// Number of valid frames currently in the WAL.
    frame_count: usize,
}

impl<F: VfsFile> WalFile<F> {
    /// Size in bytes of a single frame (header + page data).
    #[must_use]
    pub fn frame_size(&self) -> usize {
        WAL_FRAME_HEADER_SIZE + self.page_size
    }

    /// Byte offset of frame `index` (0-based) within the WAL file.
    #[allow(clippy::cast_possible_truncation)]
    fn frame_offset(&self, index: usize) -> u64 {
        // Compute in u64 to prevent usize overflow on 32-bit targets.
        WAL_HEADER_SIZE as u64 + index as u64 * self.frame_size() as u64
    }

    /// Number of valid frames in the WAL.
    #[must_use]
    pub fn frame_count(&self) -> usize {
        self.frame_count
    }

    /// The parsed WAL header.
    #[must_use]
    pub fn header(&self) -> &WalHeader {
        &self.header
    }

    /// Database page size in bytes.
    #[must_use]
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Whether the WAL uses big-endian checksum words.
    #[must_use]
    pub fn big_endian_checksum(&self) -> bool {
        self.big_endian_checksum
    }

    /// The current rolling checksum (after the last valid frame, or header seed).
    #[must_use]
    pub fn running_checksum(&self) -> SqliteWalChecksum {
        self.running_checksum
    }

    /// Create a new WAL file, writing the 32-byte header.
    ///
    /// The file should already be opened via the VFS. This overwrites any
    /// existing content by writing the header at offset 0 and truncating.
    pub fn create(
        cx: &Cx,
        mut file: F,
        page_size: u32,
        checkpoint_seq: u32,
        salts: WalSalts,
    ) -> Result<Self> {
        let header = WalHeader {
            magic: WAL_MAGIC_LE,
            format_version: WAL_FORMAT_VERSION,
            page_size,
            checkpoint_seq,
            salts,
            checksum: SqliteWalChecksum::default(), // computed by to_bytes()
        };
        let header_bytes = header.to_bytes()?;
        file.write(cx, &header_bytes, 0)?;
        file.truncate(
            cx,
            u64::try_from(WAL_HEADER_SIZE).expect("header size fits u64"),
        )?;

        let running_checksum = read_wal_header_checksum(&header_bytes)?;

        debug!(
            page_size,
            checkpoint_seq,
            salt1 = header.salts.salt1,
            salt2 = header.salts.salt2,
            "WAL file created"
        );

        Ok(Self {
            file,
            page_size: usize::try_from(page_size).expect("page size fits usize"),
            big_endian_checksum: false,
            header,
            running_checksum,
            frame_count: 0,
        })
    }

    /// Open an existing WAL file by reading and validating its header,
    /// then scanning frames to determine the valid frame count and
    /// running checksum.
    #[allow(clippy::too_many_lines)]
    pub fn open(cx: &Cx, mut file: F) -> Result<Self> {
        // Read and parse the 32-byte header.
        let mut header_buf = [0u8; WAL_HEADER_SIZE];
        let bytes_read = file.read(cx, &mut header_buf, 0)?;
        if bytes_read < WAL_HEADER_SIZE {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WAL file too small for header: read {bytes_read}, need {WAL_HEADER_SIZE}"
                ),
            });
        }
        let header = WalHeader::from_bytes(&header_buf)?;
        let page_size = usize::try_from(header.page_size).expect("WAL header page size fits usize");
        let big_endian_checksum = header.big_endian_checksum();
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;

        // Validate header checksum.
        let header_checksum = read_wal_header_checksum(&header_buf)?;
        let expected_checksum =
            crate::checksum::wal_header_checksum(&header_buf, big_endian_checksum)?;
        if header_checksum != expected_checksum {
            error!("WAL header checksum mismatch — file may be corrupt");
            return Err(FrankenError::WalCorrupt {
                detail: "WAL header checksum mismatch".to_owned(),
            });
        }

        // Scan frames to determine valid count and running checksum.
        let file_size = file.file_size(cx)?;
        let data_bytes =
            file_size.saturating_sub(u64::try_from(WAL_HEADER_SIZE).expect("header size fits u64"));
        let max_frames = usize::try_from(data_bytes).unwrap_or(usize::MAX) / frame_size;

        let mut running_checksum = header_checksum;
        let mut valid_frames = 0_usize;
        let mut frame_buf = vec![0u8; frame_size];

        for frame_index in 0..max_frames {
            // Compute in u64 to prevent usize overflow on 32-bit targets.
            #[allow(clippy::cast_possible_truncation)]
            let file_offset = WAL_HEADER_SIZE as u64 + frame_index as u64 * frame_size as u64;
            let bytes_read = file.read(cx, &mut frame_buf, file_offset)?;
            if bytes_read < frame_size {
                break; // truncated frame
            }

            // Verify salt match.
            let frame_header = WalFrameHeader::from_bytes(&frame_buf[..WAL_FRAME_HEADER_SIZE])?;
            if frame_header.salts != header.salts {
                error!(frame_index, "WAL frame salt mismatch — chain terminated");
                break; // salt mismatch terminates the chain
            }

            // Verify checksum chain.
            let expected = compute_wal_frame_checksum(
                &frame_buf,
                page_size,
                running_checksum,
                big_endian_checksum,
            )?;
            if frame_header.checksum != expected {
                error!(
                    frame_index,
                    "WAL frame checksum mismatch — chain terminated"
                );
                break; // checksum mismatch terminates the chain
            }

            running_checksum = expected;
            valid_frames += 1;
        }

        debug!(
            page_size,
            big_endian_checksum,
            checkpoint_seq = header.checkpoint_seq,
            valid_frames,
            "WAL file opened"
        );

        Ok(Self {
            file,
            page_size,
            big_endian_checksum,
            header,
            running_checksum,
            frame_count: valid_frames,
        })
    }

    /// Append a frame to the WAL.
    ///
    /// `page_number` is the database page this frame writes.
    /// `page_data` must be exactly `page_size` bytes.
    /// `db_size_if_commit` should be the database size in pages for commit
    /// frames, or 0 for non-commit frames.
    pub fn append_frame(
        &mut self,
        cx: &Cx,
        page_number: u32,
        page_data: &[u8],
        db_size_if_commit: u32,
    ) -> Result<()> {
        if page_data.len() != self.page_size {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "page data size mismatch: expected {}, got {}",
                    self.page_size,
                    page_data.len()
                ),
            });
        }

        // Build the frame: header + page data.
        let frame_size = self.frame_size();
        let mut frame = vec![0u8; frame_size];

        // Write page number and db_size into the first 8 bytes.
        frame[..4].copy_from_slice(&page_number.to_be_bytes());
        frame[4..8].copy_from_slice(&db_size_if_commit.to_be_bytes());

        // Write salts.
        write_wal_frame_salts(&mut frame[..WAL_FRAME_HEADER_SIZE], self.header.salts)?;

        // Copy page data.
        frame[WAL_FRAME_HEADER_SIZE..].copy_from_slice(page_data);

        // Compute and write checksum (updates bytes 16..24 of the frame header).
        let new_checksum = write_wal_frame_checksum(
            &mut frame,
            self.page_size,
            self.running_checksum,
            self.big_endian_checksum,
        )?;

        // Write to file.
        let offset = self.frame_offset(self.frame_count);
        self.file.write(cx, &frame, offset)?;

        self.running_checksum = new_checksum;
        self.frame_count += 1;

        let bytes_written = u64::try_from(frame_size).unwrap_or(u64::MAX);
        let span = tracing::span!(
            tracing::Level::DEBUG,
            "wal_write",
            frame_count = self.frame_count,
            bytes_written = bytes_written,
            page_number = page_number,
            is_commit = db_size_if_commit > 0,
        );
        let _guard = span.enter();

        debug!(
            frame_index = self.frame_count - 1,
            page_number,
            is_commit = db_size_if_commit > 0,
            "WAL frame appended"
        );

        crate::metrics::GLOBAL_WAL_METRICS.record_frame_write(bytes_written);

        Ok(())
    }

    /// Read a frame by 0-based index, returning header and page data.
    pub fn read_frame(&mut self, cx: &Cx, frame_index: usize) -> Result<(WalFrameHeader, Vec<u8>)> {
        if frame_index >= self.frame_count {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "frame index {frame_index} out of range (count: {})",
                    self.frame_count
                ),
            });
        }

        let frame_size = self.frame_size();
        let mut frame_buf = vec![0u8; frame_size];
        let offset = self.frame_offset(frame_index);
        let bytes_read = self.file.read(cx, &mut frame_buf, offset)?;
        if bytes_read < frame_size {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "short read at frame {frame_index}: got {bytes_read}, need {frame_size}"
                ),
            });
        }

        let header = WalFrameHeader::from_bytes(&frame_buf[..WAL_FRAME_HEADER_SIZE])?;
        let page_data = frame_buf[WAL_FRAME_HEADER_SIZE..].to_vec();
        Ok((header, page_data))
    }

    /// Read just the frame header at a given 0-based index.
    pub fn read_frame_header(&mut self, cx: &Cx, frame_index: usize) -> Result<WalFrameHeader> {
        if frame_index >= self.frame_count {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "frame index {frame_index} out of range (count: {})",
                    self.frame_count
                ),
            });
        }

        let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE];
        let offset = self.frame_offset(frame_index);
        let bytes_read = self.file.read(cx, &mut header_buf, offset)?;
        if bytes_read < WAL_FRAME_HEADER_SIZE {
            return Err(FrankenError::WalCorrupt {
                detail: format!("short header read at frame {frame_index}: got {bytes_read}"),
            });
        }

        WalFrameHeader::from_bytes(&header_buf)
    }

    /// Find the last commit frame index, or `None` if there are no commits.
    pub fn last_commit_frame(&mut self, cx: &Cx) -> Result<Option<usize>> {
        let mut last = None;
        for i in 0..self.frame_count {
            let header = self.read_frame_header(cx, i)?;
            if header.is_commit() {
                last = Some(i);
            }
        }
        Ok(last)
    }

    /// Sync the WAL file to stable storage.
    pub fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.file.sync(cx, flags)
    }

    /// Reset the WAL for a new checkpoint generation.
    ///
    /// Writes a new header with updated checkpoint sequence and salts,
    /// then truncates the file to header-only. Resets the running checksum
    /// and frame count to zero.
    pub fn reset(&mut self, cx: &Cx, new_checkpoint_seq: u32, new_salts: WalSalts) -> Result<()> {
        let new_header = WalHeader {
            magic: self.header.magic,
            format_version: WAL_FORMAT_VERSION,
            page_size: self.header.page_size,
            checkpoint_seq: new_checkpoint_seq,
            salts: new_salts,
            checksum: SqliteWalChecksum::default(),
        };
        let header_bytes = new_header.to_bytes()?;
        self.file.write(cx, &header_bytes, 0)?;
        self.file.truncate(
            cx,
            u64::try_from(WAL_HEADER_SIZE).expect("header size fits u64"),
        )?;

        self.running_checksum = read_wal_header_checksum(&header_bytes)?;
        self.header = WalHeader::from_bytes(&header_bytes)?;
        self.frame_count = 0;

        debug!(
            checkpoint_seq = new_checkpoint_seq,
            salt1 = new_salts.salt1,
            salt2 = new_salts.salt2,
            "WAL reset"
        );

        crate::metrics::GLOBAL_WAL_METRICS.record_wal_reset();

        Ok(())
    }

    /// Consume this `WalFile` and close the underlying VFS file handle.
    pub fn close(mut self, cx: &Cx) -> Result<()> {
        self.file.close(cx)
    }

    /// Return a reference to the underlying VFS file handle.
    #[must_use]
    pub fn file(&self) -> &F {
        &self.file
    }

    /// Return a mutable reference to the underlying VFS file handle.
    pub fn file_mut(&mut self) -> &mut F {
        &mut self.file
    }
}

#[cfg(test)]
mod tests {
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::MemoryVfs;
    use fsqlite_vfs::traits::Vfs;

    use super::*;

    const PAGE_SIZE: u32 = 4096;

    fn test_cx() -> Cx {
        Cx::default()
    }

    fn test_salts() -> WalSalts {
        WalSalts {
            salt1: 0xDEAD_BEEF,
            salt2: 0xCAFE_BABE,
        }
    }

    fn sample_page(seed: u8) -> Vec<u8> {
        let page_size = usize::try_from(PAGE_SIZE).expect("page size fits usize");
        let mut page = vec![0u8; page_size];
        for (i, byte) in page.iter_mut().enumerate() {
            let reduced = u8::try_from(i % 251).expect("modulo fits u8");
            *byte = reduced ^ seed;
        }
        page
    }

    fn open_wal_file(vfs: &MemoryVfs, cx: &Cx) -> <MemoryVfs as Vfs>::File {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (file, _) = vfs
            .open(cx, Some(std::path::Path::new("test.db-wal")), flags)
            .expect("open WAL file");
        file
    }

    #[test]
    fn test_create_and_open_empty_wal() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        assert_eq!(wal.frame_count(), 0);
        assert_eq!(wal.page_size(), usize::try_from(PAGE_SIZE).unwrap());
        assert!(!wal.big_endian_checksum());
        assert_eq!(wal.header().checkpoint_seq, 0);
        assert_eq!(wal.header().salts, test_salts());

        wal.close(&cx).expect("close WAL");

        // Reopen and verify.
        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("open WAL");
        assert_eq!(wal2.frame_count(), 0);
        assert_eq!(wal2.header().salts, test_salts());

        wal2.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_append_and_read_single_frame() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 1, test_salts()).expect("create WAL");

        let page = sample_page(0x42);
        wal.append_frame(&cx, 1, &page, 0).expect("append frame");
        assert_eq!(wal.frame_count(), 1);

        let (header, data) = wal.read_frame(&cx, 0).expect("read frame");
        assert_eq!(header.page_number, 1);
        assert_eq!(header.db_size, 0);
        assert_eq!(header.salts, test_salts());
        assert_eq!(data, page);

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_append_commit_frame() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        let page = sample_page(0x10);
        wal.append_frame(&cx, 5, &page, 10)
            .expect("append commit frame");

        let header = wal.read_frame_header(&cx, 0).expect("read header");
        assert!(header.is_commit());
        assert_eq!(header.db_size, 10);
        assert_eq!(header.page_number, 5);

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_multi_frame_checksum_chain() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 3, test_salts()).expect("create WAL");

        // Append 5 frames, last is commit.
        for i in 0..5u32 {
            let page = sample_page(u8::try_from(i).expect("fits"));
            let db_size = if i == 4 { 5 } else { 0 };
            wal.append_frame(&cx, i + 1, &page, db_size)
                .expect("append frame");
        }
        assert_eq!(wal.frame_count(), 5);

        wal.close(&cx).expect("close WAL");

        // Reopen and verify all frames are valid (checksum chain intact).
        let file2 = open_wal_file(&vfs, &cx);
        let mut wal2 = WalFile::open(&cx, file2).expect("open WAL");
        assert_eq!(wal2.frame_count(), 5);

        // Verify each frame's content.
        for i in 0..5u32 {
            let (header, data) = wal2
                .read_frame(&cx, usize::try_from(i).unwrap())
                .expect("read frame");
            assert_eq!(header.page_number, i + 1);
            let expected = sample_page(u8::try_from(i).expect("fits"));
            assert_eq!(data, expected);
        }

        // Last frame should be commit.
        let last_header = wal2.read_frame_header(&cx, 4).expect("read header");
        assert!(last_header.is_commit());
        assert_eq!(last_header.db_size, 5);

        wal2.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_last_commit_frame() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        // No frames yet.
        assert_eq!(wal.last_commit_frame(&cx).expect("query"), None);

        // Append non-commit frame.
        wal.append_frame(&cx, 1, &sample_page(1), 0)
            .expect("append");
        assert_eq!(wal.last_commit_frame(&cx).expect("query"), None);

        // Append commit frame.
        wal.append_frame(&cx, 2, &sample_page(2), 3)
            .expect("append");
        assert_eq!(wal.last_commit_frame(&cx).expect("query"), Some(1));

        // Append more non-commit, then another commit.
        wal.append_frame(&cx, 3, &sample_page(3), 0)
            .expect("append");
        wal.append_frame(&cx, 4, &sample_page(4), 5)
            .expect("append");
        assert_eq!(wal.last_commit_frame(&cx).expect("query"), Some(3));

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_reset_clears_frames() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        // Append some frames.
        for i in 0..3u8 {
            wal.append_frame(&cx, u32::from(i) + 1, &sample_page(i), 0)
                .expect("append");
        }
        assert_eq!(wal.frame_count(), 3);

        // Reset with new salts.
        let new_salts = WalSalts {
            salt1: 0x1111_2222,
            salt2: 0x3333_4444,
        };
        wal.reset(&cx, 1, new_salts).expect("reset");
        assert_eq!(wal.frame_count(), 0);
        assert_eq!(wal.header().checkpoint_seq, 1);
        assert_eq!(wal.header().salts, new_salts);

        // Can append new frames after reset.
        wal.append_frame(&cx, 10, &sample_page(0xAA), 1)
            .expect("append after reset");
        assert_eq!(wal.frame_count(), 1);

        wal.close(&cx).expect("close WAL");

        // Reopen and verify reset took effect.
        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("open WAL");
        assert_eq!(wal2.frame_count(), 1);
        assert_eq!(wal2.header().checkpoint_seq, 1);
        assert_eq!(wal2.header().salts, new_salts);

        wal2.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_page_size_mismatch_rejected() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        // Wrong-size page data should be rejected.
        let short_page = vec![0u8; 100];
        let result = wal.append_frame(&cx, 1, &short_page, 0);
        assert!(result.is_err());

        let long_page = vec![0u8; 8192];
        let result = wal.append_frame(&cx, 1, &long_page, 0);
        assert!(result.is_err());

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_frame_index_out_of_range() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        // Reading from empty WAL should fail.
        assert!(wal.read_frame(&cx, 0).is_err());
        assert!(wal.read_frame_header(&cx, 0).is_err());

        // Append one frame, then reading index 1 should fail.
        wal.append_frame(&cx, 1, &sample_page(0), 0)
            .expect("append");
        assert!(wal.read_frame(&cx, 0).is_ok());
        assert!(wal.read_frame(&cx, 1).is_err());

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_reopen_preserves_checksum_chain() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        // Write 3 frames.
        for i in 0..3u8 {
            wal.append_frame(&cx, u32::from(i) + 1, &sample_page(i), 0)
                .expect("append");
        }
        let checksum_after_3 = wal.running_checksum();
        wal.close(&cx).expect("close WAL");

        // Reopen and append more frames (checksum chain must continue).
        let file2 = open_wal_file(&vfs, &cx);
        let mut wal2 = WalFile::open(&cx, file2).expect("open WAL");
        assert_eq!(wal2.frame_count(), 3);
        assert_eq!(wal2.running_checksum(), checksum_after_3);

        wal2.append_frame(&cx, 4, &sample_page(3), 0)
            .expect("append");
        wal2.append_frame(&cx, 5, &sample_page(4), 5)
            .expect("append commit");
        assert_eq!(wal2.frame_count(), 5);
        wal2.close(&cx).expect("close WAL");

        // Final reopen: all 5 frames valid.
        let file3 = open_wal_file(&vfs, &cx);
        let wal3 = WalFile::open(&cx, file3).expect("open WAL");
        assert_eq!(wal3.frame_count(), 5);
        wal3.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_sync_does_not_panic() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        wal.append_frame(&cx, 1, &sample_page(0), 1)
            .expect("append");
        wal.sync(&cx, SyncFlags::NORMAL).expect("sync");
        wal.sync(&cx, SyncFlags::FULL).expect("full sync");

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_file_accessors() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        // file() and file_mut() should work without panic.
        let _size = wal.file().file_size(&cx).expect("file_size");
        let _size = wal.file_mut().file_size(&cx).expect("file_size via mut");

        wal.close(&cx).expect("close WAL");
    }
}
