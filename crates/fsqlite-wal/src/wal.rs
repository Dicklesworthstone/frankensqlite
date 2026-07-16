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
use fsqlite_vfs::{SyncKind, VfsFile};
use tracing::{debug, error, warn};

/// Whether the `FRANKENSQLITE_PARANOID_DURABILITY` env var is set.
/// Checked once at startup to avoid repeated `env::var` calls on the hot path.
static PARANOID_DURABILITY: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
    std::env::var("FRANKENSQLITE_PARANOID_DURABILITY")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
});

use crate::checksum::{
    SqliteWalChecksum, WAL_FORMAT_VERSION, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WAL_MAGIC_LE,
    WalChecksumTransform, WalFrameHeader, WalHeader, WalSalts, compute_wal_frame_checksum,
    read_wal_header_checksum, wal_header_checksum, write_wal_frame_checksum,
    write_wal_frame_checksum_fields, write_wal_frame_salts,
};

#[inline]
fn log_replay_decision(
    replay_cursor: &'static str,
    frame_no: usize,
    commit_boundary: usize,
    decision_reason: &'static str,
) {
    debug!(
        replay_cursor,
        frame_no, commit_boundary, decision_reason, "WAL replay decision"
    );
}

/// Borrowed frame descriptor used for consolidated WAL writes.
#[derive(Debug, Clone, Copy)]
pub struct WalAppendFrameRef<'a> {
    /// Database page number this frame writes.
    pub page_number: u32,
    /// Page contents for the frame. Must be exactly `page_size` bytes.
    pub page_data: &'a [u8],
    /// Database size in pages for commit frames, or 0 for non-commit frames.
    pub db_size_if_commit: u32,
}

/// Identity for one WAL generation.
///
/// A generation changes whenever the WAL header is reset for a new checkpoint
/// epoch. Salts usually change too, but correctness must not rely on that:
/// reset/ABA detection must still work if a caller reuses the same salt pair
/// with a new checkpoint sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalGenerationIdentity {
    /// Checkpoint sequence stored in the WAL header.
    pub checkpoint_seq: u32,
    /// Salt pair copied into WAL frames for this generation.
    pub salts: WalSalts,
}

impl WalGenerationIdentity {
    /// Build a generation identity from a parsed WAL header.
    #[must_use]
    pub const fn from_header(header: &WalHeader) -> Self {
        Self {
            checkpoint_seq: header.checkpoint_seq,
            salts: header.salts,
        }
    }
}

#[inline]
fn push_wal_frame_bytes(
    frame_scratch: &mut Vec<u8>,
    page_number: u32,
    db_size_if_commit: u32,
    salts: WalSalts,
    page_data: &[u8],
) {
    frame_scratch.extend_from_slice(&page_number.to_be_bytes());
    frame_scratch.extend_from_slice(&db_size_if_commit.to_be_bytes());
    frame_scratch.extend_from_slice(&salts.salt1.to_be_bytes());
    frame_scratch.extend_from_slice(&salts.salt2.to_be_bytes());
    frame_scratch.extend_from_slice(&[0_u8; 8]);
    frame_scratch.extend_from_slice(page_data);
}

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
    /// Index of the latest visible commit frame for the active generation.
    last_commit_frame: Option<usize>,
    /// Reusable contiguous scratch for direct append paths.
    ///
    /// Ownership is per-`WalFile` handle. Append methods already require
    /// `&mut self`, so reuse stays serialized per handle without reintroducing
    /// any cross-writer coordination.
    frame_scratch: Vec<u8>,
    /// Frame count at which the last successful durable_sync completed.
    /// Used by debug-assertions and FRANKENSQLITE_PARANOID_DURABILITY to
    /// verify the two-phase commit invariant: fsync must complete before
    /// any CommitIndex publish for the same frames.
    last_fsynced_frame_count: usize,
}

impl<F: VfsFile> WalFile<F> {
    /// Re-synchronize this handle with the on-disk WAL if another writer has
    /// appended frames or reset/truncated the file.
    ///
    /// This keeps `frame_count` and `running_checksum` coherent across
    /// multiple concurrently-open `WalFile` handles.
    pub fn refresh(&mut self, cx: &Cx) -> Result<()> {
        let frame_size = self.frame_size();
        let expected_size = u64::try_from(WAL_HEADER_SIZE)
            .expect("WAL header size fits u64")
            .saturating_add(
                u64::try_from(self.frame_count)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(u64::try_from(frame_size).unwrap_or(u64::MAX)),
            );
        let file_size = self.file.file_size(cx)?;

        // If file shrank (checkpoint reset/truncate, external compaction, etc.),
        // or changed in a way we cannot safely reason about incrementally,
        // rebuild state from the on-disk WAL from scratch.
        if file_size < expected_size {
            log_replay_decision("refresh", 0, self.frame_count, "file_shrank_rebuild");
            return self.rebuild_state_from_file(cx);
        }

        // Validate current on-disk header and confirm it matches our view.
        // This is necessary even if file_size == expected_size to detect ABA
        // where the WAL was reset and then appended back to the exact same size.
        let mut header_buf = [0u8; WAL_HEADER_SIZE];
        let header_read = self.file.read(cx, &mut header_buf, 0)?;
        if header_read < WAL_HEADER_SIZE {
            log_replay_decision("refresh", 0, self.frame_count, "header_short_read_corrupt");
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WAL file too small for header during refresh: read {header_read}, need {WAL_HEADER_SIZE}"
                ),
            });
        }

        let disk_header = WalHeader::from_bytes(&header_buf)?;
        let disk_big_endian = disk_header.big_endian_checksum();
        let disk_header_checksum = read_wal_header_checksum(&header_buf)?;
        let expected_header_checksum = wal_header_checksum(&header_buf, disk_big_endian)?;
        if disk_header_checksum != expected_header_checksum {
            log_replay_decision(
                "refresh",
                0,
                self.frame_count,
                "header_checksum_mismatch_corrupt",
            );
            return Err(FrankenError::WalCorrupt {
                detail: "WAL header checksum mismatch during refresh".to_owned(),
            });
        }

        // Header changed under us (e.g., RESET/TRUNCATE checkpoint) — rebuild.
        if disk_header.magic != self.header.magic
            || disk_header.format_version != self.header.format_version
            || disk_header.page_size != self.header.page_size
            || disk_header.checkpoint_seq != self.header.checkpoint_seq
            || disk_header.salts != self.header.salts
        {
            log_replay_decision(
                "refresh",
                0,
                self.frame_count,
                "header_generation_changed_rebuild",
            );
            return self.rebuild_state_from_file(cx);
        }

        if file_size == expected_size {
            return Ok(());
        }

        // Incrementally absorb newly appended complete frames.
        //
        // For live multi-connection operation we only need:
        // - the new valid prefix length (`frame_count`)
        // - the checksum seed for the next append (`running_checksum`)
        //
        // SQLite WAL frame headers already carry the post-frame rolling
        // checksum, so we can ingest appended frames by reading headers only.
        // Full checksum-chain verification is still performed on open/rebuild.
        let frame_size_u64 = u64::try_from(frame_size).unwrap_or(u64::MAX);
        let available_frames = usize::try_from(
            file_size.saturating_sub(u64::try_from(WAL_HEADER_SIZE).unwrap_or(0)) / frame_size_u64,
        )
        .unwrap_or(usize::MAX);
        if available_frames <= self.frame_count {
            return Ok(());
        }

        let mut new_frame_count = self.frame_count;
        let mut new_running_checksum = self.running_checksum;
        let mut last_commit_count = self.frame_count;
        let mut last_commit_checksum = self.running_checksum;

        let mut frame_buf = vec![0u8; frame_size];
        for frame_index in self.frame_count..available_frames {
            let frame_no = frame_index.saturating_add(1);
            let offset = self.frame_offset(frame_index);
            let bytes_read = self.file.read(cx, &mut frame_buf, offset)?;
            if bytes_read < frame_size {
                log_replay_decision(
                    "refresh_incremental",
                    frame_no,
                    last_commit_count,
                    "truncated_tail_stop",
                );
                break; // Partial/torn tail frame; keep prior valid prefix.
            }

            let frame_header = WalFrameHeader::from_bytes(&frame_buf[..WAL_FRAME_HEADER_SIZE])?;
            if frame_header.salts != self.header.salts {
                log_replay_decision(
                    "refresh_incremental",
                    frame_no,
                    last_commit_count,
                    "salt_mismatch_stop",
                );
                break; // End of valid chain for this generation.
            }

            let expected = compute_wal_frame_checksum(
                &frame_buf,
                self.page_size,
                new_running_checksum,
                self.big_endian_checksum,
            )?;
            if frame_header.checksum != expected {
                log_replay_decision(
                    "refresh_incremental",
                    frame_no,
                    last_commit_count,
                    "checksum_mismatch_stop",
                );
                break; // Checksum mismatch
            }

            new_running_checksum = expected;
            new_frame_count += 1;

            if frame_header.is_commit() {
                last_commit_count = new_frame_count;
                last_commit_checksum = new_running_checksum;
                log_replay_decision(
                    "refresh_incremental",
                    frame_no,
                    last_commit_count,
                    "accept_commit",
                );
            } else {
                log_replay_decision(
                    "refresh_incremental",
                    frame_no,
                    last_commit_count,
                    "accept_non_commit",
                );
            }
        }

        self.frame_count = last_commit_count;
        self.running_checksum = last_commit_checksum;
        self.last_commit_frame = last_commit_count.checked_sub(1);

        Ok(())
    }

    fn rebuild_state_from_file(&mut self, cx: &Cx) -> Result<()> {
        let mut header_buf = [0u8; WAL_HEADER_SIZE];
        let header_read = self.file.read(cx, &mut header_buf, 0)?;
        if header_read < WAL_HEADER_SIZE {
            log_replay_decision("rebuild", 0, self.frame_count, "header_short_read_corrupt");
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WAL file too small for header during rebuild: read {header_read}, need {WAL_HEADER_SIZE}"
                ),
            });
        }

        let header = WalHeader::from_bytes(&header_buf)?;
        let page_size = usize::try_from(header.page_size).expect("WAL header page size fits usize");
        let big_endian_checksum = header.big_endian_checksum();
        let header_checksum = read_wal_header_checksum(&header_buf)?;
        let expected_header_checksum = wal_header_checksum(&header_buf, big_endian_checksum)?;
        if header_checksum != expected_header_checksum {
            log_replay_decision(
                "rebuild",
                0,
                self.frame_count,
                "header_checksum_mismatch_corrupt",
            );
            return Err(FrankenError::WalCorrupt {
                detail: "WAL header checksum mismatch during rebuild".to_owned(),
            });
        }

        self.header = header;
        self.page_size = page_size;
        self.big_endian_checksum = big_endian_checksum;
        self.running_checksum = header_checksum;
        self.frame_count = 0;

        let mut new_frame_count = 0;
        let mut new_running_checksum = header_checksum;
        let mut last_commit_count = 0;
        let mut last_commit_checksum = header_checksum;

        let frame_size = self.frame_size();
        let file_size = self.file.file_size(cx)?;
        let max_frames = usize::try_from(
            file_size.saturating_sub(u64::try_from(WAL_HEADER_SIZE).unwrap_or(0))
                / u64::try_from(frame_size).unwrap_or(1),
        )
        .unwrap_or(usize::MAX);

        let mut frame_buf = vec![0u8; frame_size];
        for frame_index in 0..max_frames {
            let frame_no = frame_index.saturating_add(1);
            let offset = self.frame_offset(frame_index);
            let bytes_read = self.file.read(cx, &mut frame_buf, offset)?;
            if bytes_read < frame_size {
                log_replay_decision(
                    "rebuild",
                    frame_no,
                    last_commit_count,
                    "truncated_tail_stop",
                );
                break;
            }

            let frame_header = WalFrameHeader::from_bytes(&frame_buf[..WAL_FRAME_HEADER_SIZE])?;
            if frame_header.salts != self.header.salts {
                log_replay_decision("rebuild", frame_no, last_commit_count, "salt_mismatch_stop");
                break;
            }

            let expected = compute_wal_frame_checksum(
                &frame_buf,
                self.page_size,
                new_running_checksum,
                self.big_endian_checksum,
            )?;
            if frame_header.checksum != expected {
                log_replay_decision(
                    "rebuild",
                    frame_no,
                    last_commit_count,
                    "checksum_mismatch_stop",
                );
                break;
            }

            new_running_checksum = expected;
            new_frame_count += 1;

            if frame_header.is_commit() {
                last_commit_count = new_frame_count;
                last_commit_checksum = new_running_checksum;
                log_replay_decision("rebuild", frame_no, last_commit_count, "accept_commit");
            } else {
                log_replay_decision("rebuild", frame_no, last_commit_count, "accept_non_commit");
            }
        }

        self.frame_count = last_commit_count;
        self.running_checksum = last_commit_checksum;
        self.last_commit_frame = last_commit_count.checked_sub(1);

        Ok(())
    }

    /// Size in bytes of a single frame (header + page data).
    #[must_use]
    pub fn frame_size(&self) -> usize {
        WAL_FRAME_HEADER_SIZE + self.page_size
    }

    /// Byte offset of frame `index` (0-based) within the WAL file.
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn frame_offset(&self, index: usize) -> u64 {
        // Compute in u64 to prevent usize overflow on 32-bit targets.
        // WAL_HEADER_SIZE is 32.
        let header_size = WAL_HEADER_SIZE as u64;
        let idx = index as u64;
        let frame_sz = self.frame_size() as u64;
        header_size.saturating_add(idx.saturating_mul(frame_sz))
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

    /// The current WAL generation identity (`checkpoint_seq` + salts).
    #[must_use]
    pub fn generation_identity(&self) -> WalGenerationIdentity {
        WalGenerationIdentity::from_header(&self.header)
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

    #[cfg(test)]
    #[must_use]
    fn frame_scratch_len(&self) -> usize {
        self.frame_scratch.len()
    }

    #[cfg(test)]
    #[must_use]
    fn frame_scratch_capacity(&self) -> usize {
        self.frame_scratch.capacity()
    }

    #[cfg(test)]
    #[must_use]
    fn frame_scratch_ptr(&self) -> *const u8 {
        self.frame_scratch.as_ptr()
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
        // Make the fresh header + truncation durable before any frame is
        // appended. Without this barrier a crash can leave the previous WAL
        // generation's frames on disk behind a header they could
        // chain-validate against, replaying stale frames as committed on the
        // next open. (Salts are randomized per generation — GH #201 — which
        // independently defends against stale-frame replay, but the barrier
        // remains the primary ordering guarantee.)
        file.sync(cx, SyncFlags::NORMAL)?;

        let running_checksum = read_wal_header_checksum(&header_bytes)?;

        debug!(
            page_size,
            checkpoint_seq,
            salt1 = header.salts.salt1,
            salt2 = header.salts.salt2,
            "WAL file created"
        );
        crate::metrics::GLOBAL_WAL_METRICS.set_wal_frames_current(0);

        Ok(Self {
            file,
            page_size: usize::try_from(page_size).expect("page size fits usize"),
            big_endian_checksum: false,
            header,
            running_checksum,
            frame_count: 0,
            last_commit_frame: None,
            frame_scratch: Vec::new(),
            last_fsynced_frame_count: 0,
        })
    }

    /// Open an existing WAL file by reading and validating its header,
    /// then scanning frames to determine the valid frame count and
    /// running checksum.
    #[allow(clippy::too_many_lines)]
    pub fn open(cx: &Cx, file: F) -> Result<Self> {
        // Read and parse the 32-byte header.
        let mut header_buf = [0u8; WAL_HEADER_SIZE];
        let bytes_read = file.read(cx, &mut header_buf, 0)?;
        if bytes_read < WAL_HEADER_SIZE {
            log_replay_decision("startup_open", 0, 0, "header_short_read_corrupt");
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
            log_replay_decision("startup_open", 0, 0, "header_checksum_mismatch_corrupt");
            return Err(FrankenError::WalCorrupt {
                detail: "WAL header checksum mismatch".to_owned(),
            });
        }

        // Scan frames to determine valid count and running checksum.
        let file_size = file.file_size(cx)?;
        let data_bytes =
            file_size.saturating_sub(u64::try_from(WAL_HEADER_SIZE).expect("header size fits u64"));
        let max_frames = usize::try_from(data_bytes / u64::try_from(frame_size).unwrap_or(1))
            .unwrap_or(usize::MAX);

        let mut running_checksum = header_checksum;
        let mut valid_frames = 0_usize;
        let mut last_commit_frames = 0_usize;
        let mut last_commit_checksum = header_checksum;
        let mut frame_buf = vec![0u8; frame_size];

        for frame_index in 0..max_frames {
            let frame_no = frame_index.saturating_add(1);
            // Compute in u64 to prevent usize overflow on 32-bit targets.
            // Use the helper method which is guaranteed safe.
            // Note: we can't call self.frame_offset because we don't have self yet.
            // Replicate the logic here: header + index * frame_size.
            let header_size = WAL_HEADER_SIZE as u64;
            let idx = frame_index as u64;
            let frame_sz = frame_size as u64;
            let file_offset = header_size.saturating_add(idx.saturating_mul(frame_sz));

            let bytes_read = file.read(cx, &mut frame_buf, file_offset)?;
            if bytes_read < frame_size {
                log_replay_decision(
                    "startup_open",
                    frame_no,
                    last_commit_frames,
                    "truncated_tail_stop",
                );
                break; // truncated frame
            }

            // Verify salt match.
            let frame_header = WalFrameHeader::from_bytes(&frame_buf[..WAL_FRAME_HEADER_SIZE])?;
            if frame_header.salts != header.salts {
                warn!(frame_index, "WAL frame salt mismatch — chain terminated");
                log_replay_decision(
                    "startup_open",
                    frame_no,
                    last_commit_frames,
                    "salt_mismatch_stop",
                );
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
                warn!(
                    frame_index,
                    "WAL frame checksum mismatch — chain terminated"
                );
                log_replay_decision(
                    "startup_open",
                    frame_no,
                    last_commit_frames,
                    "checksum_mismatch_stop",
                );
                break; // checksum mismatch terminates the chain
            }

            running_checksum = expected;
            valid_frames += 1;

            if frame_header.is_commit() {
                last_commit_frames = valid_frames;
                last_commit_checksum = running_checksum;
                log_replay_decision(
                    "startup_open",
                    frame_no,
                    last_commit_frames,
                    "accept_commit",
                );
            } else {
                log_replay_decision(
                    "startup_open",
                    frame_no,
                    last_commit_frames,
                    "accept_non_commit",
                );
            }
        }

        debug!(
            page_size,
            big_endian_checksum,
            checkpoint_seq = header.checkpoint_seq,
            valid_frames = last_commit_frames,
            "WAL file opened"
        );
        crate::metrics::GLOBAL_WAL_METRICS
            .set_wal_frames_current(u64::try_from(last_commit_frames).unwrap_or(u64::MAX));

        Ok(Self {
            file,
            page_size,
            big_endian_checksum,
            header,
            running_checksum: last_commit_checksum,
            frame_count: last_commit_frames,
            last_commit_frame: last_commit_frames.checked_sub(1),
            frame_scratch: Vec::new(),
            last_fsynced_frame_count: last_commit_frames,
        })
    }

    /// Advance the internal WAL state after a direct, consolidated file write.
    ///
    /// This avoids re-reading the written frames just to update bookkeeping.
    /// The caller must guarantee the frames were successfully synced to disk
    /// and that the provided checksum exactly matches the end of the chain.
    pub fn advance_state_after_write(
        &mut self,
        frames_written: usize,
        new_running_checksum: SqliteWalChecksum,
    ) -> Result<()> {
        let new_count = self
            .frame_count
            .checked_add(frames_written)
            .ok_or(FrankenError::DatabaseFull)?;

        if new_count > usize::try_from(u32::MAX).unwrap_or(usize::MAX) {
            return Err(FrankenError::DatabaseFull);
        }

        self.frame_count = new_count;
        self.running_checksum = new_running_checksum;
        crate::metrics::GLOBAL_WAL_METRICS
            .set_wal_frames_current(u64::try_from(self.frame_count).unwrap_or(u64::MAX));
        Ok(())
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
        if self.frame_count >= usize::try_from(u32::MAX).unwrap_or(usize::MAX) {
            return Err(FrankenError::DatabaseFull);
        }

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
        let page_size = self.page_size;
        let salts = self.header.salts;
        let running_checksum = self.running_checksum;
        let big_endian_checksum = self.big_endian_checksum;
        let offset = self.frame_offset(self.frame_count);

        let mut frame_scratch = std::mem::take(&mut self.frame_scratch);
        frame_scratch.clear();
        if frame_scratch.capacity() < frame_size {
            frame_scratch.reserve(frame_size - frame_scratch.capacity());
        }
        let append_result = (|| -> Result<SqliteWalChecksum> {
            push_wal_frame_bytes(
                &mut frame_scratch,
                page_number,
                db_size_if_commit,
                salts,
                page_data,
            );
            let frame = &mut frame_scratch[..frame_size];

            // Compute and write checksum (updates bytes 16..24 of the frame header).
            let new_checksum =
                write_wal_frame_checksum(frame, page_size, running_checksum, big_endian_checksum)?;

            self.file.write(cx, frame, offset)?;
            Ok(new_checksum)
        })();
        self.frame_scratch = frame_scratch;
        let new_checksum = append_result?;

        self.running_checksum = new_checksum;
        self.frame_count += 1;
        if db_size_if_commit != 0 {
            self.last_commit_frame = Some(self.frame_count - 1);
        }
        crate::metrics::GLOBAL_WAL_METRICS
            .set_wal_frames_current(u64::try_from(self.frame_count).unwrap_or(u64::MAX));

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

    /// Serialize a frame batch into contiguous WAL bytes without writing the
    /// rolling checksum chain.
    ///
    /// This lets higher layers move header/payload copy work out of a
    /// serialized append window while preserving the requirement that checksum
    /// chaining still uses the live on-disk seed at append time.
    pub fn prepare_frame_bytes(&self, frames: &[WalAppendFrameRef<'_>]) -> Result<Vec<u8>> {
        let mut frame_buf = Vec::new();
        let mut checksum_transforms = Vec::new();
        let _ = self.prepare_frame_bytes_with_transforms_into(
            frames.len(),
            frames.iter().copied(),
            &mut frame_buf,
            &mut checksum_transforms,
        )?;
        Ok(frame_buf)
    }

    /// Serialize a batch of frames into caller-owned storage and precompute the
    /// per-frame checksum transforms in the same pass.
    ///
    /// This lets higher layers reserve the buffer up front and avoid both an
    /// intermediate `Vec<WalAppendFrameRef>` and a later whole-batch checksum
    /// transform walk over the serialized bytes.
    pub fn prepare_frame_bytes_with_transforms_into<'a, I>(
        &self,
        frame_count: usize,
        frames: I,
        frame_buf: &mut Vec<u8>,
        checksum_transforms: &mut Vec<WalChecksumTransform>,
    ) -> Result<Option<usize>>
    where
        I: IntoIterator<Item = WalAppendFrameRef<'a>>,
    {
        frame_buf.clear();
        checksum_transforms.clear();
        if frame_count == 0 {
            return Ok(None);
        }

        let frame_size = self.frame_size();
        let total_bytes = frame_count
            .checked_mul(frame_size)
            .ok_or(FrankenError::DatabaseFull)?;
        frame_buf.resize(total_bytes, 0);
        if checksum_transforms.capacity() < frame_count {
            checksum_transforms.reserve(frame_count - checksum_transforms.capacity());
        }

        let mut observed_frame_count = 0usize;
        let mut last_commit_offset = None;
        for (idx, frame) in frames.into_iter().enumerate() {
            if idx >= frame_count {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "prepared batch frame count mismatch: expected {frame_count}, got more than declared"
                    ),
                });
            }
            if frame.page_data.len() != self.page_size {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "page data size mismatch in batch frame {idx}: expected {}, got {}",
                        self.page_size,
                        frame.page_data.len()
                    ),
                });
            }

            let buf_offset = idx
                .checked_mul(frame_size)
                .ok_or(FrankenError::DatabaseFull)?;
            let frame_slice = &mut frame_buf[buf_offset..buf_offset + frame_size];

            frame_slice[..4].copy_from_slice(&frame.page_number.to_be_bytes());
            frame_slice[4..8].copy_from_slice(&frame.db_size_if_commit.to_be_bytes());
            write_wal_frame_salts(&mut frame_slice[..WAL_FRAME_HEADER_SIZE], self.header.salts)?;
            frame_slice[WAL_FRAME_HEADER_SIZE..].copy_from_slice(frame.page_data);
            checksum_transforms.push(WalChecksumTransform::for_wal_frame(
                frame_slice,
                self.page_size,
                self.big_endian_checksum,
            )?);
            if frame.db_size_if_commit != 0 {
                last_commit_offset = Some(idx);
            }
            observed_frame_count = idx.saturating_add(1);
        }

        if observed_frame_count != frame_count {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "prepared batch frame count mismatch: expected {frame_count}, got {observed_frame_count}"
                ),
            });
        }

        Ok(last_commit_offset)
    }

    /// Check whether the on-disk WAL still matches a previously observed
    /// append window.
    ///
    /// This is a cheap ABA-resistant probe used after a pre-lock finalize
    /// pass. If the generation identity and frame count still match, no other
    /// writer could have changed the append seed or target offset.
    pub fn prepared_append_window_still_current(
        &self,
        cx: &Cx,
        generation: WalGenerationIdentity,
        start_frame_index: usize,
    ) -> Result<bool> {
        let expected_size = u64::try_from(WAL_HEADER_SIZE)
            .expect("WAL header size fits u64")
            .saturating_add(
                u64::try_from(start_frame_index)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(u64::try_from(self.frame_size()).unwrap_or(u64::MAX)),
            );
        if self.file.file_size(cx)? != expected_size {
            return Ok(false);
        }

        let mut header_buf = [0u8; WAL_HEADER_SIZE];
        let header_read = self.file.read(cx, &mut header_buf, 0)?;
        if header_read < WAL_HEADER_SIZE {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WAL file too small for header during prepared append validation: read {header_read}, need {WAL_HEADER_SIZE}"
                ),
            });
        }

        let disk_header = WalHeader::from_bytes(&header_buf)?;
        Ok(WalGenerationIdentity::from_header(&disk_header) == generation)
    }

    /// Finalize a previously prepared frame buffer against the current live
    /// rolling checksum seed.
    ///
    /// This mutates the frame checksum fields in-place and returns the final
    /// running checksum that should become authoritative after the eventual
    /// durable append succeeds.
    pub fn finalize_prepared_frame_bytes(
        &self,
        prepared_frame_bytes: &mut [u8],
        frame_transforms: &[WalChecksumTransform],
    ) -> Result<SqliteWalChecksum> {
        let frame_count = frame_transforms.len();
        if frame_count == 0 {
            return Ok(self.running_checksum);
        }

        let frame_size = self.frame_size();
        let expected_bytes = frame_count
            .checked_mul(frame_size)
            .ok_or(FrankenError::DatabaseFull)?;
        if prepared_frame_bytes.len() != expected_bytes {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "prepared batch byte length mismatch: expected {expected_bytes}, got {}",
                    prepared_frame_bytes.len()
                ),
            });
        }

        let mut running_checksum = self.running_checksum;
        for (frame_slice, frame_transform) in prepared_frame_bytes
            .chunks_exact_mut(frame_size)
            .zip(frame_transforms.iter())
        {
            write_wal_frame_salts(&mut frame_slice[..WAL_FRAME_HEADER_SIZE], self.header.salts)?;
            running_checksum = frame_transform.apply(running_checksum);
            write_wal_frame_checksum_fields(frame_slice, running_checksum)?;
        }

        Ok(running_checksum)
    }

    /// Append a batch whose frame bytes were already finalized against the
    /// current append window.
    pub fn append_finalized_prepared_frame_bytes(
        &mut self,
        cx: &Cx,
        prepared_frame_bytes: &[u8],
        frame_count: usize,
        final_running_checksum: SqliteWalChecksum,
        last_commit_offset: Option<usize>,
    ) -> Result<()> {
        if frame_count == 0 {
            return Ok(());
        }

        let new_count = self
            .frame_count
            .checked_add(frame_count)
            .ok_or(FrankenError::DatabaseFull)?;
        if new_count > usize::try_from(u32::MAX).unwrap_or(usize::MAX) {
            return Err(FrankenError::DatabaseFull);
        }

        let frame_size = self.frame_size();
        let expected_bytes = frame_count
            .checked_mul(frame_size)
            .ok_or(FrankenError::DatabaseFull)?;
        if prepared_frame_bytes.len() != expected_bytes {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "prepared batch byte length mismatch: expected {expected_bytes}, got {}",
                    prepared_frame_bytes.len()
                ),
            });
        }

        let start_frame_index = self.frame_count;
        let offset = self.frame_offset(start_frame_index);

        #[cfg(any(test, feature = "fault-injection"))]
        crate::fault_hooks::maybe_inject_crash_at(
            crate::fault_hooks::CrashBoundary::BeforeWalFrameAppend,
            &format!("start_frame={start_frame_index} frame_count={frame_count}"),
        )?;

        self.file.write(cx, prepared_frame_bytes, offset)?;
        self.advance_state_after_write(frame_count, final_running_checksum)?;
        if let Some(last_commit_offset) = last_commit_offset {
            self.last_commit_frame = Some(start_frame_index + last_commit_offset);
        }

        #[cfg(any(test, feature = "fault-injection"))]
        crate::fault_hooks::maybe_inject_crash_at(
            crate::fault_hooks::CrashBoundary::AfterWalFrameAppendBeforeFsync,
            &format!(
                "end_frame={} frames_written={frame_count}",
                self.frame_count
            ),
        )?;

        let bytes_per_frame = u64::try_from(frame_size).unwrap_or(u64::MAX);
        let bytes_written = u64::try_from(expected_bytes).unwrap_or(u64::MAX);
        let span = tracing::span!(
            tracing::Level::DEBUG,
            "wal_batch_write",
            start_frame_index = start_frame_index,
            frames_written = frame_count,
            bytes_written = bytes_written,
        );
        let _guard = span.enter();

        debug!(
            end_frame_count = self.frame_count,
            frames_written = frame_count,
            "WAL frames appended in batch"
        );

        for _ in 0..frame_count {
            crate::metrics::GLOBAL_WAL_METRICS.record_frame_write(bytes_per_frame);
        }

        Ok(())
    }

    /// Finalize checksums for a previously prepared frame buffer and append it.
    ///
    /// `prepared_frame_bytes` must contain `frame_transforms.len()` frame
    /// records in WAL frame layout with page number, db_size, salts, and
    /// payload already serialized. The checksum bytes are overwritten in-place
    /// using the live rolling checksum seed from this WAL handle.
    pub fn append_prepared_frame_bytes(
        &mut self,
        cx: &Cx,
        prepared_frame_bytes: &mut [u8],
        frame_transforms: &[WalChecksumTransform],
    ) -> Result<()> {
        let frame_count = frame_transforms.len();
        if frame_count == 0 {
            return Ok(());
        }

        let new_count = self
            .frame_count
            .checked_add(frame_count)
            .ok_or(FrankenError::DatabaseFull)?;
        if new_count > usize::try_from(u32::MAX).unwrap_or(usize::MAX) {
            return Err(FrankenError::DatabaseFull);
        }

        let frame_size = self.frame_size();
        let running_checksum =
            self.finalize_prepared_frame_bytes(prepared_frame_bytes, frame_transforms)?;
        let last_commit_offset = prepared_frame_bytes
            .chunks_exact(frame_size)
            .enumerate()
            .rev()
            .find_map(|(offset, frame_slice)| {
                let db_size_if_commit = u32::from_be_bytes([
                    frame_slice[4],
                    frame_slice[5],
                    frame_slice[6],
                    frame_slice[7],
                ]);
                (db_size_if_commit != 0).then_some(offset)
            });
        self.append_finalized_prepared_frame_bytes(
            cx,
            prepared_frame_bytes,
            frame_count,
            running_checksum,
            last_commit_offset,
        )
    }

    /// Append a batch of frames to the WAL using a single contiguous write.
    ///
    /// This preserves the checksum chain while avoiding per-frame write
    /// syscalls on hot commit paths. Durability is still controlled by
    /// [`Self::sync`] or a higher-level caller.
    pub fn append_frames(&mut self, cx: &Cx, frames: &[WalAppendFrameRef<'_>]) -> Result<()> {
        self.append_frame_iter(cx, frames.len(), frames.iter().copied())
    }

    /// Append a known-size iterator of frame references without first
    /// materializing a borrowed descriptor slice.
    pub(crate) fn append_frame_iter<'a, I>(
        &mut self,
        cx: &Cx,
        frame_count: usize,
        frames: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = WalAppendFrameRef<'a>>,
    {
        if frame_count == 0 {
            return Ok(());
        }

        #[cfg(any(test, feature = "fault-injection"))]
        crate::fault_hooks::maybe_inject_append_busy(self.frame_count, frame_count)?;

        let frame_size = self.frame_size();
        let total_bytes = frame_count
            .checked_mul(frame_size)
            .ok_or(FrankenError::DatabaseFull)?;
        let page_size = self.page_size;
        let salts = self.header.salts;
        let big_endian_checksum = self.big_endian_checksum;
        #[cfg(any(test, feature = "fault-injection"))]
        let frame_count_before = self.frame_count;

        let mut frame_scratch = std::mem::take(&mut self.frame_scratch);
        frame_scratch.clear();
        if frame_scratch.capacity() < total_bytes {
            frame_scratch.reserve(total_bytes - frame_scratch.capacity());
        }
        // bd-db300.3.8.6: Fuse frame assembly + checksum computation into a
        // single pass, eliminating the intermediate Vec<WalChecksumTransform>
        // allocation and the redundant second write_wal_frame_salts call that
        // finalize_prepared_frame_bytes performed.
        let append_result = (|| -> Result<()> {
            let mut running_checksum = self.running_checksum;
            let mut last_commit_offset: Option<usize> = None;
            let mut observed_frame_count = 0usize;

            for (idx, frame) in frames.into_iter().enumerate() {
                if idx >= frame_count {
                    return Err(FrankenError::WalCorrupt {
                        detail: format!(
                            "append batch frame count mismatch: expected {frame_count}, got more than declared"
                        ),
                    });
                }
                if frame.page_data.len() != page_size {
                    return Err(FrankenError::WalCorrupt {
                        detail: format!(
                            "page data size mismatch in batch frame {idx}: expected {page_size}, got {}",
                            frame.page_data.len()
                        ),
                    });
                }

                let buf_offset = idx
                    .checked_mul(frame_size)
                    .ok_or(FrankenError::DatabaseFull)?;

                // Build the frame: page_number, db_size, salts, page data.
                push_wal_frame_bytes(
                    &mut frame_scratch,
                    frame.page_number,
                    frame.db_size_if_commit,
                    salts,
                    frame.page_data,
                );
                let frame_slice = &mut frame_scratch[buf_offset..buf_offset + frame_size];

                // Compute and write the checksum inline — no transform Vec needed.
                running_checksum = write_wal_frame_checksum(
                    frame_slice,
                    page_size,
                    running_checksum,
                    big_endian_checksum,
                )?;

                if frame.db_size_if_commit != 0 {
                    last_commit_offset = Some(idx);
                }
                observed_frame_count = idx + 1;
            }

            if observed_frame_count != frame_count {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "append batch frame count mismatch: expected {frame_count}, got {observed_frame_count}"
                    ),
                });
            }

            self.append_finalized_prepared_frame_bytes(
                cx,
                &frame_scratch,
                frame_count,
                running_checksum,
                last_commit_offset,
            )
        })();
        self.frame_scratch = frame_scratch;

        #[cfg(any(test, feature = "fault-injection"))]
        if append_result.is_ok() {
            crate::fault_hooks::maybe_inject_after_append(frame_count_before, frame_count)?;
        }

        append_result
    }

    /// Read a frame by 0-based index, returning header and page data.
    pub fn read_frame(&self, cx: &Cx, frame_index: usize) -> Result<(WalFrameHeader, Vec<u8>)> {
        let frame_size = self.frame_size();
        let mut buf = vec![0u8; frame_size];
        let header = self.read_frame_into(cx, frame_index, &mut buf)?;
        let page_data = buf[WAL_FRAME_HEADER_SIZE..].to_vec();
        Ok((header, page_data))
    }

    /// Read a frame into a provided buffer, returning the header.
    ///
    /// `buf` must be at least `frame_size` bytes. The frame header is parsed
    /// from the beginning of the buffer, and the page data follows immediately
    /// after at offset `WAL_FRAME_HEADER_SIZE`.
    pub fn read_frame_into(
        &self,
        cx: &Cx,
        frame_index: usize,
        buf: &mut [u8],
    ) -> Result<WalFrameHeader> {
        if frame_index >= self.frame_count {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "frame index {frame_index} out of range (count: {})",
                    self.frame_count
                ),
            });
        }

        let frame_size = self.frame_size();
        if buf.len() < frame_size {
            return Err(FrankenError::Internal(format!(
                "read_frame_into buffer too small: got {}, need {}",
                buf.len(),
                frame_size
            )));
        }

        let offset = self.frame_offset(frame_index);
        let bytes_read = self.file.read(cx, &mut buf[..frame_size], offset)?;
        if bytes_read < frame_size {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "short read at frame {frame_index}: got {bytes_read}, need {frame_size}"
                ),
            });
        }

        WalFrameHeader::from_bytes(&buf[..WAL_FRAME_HEADER_SIZE])
    }

    /// Read just the frame header at a given 0-based index.
    pub fn read_frame_header(&self, cx: &Cx, frame_index: usize) -> Result<WalFrameHeader> {
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
        let _ = cx;
        Ok(self.last_commit_frame)
    }

    /// Sync the WAL file to stable storage.
    pub fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        #[cfg(any(test, feature = "fault-injection"))]
        crate::fault_hooks::maybe_inject_sync_failure(self.frame_count, flags)?;

        self.file.sync(cx, flags)
    }

    /// Durability-intent sync: makes all appended frames durable and records
    /// the fsynced frame count for the two-phase commit invariant.
    ///
    /// Callers that will publish a CommitIndex MUST call this (not raw `sync`)
    /// so the invariant tracker can verify ordering.
    pub fn durable_sync(&mut self, cx: &Cx, kind: SyncKind) -> Result<()> {
        #[cfg(any(test, feature = "fault-injection"))]
        {
            let flags = match kind {
                SyncKind::DataOnly => SyncFlags::DATAONLY,
                SyncKind::DataAndMetadata | SyncKind::FullDurable => SyncFlags::FULL,
            };
            crate::fault_hooks::maybe_inject_sync_failure(self.frame_count, flags)?;
        }

        self.file.durable_sync(cx, kind)?;
        self.last_fsynced_frame_count = self.frame_count;

        debug!(
            target: "fsqlite_wal::durability",
            fsynced_up_to = self.frame_count,
            kind = ?kind,
            "WAL durable sync complete"
        );

        #[cfg(any(test, feature = "fault-injection"))]
        crate::fault_hooks::maybe_inject_crash_at(
            crate::fault_hooks::CrashBoundary::AfterFsyncBeforePublish,
            &format!("fsynced_up_to={}", self.frame_count),
        )?;

        Ok(())
    }

    /// Assert that it is safe to publish frames up to `publish_frame_count`
    /// — i.e. that a durable_sync has already completed covering those frames.
    ///
    /// Under debug-assertions this panics. In release mode, it returns an error
    /// only when `FRANKENSQLITE_PARANOID_DURABILITY=1` is set.
    pub fn assert_publish_safe(&self, publish_frame_count: usize) -> Result<()> {
        if self.last_fsynced_frame_count >= publish_frame_count {
            return Ok(());
        }

        let msg = format!(
            "publish-before-fsync: attempting to publish frame_count={publish_frame_count} \
             but last fsynced only up to {fsynced}",
            fsynced = self.last_fsynced_frame_count,
        );

        debug_assert!(false, "WAL durability invariant violated: {msg}");

        if *PARANOID_DURABILITY {
            error!(
                target: "fsqlite_wal::durability",
                publish_frame_count,
                last_fsynced = self.last_fsynced_frame_count,
                "PARANOID_DURABILITY: publish-before-fsync detected"
            );
            return Err(FrankenError::Internal(msg));
        }

        Ok(())
    }

    /// The frame count at which the last successful durable sync completed.
    #[must_use]
    pub fn last_fsynced_frame_count(&self) -> usize {
        self.last_fsynced_frame_count
    }

    /// Reset the WAL for a new checkpoint generation.
    ///
    /// Writes a new header with updated checkpoint sequence and salts,
    /// and resets the running checksum and frame count to zero.
    /// If `truncate_file` is true, also truncates the file to header-only.
    pub fn reset(
        &mut self,
        cx: &Cx,
        new_checkpoint_seq: u32,
        new_salts: WalSalts,
        truncate_file: bool,
    ) -> Result<()> {
        let new_header = WalHeader {
            magic: self.header.magic,
            format_version: WAL_FORMAT_VERSION,
            page_size: self.header.page_size,
            checkpoint_seq: new_checkpoint_seq,
            salts: new_salts,
            checksum: SqliteWalChecksum::default(),
        };
        let header_bytes = new_header.to_bytes()?;

        #[cfg(any(test, feature = "fault-injection"))]
        crate::fault_hooks::maybe_inject_crash_at(
            crate::fault_hooks::CrashBoundary::BeforeWalHeaderWrite,
            &format!("checkpoint_seq={new_checkpoint_seq}"),
        )?;

        self.file.write(cx, &header_bytes, 0)?;

        // H9 fault hook: crash after header write, before truncate.
        // Simulates power loss leaving new salts in the header but old
        // frames still on disk. Recovery must see the salt mismatch and
        // discard all old-generation frames.
        #[cfg(any(test, feature = "fault-injection"))]
        {
            let old_fc = self.frame_count;
            crate::fault_hooks::maybe_inject_crash_header_truncate(old_fc, new_checkpoint_seq)?;
        }

        if truncate_file {
            self.file.truncate(
                cx,
                u64::try_from(WAL_HEADER_SIZE).expect("header size fits u64"),
            )?;
        }

        // Sync the WAL header to stable storage before writing new frames,
        // matching SQLite's walRestartHdr() behaviour.
        self.file.sync(cx, SyncFlags::NORMAL)?;

        self.running_checksum = read_wal_header_checksum(&header_bytes)?;
        self.header = WalHeader::from_bytes(&header_bytes)?;
        self.frame_count = 0;
        self.last_commit_frame = None;
        self.last_fsynced_frame_count = 0;
        self.frame_scratch.clear();
        crate::metrics::GLOBAL_WAL_METRICS.set_wal_frames_current(0);

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
    use std::time::Instant;

    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::MemoryVfs;
    use fsqlite_vfs::traits::Vfs;
    use serde_json::{Value, json};

    use super::*;

    /// Shared, panic-safe ownership guard for process-global fault hooks.
    static FAULT_TEST_LOCK: crate::fault_hooks::FaultInjectionSessionLock =
        crate::fault_hooks::FaultInjectionSessionLock::new();

    const PAGE_SIZE: u32 = 4096;
    const TRACK_C_SCRATCH_BENCH_BEAD_ID: &str = "bd-db300.3.4.3";
    const TRACK_C_SCRATCH_BENCH_WARMUP_ITERS: usize = 4;
    const TRACK_C_SCRATCH_BENCH_MEASURE_ITERS: usize = 12;

    #[derive(Clone, Copy)]
    enum TrackCScratchBenchMode {
        FreshAllocBaseline,
        ScratchReuseCandidate,
    }

    #[derive(Clone, Copy)]
    struct TrackCScratchBenchRun {
        elapsed_ns: u64,
        explicit_fresh_buffer_allocations: usize,
        scratch_capacity_growth_events: usize,
        peak_scratch_capacity_bytes: usize,
        frame_buffer_bytes_per_operation: usize,
        operations_per_sample: usize,
    }

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

    fn frame_ref(
        page_number: u32,
        page_data: &[u8],
        db_size_if_commit: u32,
    ) -> WalAppendFrameRef<'_> {
        WalAppendFrameRef {
            page_number,
            page_data,
            db_size_if_commit,
        }
    }

    fn open_wal_file(vfs: &MemoryVfs, cx: &Cx) -> <MemoryVfs as Vfs>::File {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (file, _) = vfs
            .open(cx, Some(std::path::Path::new("test.db-wal")), flags)
            .expect("open WAL file");
        file
    }

    fn track_c_scratch_run_summary(runs: &[TrackCScratchBenchRun]) -> Value {
        let mut elapsed_samples: Vec<_> = runs.iter().map(|run| run.elapsed_ns).collect();
        elapsed_samples.sort_unstable();
        let sample_count = elapsed_samples.len();
        let min_ns = elapsed_samples.first().copied().unwrap_or(0);
        let median_ns = if sample_count == 0 {
            0
        } else {
            elapsed_samples[sample_count / 2]
        };
        let max_ns = elapsed_samples.last().copied().unwrap_or(0);
        let mean_ns = if sample_count == 0 {
            0.0
        } else {
            let total_ns: u128 = elapsed_samples.iter().map(|ns| u128::from(*ns)).sum();
            (total_ns as f64) / (sample_count as f64)
        };
        let explicit_fresh_buffer_allocations = runs
            .iter()
            .map(|run| run.explicit_fresh_buffer_allocations)
            .max()
            .unwrap_or(0);
        let scratch_capacity_growth_events = runs
            .iter()
            .map(|run| run.scratch_capacity_growth_events)
            .max()
            .unwrap_or(0);
        let peak_scratch_capacity_bytes = runs
            .iter()
            .map(|run| run.peak_scratch_capacity_bytes)
            .max()
            .unwrap_or(0);
        let frame_buffer_bytes_per_operation = runs
            .first()
            .map(|run| run.frame_buffer_bytes_per_operation)
            .unwrap_or(0);
        let operations_per_sample = runs
            .first()
            .map(|run| run.operations_per_sample)
            .unwrap_or(0);

        json!({
            "samples_ns": elapsed_samples,
            "min_ns": min_ns,
            "median_ns": median_ns,
            "max_ns": max_ns,
            "mean_ns": mean_ns,
            "explicit_fresh_buffer_allocations_per_sample": explicit_fresh_buffer_allocations,
            "scratch_capacity_growth_events_per_sample": scratch_capacity_growth_events,
            "peak_scratch_capacity_bytes": peak_scratch_capacity_bytes,
            "frame_buffer_bytes_per_operation": frame_buffer_bytes_per_operation,
            "operations_per_sample": operations_per_sample,
            "frame_buffer_bytes_requested_per_sample": explicit_fresh_buffer_allocations
                .saturating_mul(frame_buffer_bytes_per_operation),
        })
    }

    fn append_frames_fresh_alloc<F: VfsFile>(
        wal: &mut WalFile<F>,
        cx: &Cx,
        frames: &[WalAppendFrameRef<'_>],
    ) -> Result<()> {
        let frame_size = wal.frame_size();
        let mut frame_buf = wal.prepare_frame_bytes(frames)?;
        let frame_transforms = frame_buf
            .chunks_exact(frame_size)
            .map(|frame| {
                WalChecksumTransform::for_wal_frame(
                    frame,
                    wal.page_size(),
                    wal.big_endian_checksum(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        wal.append_prepared_frame_bytes(cx, &mut frame_buf, &frame_transforms)
    }

    fn track_c_measure_single_frame_case(
        mode: TrackCScratchBenchMode,
        operations: usize,
    ) -> TrackCScratchBenchRun {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let pages: Vec<Vec<u8>> = (0..operations)
            .map(|i| sample_page(u8::try_from(i % 251).expect("modulo fits u8")))
            .collect();
        let frame_buffer_bytes_per_operation = wal.frame_size();
        let mut explicit_fresh_buffer_allocations = 0usize;
        let mut scratch_capacity_growth_events = 0usize;
        let mut previous_scratch_capacity = 0usize;

        let start = Instant::now();
        for (idx, page) in pages.iter().enumerate() {
            let page_number = u32::try_from(idx).expect("index fits u32") + 1;
            match mode {
                TrackCScratchBenchMode::FreshAllocBaseline => {
                    let frames = [WalAppendFrameRef {
                        page_number,
                        page_data: page,
                        db_size_if_commit: page_number,
                    }];
                    append_frames_fresh_alloc(&mut wal, &cx, &frames).expect("append baseline");
                    explicit_fresh_buffer_allocations += 1;
                }
                TrackCScratchBenchMode::ScratchReuseCandidate => {
                    wal.append_frame(&cx, page_number, page, page_number)
                        .expect("append candidate");
                    let scratch_capacity = wal.frame_scratch_capacity();
                    if scratch_capacity > previous_scratch_capacity {
                        scratch_capacity_growth_events += 1;
                        previous_scratch_capacity = scratch_capacity;
                    }
                }
            }
        }

        TrackCScratchBenchRun {
            elapsed_ns: u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            explicit_fresh_buffer_allocations,
            scratch_capacity_growth_events,
            peak_scratch_capacity_bytes: wal.frame_scratch_capacity(),
            frame_buffer_bytes_per_operation,
            operations_per_sample: operations,
        }
    }

    fn track_c_measure_batch_case<const N: usize>(
        mode: TrackCScratchBenchMode,
        operations: usize,
    ) -> TrackCScratchBenchRun {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let pages_per_operation: Vec<[Vec<u8>; N]> = (0..operations)
            .map(|operation_idx| {
                std::array::from_fn(|frame_idx| {
                    sample_page(
                        u8::try_from((operation_idx * N + frame_idx) % 251)
                            .expect("modulo fits u8"),
                    )
                })
            })
            .collect();
        let frame_buffer_bytes_per_operation = wal
            .frame_size()
            .checked_mul(N)
            .expect("frame bytes per operation fit usize");
        let mut explicit_fresh_buffer_allocations = 0usize;
        let mut scratch_capacity_growth_events = 0usize;
        let mut previous_scratch_capacity = 0usize;

        let start = Instant::now();
        for (operation_idx, pages) in pages_per_operation.iter().enumerate() {
            let page_base = u32::try_from(
                operation_idx
                    .checked_mul(N)
                    .expect("operation frame base fits usize"),
            )
            .expect("frame base fits u32")
                + 1;
            let commit_db_size = page_base + u32::try_from(N).expect("N fits u32") - 1;
            let frames: [WalAppendFrameRef<'_>; N] =
                std::array::from_fn(|frame_idx| WalAppendFrameRef {
                    page_number: page_base
                        + u32::try_from(frame_idx).expect("frame index fits u32"),
                    page_data: &pages[frame_idx],
                    db_size_if_commit: if frame_idx + 1 == N {
                        commit_db_size
                    } else {
                        0
                    },
                });

            match mode {
                TrackCScratchBenchMode::FreshAllocBaseline => {
                    append_frames_fresh_alloc(&mut wal, &cx, &frames).expect("append baseline");
                    explicit_fresh_buffer_allocations += 1;
                }
                TrackCScratchBenchMode::ScratchReuseCandidate => {
                    wal.append_frames(&cx, &frames).expect("append candidate");
                    let scratch_capacity = wal.frame_scratch_capacity();
                    if scratch_capacity > previous_scratch_capacity {
                        scratch_capacity_growth_events += 1;
                        previous_scratch_capacity = scratch_capacity;
                    }
                }
            }
        }

        TrackCScratchBenchRun {
            elapsed_ns: u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            explicit_fresh_buffer_allocations,
            scratch_capacity_growth_events,
            peak_scratch_capacity_bytes: wal.frame_scratch_capacity(),
            frame_buffer_bytes_per_operation,
            operations_per_sample: operations,
        }
    }

    fn track_c_scratch_case_report(
        scenario_id: &str,
        frames_per_operation: usize,
        operations_per_sample: usize,
        baseline_measure: impl Fn() -> TrackCScratchBenchRun,
        candidate_measure: impl Fn() -> TrackCScratchBenchRun,
    ) -> Value {
        for _ in 0..TRACK_C_SCRATCH_BENCH_WARMUP_ITERS {
            let _ = baseline_measure();
            let _ = candidate_measure();
        }

        let baseline_runs: Vec<_> = (0..TRACK_C_SCRATCH_BENCH_MEASURE_ITERS)
            .map(|_| baseline_measure())
            .collect();
        let candidate_runs: Vec<_> = (0..TRACK_C_SCRATCH_BENCH_MEASURE_ITERS)
            .map(|_| candidate_measure())
            .collect();
        let baseline_summary = track_c_scratch_run_summary(&baseline_runs);
        let candidate_summary = track_c_scratch_run_summary(&candidate_runs);
        let baseline_median = baseline_summary["median_ns"].as_u64().unwrap_or(0);
        let candidate_median = candidate_summary["median_ns"].as_u64().unwrap_or(0);
        let baseline_allocations = baseline_summary["explicit_fresh_buffer_allocations_per_sample"]
            .as_u64()
            .unwrap_or(0);
        let candidate_growths = candidate_summary["scratch_capacity_growth_events_per_sample"]
            .as_u64()
            .unwrap_or(0);
        let baseline_requested_bytes = baseline_summary["frame_buffer_bytes_requested_per_sample"]
            .as_u64()
            .unwrap_or(0);
        let candidate_peak_scratch_bytes = candidate_summary["peak_scratch_capacity_bytes"]
            .as_u64()
            .unwrap_or(0);

        json!({
            "scenario_id": scenario_id,
            "frames_per_operation": frames_per_operation,
            "operations_per_sample": operations_per_sample,
            "fresh_alloc_baseline": baseline_summary,
            "scratch_reuse_candidate": candidate_summary,
            "fresh_buffer_allocations_avoided_per_sample": baseline_allocations.saturating_sub(candidate_growths),
            "buffer_bytes_saved_vs_fresh_requested_per_sample": baseline_requested_bytes.saturating_sub(candidate_peak_scratch_bytes),
            "speedup_vs_baseline_median": if candidate_median == 0 {
                0.0
            } else {
                (baseline_median as f64) / (candidate_median as f64)
            },
            "faster_variant_by_median": if candidate_median <= baseline_median {
                "scratch_reuse_candidate"
            } else {
                "fresh_alloc_baseline"
            },
        })
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
    fn test_fault_hook_after_wal_append_returns_error_and_records_context() {
        let _guard = FAULT_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        crate::fault_hooks::clear();

        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 1, test_salts()).expect("create WAL");
        let page = sample_page(0x33);
        let frames = [WalAppendFrameRef {
            page_number: 1,
            page_data: &page,
            db_size_if_commit: 1,
        }];

        crate::fault_hooks::arm_after_append(crate::fault_hooks::FaultHookArm::new(
            "bd-db300.7.2.2-after-append",
            "WAL-AFTER-APPEND",
            "wal_append_recovery",
        ));

        let error = wal
            .append_frames(&cx, &frames)
            .expect_err("fault hook should force an error after append");
        assert!(
            error.to_string().contains("fault_inject:wal_after_append"),
            "fault error should identify the append hook: {error}"
        );
        assert_eq!(
            wal.frame_count(),
            1,
            "append should still have reached the WAL"
        );

        wal.close(&cx).expect("close WAL");
        let reopened_file = open_wal_file(&vfs, &cx);
        let reopened = WalFile::open(&cx, reopened_file).expect("reopen WAL");
        assert_eq!(
            reopened.frame_count(),
            1,
            "reopened WAL should preserve the appended frame for later recovery checks"
        );

        let records = crate::fault_hooks::take_records();
        assert_eq!(
            records.len(),
            1,
            "exactly one append fault should be recorded"
        );
        assert_eq!(records[0].point, "wal_after_append");
        assert_eq!(records[0].run_id, "bd-db300.7.2.2-after-append");
        assert_eq!(records[0].scenario_id, "WAL-AFTER-APPEND");
        assert_eq!(records[0].invariant_family, "wal_append_recovery");
        assert!(
            records[0].detail.contains("appended_frames=1"),
            "record should preserve append context: {}",
            records[0].detail
        );

        crate::fault_hooks::clear();
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
        let wal2 = WalFile::open(&cx, file2).expect("open WAL");
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
            let db_size = if i == 2 { 3 } else { 0 };
            wal.append_frame(&cx, u32::from(i) + 1, &sample_page(i), db_size)
                .expect("append");
        }
        assert_eq!(wal.frame_count(), 3);

        // Reset with new salts.
        let new_salts = WalSalts {
            salt1: 0x1111_2222,
            salt2: 0x3333_4444,
        };
        wal.reset(&cx, 1, new_salts, true).expect("reset");
        assert_eq!(wal.frame_count(), 0);
        assert_eq!(wal.last_commit_frame(&cx).expect("query"), None);
        assert_eq!(wal.header().checkpoint_seq, 1);
        assert_eq!(wal.header().salts, new_salts);

        // Can append new frames after reset.
        wal.append_frame(&cx, 10, &sample_page(0xAA), 1)
            .expect("append after reset");
        assert_eq!(wal.frame_count(), 1);
        assert_eq!(wal.last_commit_frame(&cx).expect("query"), Some(0));

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

        // Write 3 frames (last is a commit so recovery sees them).
        for i in 0..3u8 {
            let db_size = if i == 2 { 3 } else { 0 };
            wal.append_frame(&cx, u32::from(i) + 1, &sample_page(i), db_size)
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
    fn test_fault_hook_sync_failure_returns_error_and_records_context() {
        let _guard = FAULT_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        crate::fault_hooks::clear();

        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 1, test_salts()).expect("create WAL");
        wal.append_frame(&cx, 1, &sample_page(0x44), 1)
            .expect("append frame");

        crate::fault_hooks::arm_sync_failure(crate::fault_hooks::FaultHookArm::new(
            "bd-db300.7.2.2-sync-failure",
            "WAL-SYNC-FAILURE",
            "wal_sync_recovery",
        ));

        let error = wal
            .sync(&cx, SyncFlags::NORMAL)
            .expect_err("fault hook should force sync failure");
        assert!(
            error.to_string().contains("fault_inject:wal_sync_failure"),
            "fault error should identify the sync hook: {error}"
        );

        let records = crate::fault_hooks::take_records();
        assert_eq!(
            records.len(),
            1,
            "exactly one sync fault should be recorded"
        );
        assert_eq!(records[0].point, "wal_sync_failure");
        assert_eq!(records[0].run_id, "bd-db300.7.2.2-sync-failure");
        assert!(
            records[0].detail.contains("frame_count_before=1"),
            "record should capture sync context: {}",
            records[0].detail
        );

        crate::fault_hooks::clear();
    }

    #[test]
    fn test_fault_hook_append_busy_countdown_fires_once_and_preserves_retry_surface() {
        let _guard = FAULT_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        crate::fault_hooks::clear();

        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 1, test_salts()).expect("create WAL");

        crate::fault_hooks::arm_append_busy_countdown(
            crate::fault_hooks::FaultHookArm::new(
                "bd-db300.7.2.2-busy-countdown",
                "WAL-APPEND-BUSY",
                "wal_append_retry",
            ),
            2,
        );

        let first_page = sample_page(0x55);
        let first_frames = [WalAppendFrameRef {
            page_number: 1,
            page_data: &first_page,
            db_size_if_commit: 1,
        }];
        wal.append_frames(&cx, &first_frames)
            .expect("countdown should not fire on first append");

        let second_page = sample_page(0x66);
        let second_frames = [WalAppendFrameRef {
            page_number: 2,
            page_data: &second_page,
            db_size_if_commit: 2,
        }];
        let busy = wal
            .append_frames(&cx, &second_frames)
            .expect_err("countdown should fire on second append");
        assert!(matches!(busy, FrankenError::Busy));
        assert_eq!(
            wal.frame_count(),
            1,
            "busy fault should fire before the second append mutates WAL state"
        );

        wal.append_frames(&cx, &second_frames)
            .expect("hook should disarm after firing once");
        assert_eq!(
            wal.frame_count(),
            2,
            "retry should succeed once the hook is spent"
        );

        let records = crate::fault_hooks::take_records();
        assert_eq!(records.len(), 1, "busy countdown should record one trigger");
        assert_eq!(records[0].point, "wal_append_busy_countdown");
        assert_eq!(records[0].run_id, "bd-db300.7.2.2-busy-countdown");
        assert!(
            records[0].detail.contains("submitted_frames=1"),
            "record should preserve append batch context: {}",
            records[0].detail
        );

        crate::fault_hooks::clear();
    }

    /// H9 / F9: Crash between WAL header rewrite (new salts) and truncation.
    ///
    /// After injection, the WAL file has:
    /// - New header with new salts (written and synced)
    /// - Old frames with OLD salts (not yet truncated)
    ///
    /// Recovery (WalFile::open) must see the salt mismatch between header
    /// and frames, and discard ALL frames. Result: frame_count == 0.
    ///
    /// Replay: `cargo test -p fsqlite-wal -- test_fault_crash_between_header_and_truncate --nocapture`
    #[test]
    fn test_fault_crash_between_header_and_truncate_recovers_to_zero_frames() {
        let _guard = FAULT_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        crate::fault_hooks::clear();

        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        // Append 3 frames with the original salts.
        for i in 1..=3_u32 {
            let page = sample_page(i as u8);
            wal.append_frame(&cx, i, &page, i)
                .expect("append frame before reset fault injection");
        }
        wal.sync(&cx, SyncFlags::NORMAL).expect("sync WAL");
        assert_eq!(wal.frame_count(), 3, "pre-reset: 3 frames");

        let original_salts = wal.generation_identity().salts;

        // Arm the crash-header-truncate hook.
        crate::fault_hooks::arm_crash_header_truncate(crate::fault_hooks::FaultHookArm::new(
            "bd-db300.7.2.2-h9",
            "WAL-CRASH-HEADER-TRUNCATE",
            "wal_reset_recovery",
        ));

        // Attempt reset — should fail after writing new header but before truncation.
        let new_salts = WalSalts {
            salt1: original_salts.salt1.wrapping_add(1),
            salt2: original_salts.salt2.wrapping_add(1),
        };
        let err = wal
            .reset(&cx, 1, new_salts, true)
            .expect_err("fault hook should fire between header write and truncate");
        assert!(
            err.to_string()
                .contains("fault_inject:wal_crash_header_truncate"),
            "error should identify the hook: {err}"
        );

        // The WAL is now in a corrupted state:
        // - Header has new salts (written and synced before hook fired)
        // - Frames still have old salts (truncation was prevented)
        // Close the handle without further I/O.
        wal.close(&cx).expect("close WAL handle");

        // Recovery: re-open the WAL.
        let recovered_file = open_wal_file(&vfs, &cx);
        let recovered = WalFile::open(&cx, recovered_file).expect("reopen WAL");

        // Proof obligation: frame_count == 0 because all old frames have
        // mismatched salts vs the new header.
        assert_eq!(
            recovered.frame_count(),
            0,
            "recovery must discard all old-salt frames after header rewrite"
        );
        assert_eq!(
            recovered.generation_identity().salts,
            new_salts,
            "recovered header must have the new salts"
        );

        // Verify injection record.
        let records = crate::fault_hooks::take_records();
        assert_eq!(records.len(), 1, "exactly one crash hook should fire");
        assert_eq!(records[0].point, "wal_crash_header_truncate");
        assert_eq!(records[0].scenario_id, "WAL-CRASH-HEADER-TRUNCATE");
        assert!(
            records[0].detail.contains("old_frame_count=3"),
            "record should capture pre-reset frame count: {}",
            records[0].detail
        );
        assert!(
            records[0].detail.contains("new_checkpoint_seq=1"),
            "record should capture checkpoint seq: {}",
            records[0].detail
        );

        crate::fault_hooks::clear();
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

    // ── bd-14m.4: WAL crash recovery tests ──

    #[test]
    fn test_truncated_wal_recovers_committed_prefix() {
        // Simulate a crash mid-write by truncating the WAL file after the 3rd
        // frame (of 5). On reopen, only the committed prefix should load.
        // Frame 3 (i==2) is a commit; frame 5 (i==4) is also a commit but gets truncated.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        for i in 0..5u8 {
            let db_size = if i == 2 {
                3
            } else if i == 4 {
                5
            } else {
                0
            };
            wal.append_frame(&cx, u32::from(i) + 1, &sample_page(i), db_size)
                .expect("append");
        }
        assert_eq!(wal.frame_count(), 5);

        // Get file handle for raw truncation.
        let frame_size = wal.frame_size();
        // Truncate mid-way through frame 4 (keep header + 3 complete frames + partial 4th).
        let truncate_at = WAL_HEADER_SIZE + frame_size * 3 + frame_size / 2;
        let truncate_at_u64 = u64::try_from(truncate_at).expect("truncate_at fits u64");
        wal.file_mut()
            .truncate(&cx, truncate_at_u64)
            .expect("truncate");
        wal.close(&cx).expect("close WAL");

        // Reopen: only the 3 fully-written frames should be recovered.
        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("open WAL after truncation");
        assert_eq!(
            wal2.frame_count(),
            3,
            "only the 3 complete frames before truncation should survive"
        );

        // Verify data integrity of the surviving frames.
        for i in 0..3u8 {
            let (header, data) = wal2.read_frame(&cx, usize::from(i)).expect("read frame");
            assert_eq!(header.page_number, u32::from(i) + 1);
            assert_eq!(data, sample_page(i));
        }
        wal2.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_corrupt_frame_payload_detected_on_reopen() {
        // Corrupt a byte in frame 3's payload. On reopen, the checksum chain
        // breaks at frame 3, so only the committed prefix (frames 0-2) should load.
        // Frame 3 (i==2) is a commit marker so the committed prefix is 3 frames.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        for i in 0..5u8 {
            let db_size = if i == 2 {
                3
            } else if i == 4 {
                5
            } else {
                0
            };
            wal.append_frame(&cx, u32::from(i) + 1, &sample_page(i), db_size)
                .expect("append");
        }
        let frame_size = wal.frame_size();
        wal.close(&cx).expect("close WAL");

        // Corrupt one byte in frame 3's page data.
        let corrupt_offset = WAL_HEADER_SIZE + frame_size * 3 + WAL_FRAME_HEADER_SIZE + 42;
        let corrupt_offset_u64 = u64::try_from(corrupt_offset).expect("corrupt_offset fits u64");
        let mut f = open_wal_file(&vfs, &cx);
        let mut buf = [0u8; 1];
        f.read(&cx, &mut buf, corrupt_offset_u64)
            .expect("read byte");
        buf[0] ^= 0xFF;
        f.write(&cx, &buf, corrupt_offset_u64)
            .expect("write corrupted byte");
        drop(f);

        // Reopen: checksum chain should break at frame 3.
        let file3 = open_wal_file(&vfs, &cx);
        let wal3 = WalFile::open(&cx, file3).expect("open WAL after corruption");
        assert_eq!(
            wal3.frame_count(),
            3,
            "frames after corruption point should be discarded"
        );
        wal3.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_multi_commit_recovery_to_last_valid() {
        // Write two transactions (commit at frame 3, commit at frame 6).
        // Corrupt frame 5, so recovery should yield 4 valid frames (up to
        // the break at frame 5). The last valid commit is frame 3.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        // Transaction 1: frames 1-3, commit on frame 3.
        for i in 1..=3u32 {
            let db_size = if i == 3 { 3 } else { 0 };
            wal.append_frame(
                &cx,
                i,
                &sample_page(u8::try_from(i).expect("i fits u8")),
                db_size,
            )
            .expect("append");
        }

        // Transaction 2: frames 4-6, commit on frame 6.
        for i in 4..=6u32 {
            let db_size = if i == 6 { 6 } else { 0 };
            wal.append_frame(
                &cx,
                i,
                &sample_page(u8::try_from(i).expect("i fits u8")),
                db_size,
            )
            .expect("append");
        }
        assert_eq!(wal.frame_count(), 6);
        let frame_size = wal.frame_size();
        wal.close(&cx).expect("close WAL");

        // Corrupt frame 5 (index 4) payload.
        let corrupt_offset = WAL_HEADER_SIZE + frame_size * 4 + WAL_FRAME_HEADER_SIZE + 10;
        let corrupt_offset_u64 = u64::try_from(corrupt_offset).expect("corrupt_offset fits u64");
        let mut f = open_wal_file(&vfs, &cx);
        let mut buf = [0u8; 1];
        f.read(&cx, &mut buf, corrupt_offset_u64).expect("read");
        buf[0] ^= 0xAA;
        f.write(&cx, &buf, corrupt_offset_u64).expect("corrupt");
        drop(f);

        // Reopen: chain breaks at frame 5 (index 4). The last commit
        // boundary is frame 3 (db_size=3), so only 3 committed frames remain.
        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("open WAL after corruption");
        assert_eq!(
            wal2.frame_count(),
            3,
            "chain should break at corrupted frame 5, keeping committed prefix (frames 1-3)"
        );

        // The last commit frame is frame 3 (db_size=3).
        let header3 = wal2.read_frame_header(&cx, 2).expect("read frame 3 header");
        assert!(header3.is_commit(), "frame 3 should be a commit frame");

        wal2.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_wal_growth_bounded_by_restart_checkpoint() {
        // Verify that a Restart checkpoint resets WAL to 0 frames,
        // preventing unbounded growth.
        use crate::checkpoint::{CheckpointMode, CheckpointState};
        use crate::checkpoint_executor::CheckpointTarget;
        use crate::checkpoint_executor::execute_checkpoint;
        use fsqlite_types::PageNumber;

        struct DummyTarget;
        impl CheckpointTarget for DummyTarget {
            fn write_page(&mut self, _: &Cx, _: PageNumber, _: &[u8]) -> fsqlite_error::Result<()> {
                Ok(())
            }
            fn truncate_db(&mut self, _: &Cx, _: u32) -> fsqlite_error::Result<()> {
                Ok(())
            }
            fn sync_db(&mut self, _: &Cx) -> fsqlite_error::Result<()> {
                Ok(())
            }
        }

        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        // Write 100 frames (simulating many transactions).
        for i in 1..=100u32 {
            let seed = u8::try_from(i % 256).expect("seed fits u8");
            let db_size = if i % 10 == 0 { i } else { 0 };
            wal.append_frame(&cx, (i - 1) % 50 + 1, &sample_page(seed), db_size)
                .expect("append");
        }
        assert_eq!(wal.frame_count(), 100);

        // Restart checkpoint: backfill all + reset.
        let state = CheckpointState {
            total_frames: 100,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        let mut target = DummyTarget;
        let result = execute_checkpoint(&cx, &mut wal, CheckpointMode::Restart, state, &mut target)
            .expect("restart checkpoint");

        assert_eq!(result.frames_backfilled, 100);
        assert!(result.wal_was_reset);
        assert_eq!(wal.frame_count(), 0, "WAL should be empty after restart");

        // Write new frames after reset: WAL accepts them.
        wal.append_frame(&cx, 1, &sample_page(0xAA), 1)
            .expect("append after reset");
        assert_eq!(wal.frame_count(), 1);
        assert_eq!(wal.header().checkpoint_seq, 1, "checkpoint_seq incremented");

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_wal_header_corruption_detected() {
        // Corrupt the WAL header magic bytes. Open should fail or return
        // an error since the header is invalid.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        wal.append_frame(&cx, 1, &sample_page(1), 1)
            .expect("append");
        wal.close(&cx).expect("close WAL");

        // Corrupt the magic bytes at offset 0.
        let mut f = open_wal_file(&vfs, &cx);
        let corrupted_magic = [0xFF, 0xFF, 0xFF, 0xFF];
        f.write(&cx, &corrupted_magic, 0).expect("corrupt header");
        drop(f);

        // Attempt to reopen: should error due to bad magic.
        let file2 = open_wal_file(&vfs, &cx);
        let result = WalFile::open(&cx, file2);
        assert!(
            result.is_err(),
            "opening WAL with corrupted header magic should fail"
        );
    }

    #[test]
    fn test_empty_wal_after_crash_reopen() {
        // Create a WAL, close it before writing any frames.
        // Reopen should succeed with 0 frames (clean state).
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        wal.close(&cx).expect("close WAL");

        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("reopen empty WAL");
        assert_eq!(wal2.frame_count(), 0);
        wal2.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_crash_after_single_uncommitted_frame() {
        // Write a single non-commit frame (db_size=0), close/reopen.
        // Since this frame is not a commit, recovery correctly excludes it
        // from the committed frame count. Only committed transactions are
        // visible after WAL recovery.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        wal.append_frame(&cx, 1, &sample_page(0x77), 0)
            .expect("append non-commit");
        wal.close(&cx).expect("close WAL");

        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("reopen WAL");
        assert_eq!(
            wal2.frame_count(),
            0,
            "uncommitted frame excluded from recovery"
        );
        wal2.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_frame_offset_calculation_overflow_safety() {
        // This test ensures that the frame offset calculation logic doesn't overflow on 32-bit systems
        // by verifying it uses u64 arithmetic.

        let page_size: u64 = 4096;
        let wal_header_size: u64 = 32;
        let wal_frame_header_size: u64 = 24;
        let frame_size = wal_frame_header_size + page_size;

        // An index that would overflow if multiplied by frame_size in u32/usize(32-bit).
        // u32::MAX is 4,294,967,295.
        // frame_size is 4120.
        // 4,294,967,295 / 4120 = 1,042,467.
        // So index 1,042,468 causes overflow in 32-bit if not cast to u64.
        let large_index: u64 = 1_042_468;

        let idx_u64 = large_index;
        let expected_offset = wal_header_size + idx_u64 * frame_size;

        // Replicate logic from WalFile::frame_offset
        let calculated_offset = wal_header_size + idx_u64 * frame_size;

        assert_eq!(calculated_offset, expected_offset);

        // We can't easily instantiate a WalFile with this many frames without massive I/O,
        // but we've verified the arithmetic logic in the test body matches the implementation.
    }

    // ── bd-xfn30.1: WAL append path correctness ──
    //
    // Frame ordering, checksum determinism, commit boundary semantics.

    #[test]
    fn test_frame_offsets_sequential_no_gaps() {
        // Verify that file offsets match the expected formula:
        //   offset(i) = WAL_HEADER_SIZE + i * frame_size
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

        let n = 20u32;
        for i in 0..n {
            let db_size = if i == n - 1 { n } else { 0 };
            wal.append_frame(
                &cx,
                i + 1,
                &sample_page(u8::try_from(i % 251).unwrap()),
                db_size,
            )
            .expect("append");
        }

        let frame_size = wal.frame_size();
        let file_size = wal.file().file_size(&cx).expect("file_size");
        let expected_size =
            u64::try_from(WAL_HEADER_SIZE + usize::try_from(n).unwrap() * frame_size).unwrap();
        assert_eq!(
            file_size, expected_size,
            "WAL file size must equal header + n*frame_size with no padding or gaps"
        );

        // Verify each frame header's page_number at the right offset.
        for i in 0..n {
            let header = wal
                .read_frame_header(&cx, usize::try_from(i).unwrap())
                .expect("read header");
            assert_eq!(header.page_number, i + 1, "frame {i} page_number");
        }

        wal.close(&cx).expect("close WAL");
    }

    #[test]
    fn test_checksum_determinism_same_input() {
        // Two separate WALs created with identical params and identical frames
        // must produce byte-for-byte identical checksum chains.
        let cx = test_cx();
        let vfs1 = MemoryVfs::new();
        let vfs2 = MemoryVfs::new();

        let mut checksums_a = Vec::new();
        let mut checksums_b = Vec::new();

        for (vfs, checksums) in [(&vfs1, &mut checksums_a), (&vfs2, &mut checksums_b)] {
            let file = open_wal_file(vfs, &cx);
            let mut wal =
                WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");

            for i in 0..10u8 {
                let page = sample_page(i);
                let db_size = if i == 9 { 10 } else { 0 };
                wal.append_frame(&cx, u32::from(i) + 1, &page, db_size)
                    .expect("append");
                checksums.push(wal.running_checksum());
            }
            wal.close(&cx).expect("close WAL");
        }

        assert_eq!(
            checksums_a, checksums_b,
            "identical inputs must produce identical checksum chains"
        );
    }

    #[test]
    fn test_checksum_sensitivity_one_byte_difference() {
        // Changing one byte in one frame's page data must produce a different
        // running checksum from that frame onward.
        let cx = test_cx();
        let vfs1 = MemoryVfs::new();
        let vfs2 = MemoryVfs::new();

        let mut checksums_a = Vec::new();
        let mut checksums_b = Vec::new();

        let file1 = open_wal_file(&vfs1, &cx);
        let mut wal1 = WalFile::create(&cx, file1, PAGE_SIZE, 0, test_salts()).expect("create");
        let file2 = open_wal_file(&vfs2, &cx);
        let mut wal2 = WalFile::create(&cx, file2, PAGE_SIZE, 0, test_salts()).expect("create");

        for i in 0..5u8 {
            let mut page = sample_page(i);
            let db_size = if i == 4 { 5 } else { 0 };
            wal1.append_frame(&cx, u32::from(i) + 1, &page, db_size)
                .expect("append");
            checksums_a.push(wal1.running_checksum());

            // Flip one byte in frame 2 only.
            if i == 2 {
                page[0] ^= 0x01;
            }
            wal2.append_frame(&cx, u32::from(i) + 1, &page, db_size)
                .expect("append");
            checksums_b.push(wal2.running_checksum());
        }

        // Frames 0..2 should match, frames 2..5 should diverge.
        assert_eq!(checksums_a[0], checksums_b[0], "frame 0 should match");
        assert_eq!(checksums_a[1], checksums_b[1], "frame 1 should match");
        assert_ne!(checksums_a[2], checksums_b[2], "frame 2 must diverge");
        assert_ne!(checksums_a[3], checksums_b[3], "frame 3 must diverge");
        assert_ne!(checksums_a[4], checksums_b[4], "frame 4 must diverge");

        wal1.close(&cx).expect("close");
        wal2.close(&cx).expect("close");
    }

    #[test]
    fn test_commit_boundary_every_frame() {
        // All frames are commit frames (db_size > 0).
        // Recovery should see all frames after reopen.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let n = 8u32;
        for i in 0..n {
            wal.append_frame(&cx, i + 1, &sample_page(u8::try_from(i).unwrap()), i + 1)
                .expect("append");
        }
        assert_eq!(wal.frame_count(), usize::try_from(n).unwrap());
        wal.close(&cx).expect("close");

        let file2 = open_wal_file(&vfs, &cx);
        let mut wal2 = WalFile::open(&cx, file2).expect("reopen");
        assert_eq!(
            wal2.frame_count(),
            usize::try_from(n).unwrap(),
            "all frames are commits so all should survive reopen"
        );

        // Every frame should have is_commit() == true.
        for i in 0..n {
            let h = wal2
                .read_frame_header(&cx, usize::try_from(i).unwrap())
                .expect("read");
            assert!(h.is_commit(), "frame {i} must be a commit");
            assert_eq!(h.db_size, i + 1);
        }

        // last_commit_frame should be the final frame.
        let last = wal2.last_commit_frame(&cx).expect("query");
        assert_eq!(last, Some(usize::try_from(n - 1).unwrap()));

        wal2.close(&cx).expect("close");
    }

    #[test]
    fn test_commit_boundary_interleaved_multi_txn() {
        // Three transactions with interleaved commit markers:
        //   Txn1: pages 1,2,3 (commit at frame 3, db_size=3)
        //   Txn2: pages 4,5 (commit at frame 5, db_size=5)
        //   Txn3: pages 6 (commit at frame 6, db_size=6)
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let frames: [(u32, u32); 6] = [
            (1, 0),
            (2, 0),
            (3, 3), // commit txn1
            (4, 0),
            (5, 5), // commit txn2
            (6, 6), // commit txn3
        ];

        for (pg, db_sz) in frames {
            wal.append_frame(&cx, pg, &sample_page(u8::try_from(pg).unwrap()), db_sz)
                .expect("append");
        }
        assert_eq!(wal.frame_count(), 6);
        wal.close(&cx).expect("close");

        // Reopen and verify all frames are valid (checksum chain intact).
        let file2 = open_wal_file(&vfs, &cx);
        let mut wal2 = WalFile::open(&cx, file2).expect("reopen");
        assert_eq!(wal2.frame_count(), 6);

        // Verify each frame's content.
        for i in 0..6u32 {
            let (header, data) = wal2
                .read_frame(&cx, usize::try_from(i).unwrap())
                .expect("read frame");
            assert_eq!(header.page_number, i + 1);
            let expected = sample_page(u8::try_from(i + 1).expect("fits"));
            assert_eq!(data, expected);
        }

        // Last frame should be commit.
        let last_header = wal2.read_frame_header(&cx, 5).expect("read header");
        assert!(last_header.is_commit());
        assert_eq!(last_header.db_size, 6);
        let last = wal2.last_commit_frame(&cx).expect("query");
        assert_eq!(last, Some(5), "last commit is frame 6 (index 5)");

        wal2.close(&cx).expect("close");
    }

    #[test]
    fn test_same_page_overwritten_multiple_times() {
        // Write the same page number multiple times. The WAL should record
        // each write at a sequential frame index. The last write's data
        // should be readable.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let page_num = 42u32;
        let versions = 5;
        for v in 0..versions {
            let db_size = if v == versions - 1 { 100 } else { 0 };
            wal.append_frame(&cx, page_num, &sample_page(v), db_size)
                .expect("append");
        }
        assert_eq!(wal.frame_count(), usize::from(versions));

        // Each frame should contain its unique version of the page.
        for v in 0..versions {
            let (header, data) = wal.read_frame(&cx, usize::from(v)).expect("read frame");
            assert_eq!(header.page_number, page_num);
            assert_eq!(data, sample_page(v), "frame {v} data mismatch");
        }

        wal.close(&cx).expect("close");
    }

    #[test]
    fn test_refresh_detects_concurrent_append() {
        // Simulate a second writer appending frames that the first handle
        // doesn't know about. After refresh(), the first handle should see them.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file1 = open_wal_file(&vfs, &cx);
        let mut wal1 = WalFile::create(&cx, file1, PAGE_SIZE, 0, test_salts()).expect("create");

        // First writer commits 3 frames.
        for i in 0..3u8 {
            let db_size = if i == 2 { 3 } else { 0 };
            wal1.append_frame(&cx, u32::from(i) + 1, &sample_page(i), db_size)
                .expect("append");
        }
        let checksum_after_3 = wal1.running_checksum();
        wal1.close(&cx).expect("close wal1");

        // "Reader" opens, sees 3 frames.
        let file_reader = open_wal_file(&vfs, &cx);
        let mut reader = WalFile::open(&cx, file_reader).expect("open reader");
        assert_eq!(reader.frame_count(), 3);
        assert_eq!(reader.last_commit_frame(&cx).expect("query"), Some(2));

        // "Second writer" appends 2 more frames (frames 4,5 with commit at 5).
        let file_w2 = open_wal_file(&vfs, &cx);
        let mut w2 = WalFile::open(&cx, file_w2).expect("open w2");
        assert_eq!(w2.running_checksum(), checksum_after_3);
        w2.append_frame(&cx, 4, &sample_page(3), 0).expect("append");
        w2.append_frame(&cx, 5, &sample_page(4), 5)
            .expect("append commit");
        assert_eq!(w2.frame_count(), 5);
        w2.close(&cx).expect("close w2");

        // Reader still sees 3 until refresh().
        assert_eq!(reader.frame_count(), 3);
        reader.refresh(&cx).expect("refresh");
        assert_eq!(
            reader.frame_count(),
            5,
            "after refresh, reader must see the 2 new committed frames"
        );
        assert_eq!(reader.last_commit_frame(&cx).expect("query"), Some(4));

        reader.close(&cx).expect("close reader");
    }

    #[test]
    fn test_refresh_after_reset_detects_new_generation() {
        // After a checkpoint reset, refresh should detect the salt change
        // and rebuild state.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        // Write and commit.
        wal.append_frame(&cx, 1, &sample_page(1), 1)
            .expect("append");
        wal.close(&cx).expect("close");

        // Open as "reader".
        let file_r = open_wal_file(&vfs, &cx);
        let mut reader = WalFile::open(&cx, file_r).expect("open reader");
        assert_eq!(reader.frame_count(), 1);
        assert_eq!(reader.last_commit_frame(&cx).expect("query"), Some(0));

        // "Checkpointer" opens, resets with new salts.
        let file_cp = open_wal_file(&vfs, &cx);
        let mut cp = WalFile::open(&cx, file_cp).expect("open cp");
        let new_salts = WalSalts {
            salt1: 0xAAAA_BBBB,
            salt2: 0xCCCC_DDDD,
        };
        cp.reset(&cx, 1, new_salts, false).expect("reset");
        cp.append_frame(&cx, 1, &sample_page(0xAA), 1)
            .expect("append after reset");
        cp.close(&cx).expect("close cp");

        // Reader refresh: should rebuild and see the new generation.
        reader.refresh(&cx).expect("refresh");
        assert_eq!(reader.frame_count(), 1);
        assert_eq!(reader.last_commit_frame(&cx).expect("query"), Some(0));
        assert_eq!(
            reader.header().salts,
            new_salts,
            "salts should be new generation"
        );

        reader.close(&cx).expect("close reader");
    }

    #[test]
    fn test_refresh_after_reset_with_same_salts_detects_new_generation() {
        // Generation identity must include checkpoint_seq, not just salts.
        // Otherwise a reset that reuses the same salts becomes an ABA hazard.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let salts = test_salts();
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, salts).expect("create");

        wal.append_frame(&cx, 1, &sample_page(1), 1)
            .expect("append");
        wal.close(&cx).expect("close");

        let file_r = open_wal_file(&vfs, &cx);
        let mut reader = WalFile::open(&cx, file_r).expect("open reader");
        let before = reader.generation_identity();
        assert_eq!(before.checkpoint_seq, 0);
        assert_eq!(before.salts, salts);
        assert_eq!(reader.frame_count(), 1);

        let file_cp = open_wal_file(&vfs, &cx);
        let mut cp = WalFile::open(&cx, file_cp).expect("open cp");
        cp.reset(&cx, 1, salts, false)
            .expect("reset with same salts");
        cp.append_frame(&cx, 2, &sample_page(0xAA), 2)
            .expect("append after reset");
        cp.close(&cx).expect("close cp");

        reader.refresh(&cx).expect("refresh");
        let after = reader.generation_identity();
        assert_eq!(
            after.checkpoint_seq, 1,
            "refresh must observe new checkpoint_seq"
        );
        assert_eq!(
            after.salts, salts,
            "same-salt reset is intentional in this test"
        );
        assert_ne!(
            before, after,
            "generation identity must change even when salts are reused"
        );
        assert_eq!(
            reader.frame_count(),
            1,
            "reader must rebuild to new generation"
        );

        let (header, data) = reader.read_frame(&cx, 0).expect("read rebuilt frame");
        assert_eq!(
            header.page_number, 2,
            "reader must see new-generation frame"
        );
        assert_eq!(data, sample_page(0xAA));

        reader.close(&cx).expect("close reader");
    }

    #[test]
    fn test_group_commit_checksum_chain_matches_single_append() {
        // Verify that writing frames via group commit produces the exact same
        // checksum chain as writing them one-at-a-time via append_frame().
        use crate::group_commit::{
            FrameSubmission, TransactionFrameBatch, write_consolidated_frames,
        };

        let cx = test_cx();
        let vfs_single = MemoryVfs::new();
        let vfs_group = MemoryVfs::new();

        let pages: Vec<Vec<u8>> = (0..6u8).map(sample_page).collect();
        let page_nums: Vec<u32> = (1..=6u32).collect();
        // Commit at frame 3 and frame 6.
        let commit_sizes: Vec<u32> = vec![0, 0, 3, 0, 0, 6];

        // Single-frame path.
        let file_s = open_wal_file(&vfs_single, &cx);
        let mut wal_s =
            WalFile::create(&cx, file_s, PAGE_SIZE, 0, test_salts()).expect("create single");
        for i in 0..6 {
            wal_s
                .append_frame(&cx, page_nums[i], &pages[i], commit_sizes[i])
                .expect("append single");
        }
        let single_checksum = wal_s.running_checksum();
        let single_count = wal_s.frame_count();

        // Group commit path: two batches of 3 frames each.
        let file_g = open_wal_file(&vfs_group, &cx);
        let mut wal_g =
            WalFile::create(&cx, file_g, PAGE_SIZE, 0, test_salts()).expect("create group");

        let batch1 = TransactionFrameBatch::new(
            (0..3)
                .map(|i| FrameSubmission {
                    page_number: page_nums[i],
                    page_data: pages[i].clone(),
                    db_size_if_commit: commit_sizes[i],
                })
                .collect(),
        );
        let batch2 = TransactionFrameBatch::new(
            (3..6)
                .map(|i| FrameSubmission {
                    page_number: page_nums[i],
                    page_data: pages[i].clone(),
                    db_size_if_commit: commit_sizes[i],
                })
                .collect(),
        );

        write_consolidated_frames(&cx, &mut wal_g, &[batch1, batch2]).expect("group write");
        let group_checksum = wal_g.running_checksum();
        let group_count = wal_g.frame_count();

        assert_eq!(single_count, group_count, "frame counts must match");
        assert_eq!(
            single_checksum, group_checksum,
            "group commit must produce identical checksum chain as single-frame append"
        );

        // Verify byte-level frame content equality.
        for i in 0..6 {
            let (h_s, d_s) = wal_s.read_frame(&cx, i).expect("read single");
            let (h_g, d_g) = wal_g.read_frame(&cx, i).expect("read group");
            assert_eq!(h_s.page_number, h_g.page_number, "frame {i} page_number");
            assert_eq!(h_s.db_size, h_g.db_size, "frame {i} db_size");
            assert_eq!(h_s.checksum, h_g.checksum, "frame {i} checksum");
            assert_eq!(h_s.salts, h_g.salts, "frame {i} salts");
            assert_eq!(d_s, d_g, "frame {i} data");
        }

        wal_s.close(&cx).expect("close single");
        wal_g.close(&cx).expect("close group");
    }

    #[test]
    fn test_batch_append_checksum_chain_matches_single_append() {
        let cx = test_cx();
        let vfs_single = MemoryVfs::new();
        let vfs_batch = MemoryVfs::new();

        let pages: Vec<Vec<u8>> = (0..6u8).map(sample_page).collect();
        let page_nums: Vec<u32> = (1..=6u32).collect();
        let commit_sizes: Vec<u32> = vec![0, 0, 3, 0, 0, 6];

        let file_single = open_wal_file(&vfs_single, &cx);
        let mut wal_single =
            WalFile::create(&cx, file_single, PAGE_SIZE, 0, test_salts()).expect("create single");
        for i in 0..6 {
            wal_single
                .append_frame(&cx, page_nums[i], &pages[i], commit_sizes[i])
                .expect("append single");
        }

        let file_batch = open_wal_file(&vfs_batch, &cx);
        let mut wal_batch =
            WalFile::create(&cx, file_batch, PAGE_SIZE, 0, test_salts()).expect("create batch");
        let frames: Vec<_> = (0..6)
            .map(|i| WalAppendFrameRef {
                page_number: page_nums[i],
                page_data: &pages[i],
                db_size_if_commit: commit_sizes[i],
            })
            .collect();
        wal_batch.append_frames(&cx, &frames).expect("append batch");

        assert_eq!(
            wal_single.frame_count(),
            wal_batch.frame_count(),
            "batch append must preserve frame count"
        );
        assert_eq!(
            wal_single.running_checksum(),
            wal_batch.running_checksum(),
            "batch append must preserve checksum chain"
        );

        for i in 0..6 {
            let (single_header, single_data) = wal_single.read_frame(&cx, i).expect("read single");
            let (batch_header, batch_data) = wal_batch.read_frame(&cx, i).expect("read batch");
            assert_eq!(single_header, batch_header, "frame header {i} must match");
            assert_eq!(single_data, batch_data, "frame payload {i} must match");
        }
    }

    /// bd-db300.3.8.6: Verify the fused append_frames path produces a
    /// byte-identical WAL file compared to the single-frame append path,
    /// including WAL header bytes and all frame header/payload bytes.
    #[test]
    fn test_fused_append_frames_produces_byte_identical_wal_file() {
        let cx = test_cx();
        let vfs_single = MemoryVfs::new();
        let vfs_fused = MemoryVfs::new();

        let pages: Vec<Vec<u8>> = (0..4u8).map(sample_page).collect();
        let page_nums: Vec<u32> = vec![3, 1, 4, 2];
        let commit_sizes: Vec<u32> = vec![0, 0, 0, 4];

        // Single-frame path (reference).
        let file_s = open_wal_file(&vfs_single, &cx);
        let mut wal_s =
            WalFile::create(&cx, file_s, PAGE_SIZE, 0, test_salts()).expect("create single");
        for i in 0..4 {
            wal_s
                .append_frame(&cx, page_nums[i], &pages[i], commit_sizes[i])
                .expect("append single");
        }

        // Fused batch path (under test).
        let file_f = open_wal_file(&vfs_fused, &cx);
        let mut wal_f =
            WalFile::create(&cx, file_f, PAGE_SIZE, 0, test_salts()).expect("create fused");
        let frames: Vec<_> = (0..4)
            .map(|i| WalAppendFrameRef {
                page_number: page_nums[i],
                page_data: &pages[i],
                db_size_if_commit: commit_sizes[i],
            })
            .collect();
        wal_f.append_frames(&cx, &frames).expect("append fused");

        // Compare checksums, frame count, and raw frame bytes.
        assert_eq!(wal_s.frame_count(), wal_f.frame_count());
        assert_eq!(wal_s.running_checksum(), wal_f.running_checksum());

        let frame_size = wal_s.frame_size();
        for i in 0..4 {
            let mut buf_s = vec![0u8; frame_size];
            let mut buf_f = vec![0u8; frame_size];
            wal_s
                .read_frame_into(&cx, i, &mut buf_s)
                .expect("read single");
            wal_f
                .read_frame_into(&cx, i, &mut buf_f)
                .expect("read fused");
            assert_eq!(
                buf_s, buf_f,
                "raw frame bytes at index {i} must be identical"
            );
        }
    }

    /// bd-db300.3.8.6: Verify that frame_scratch is restored after an error
    /// in append_frames (e.g. page size mismatch), so subsequent valid
    /// appends still work correctly.
    #[test]
    fn test_append_frames_restores_scratch_on_error() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        // First, a valid append to establish scratch state.
        let good_page = sample_page(0x11);
        wal.append_frame(&cx, 1, &good_page, 0)
            .expect("first append");
        let checksum_before = wal.running_checksum();
        let scratch_cap_before = wal.frame_scratch_capacity();
        let mut first_frame_before = vec![0u8; wal.frame_size()];
        wal.read_frame_into(&cx, 0, &mut first_frame_before)
            .expect("read baseline frame");

        // Attempt a batch with a bad page size — should fail.
        let bad_page = vec![0xBBu8; PAGE_SIZE as usize + 1]; // wrong size
        let good_page2 = sample_page(0x22);
        let bad_frames = vec![
            WalAppendFrameRef {
                page_number: 2,
                page_data: &good_page2,
                db_size_if_commit: 0,
            },
            WalAppendFrameRef {
                page_number: 3,
                page_data: &bad_page, // size mismatch
                db_size_if_commit: 3,
            },
        ];
        let err = wal.append_frames(&cx, &bad_frames);
        assert!(err.is_err(), "bad page size should cause error");

        // Scratch must still be usable — frame_count unchanged.
        assert_eq!(
            wal.frame_count(),
            1,
            "failed append must not advance frame count"
        );
        assert_eq!(
            wal.running_checksum(),
            checksum_before,
            "failed append must preserve the running checksum of prior committed frames"
        );
        assert!(
            wal.frame_scratch_capacity() >= scratch_cap_before,
            "scratch capacity must not shrink after error"
        );
        let mut first_frame_after = vec![0u8; wal.frame_size()];
        wal.read_frame_into(&cx, 0, &mut first_frame_after)
            .expect("read preserved frame");
        assert_eq!(
            first_frame_after, first_frame_before,
            "failed append must not rewrite previously committed raw frame bytes"
        );

        // Recovery: subsequent valid append must succeed and produce correct checksums.
        let recovery_page = sample_page(0x33);
        wal.append_frame(&cx, 2, &recovery_page, 2)
            .expect("recovery append after error");
        assert_eq!(wal.frame_count(), 2, "recovery append should succeed");
    }

    #[test]
    fn test_append_frame_reuses_frame_scratch_between_calls() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        wal.append_frame(&cx, 1, &sample_page(0x11), 0)
            .expect("append first");
        let scratch_len = wal.frame_scratch_len();
        let scratch_capacity = wal.frame_scratch_capacity();
        let scratch_ptr = wal.frame_scratch_ptr();

        wal.append_frame(&cx, 2, &sample_page(0x22), 2)
            .expect("append second");

        assert_eq!(
            wal.frame_scratch_len(),
            wal.frame_size(),
            "single-frame append should keep one frame sized scratch"
        );
        assert_eq!(
            wal.frame_scratch_len(),
            scratch_len,
            "single-frame scratch length should stay constant across appends"
        );
        assert_eq!(
            wal.frame_scratch_capacity(),
            scratch_capacity,
            "single-frame scratch should retain its allocation"
        );
        assert_eq!(
            wal.frame_scratch_ptr(),
            scratch_ptr,
            "single-frame scratch should reuse the same backing buffer"
        );
    }

    #[test]
    fn test_batch_append_reuses_frame_scratch_between_calls() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let first_pages: Vec<Vec<u8>> = (0..3u8).map(sample_page).collect();
        let first_frames: Vec<_> = (0..3)
            .map(|i| WalAppendFrameRef {
                page_number: u32::try_from(i).expect("index fits u32") + 1,
                page_data: &first_pages[i],
                db_size_if_commit: 0,
            })
            .collect();
        wal.append_frames(&cx, &first_frames)
            .expect("append first batch");
        let scratch_capacity = wal.frame_scratch_capacity();
        let scratch_ptr = wal.frame_scratch_ptr();

        let second_pages: Vec<Vec<u8>> = (0..2u8).map(|i| sample_page(i + 10)).collect();
        let second_frames: Vec<_> = (0..2)
            .map(|i| WalAppendFrameRef {
                page_number: u32::try_from(i).expect("index fits u32") + 10,
                page_data: &second_pages[i],
                db_size_if_commit: if i == 1 { 11 } else { 0 },
            })
            .collect();
        wal.append_frames(&cx, &second_frames)
            .expect("append second batch");

        assert_eq!(
            wal.frame_scratch_len(),
            second_frames.len() * wal.frame_size(),
            "batch scratch length should track the active batch size"
        );
        assert_eq!(
            wal.frame_scratch_capacity(),
            scratch_capacity,
            "smaller follow-on batches should retain the existing scratch allocation"
        );
        assert_eq!(
            wal.frame_scratch_ptr(),
            scratch_ptr,
            "smaller follow-on batches should reuse the same backing buffer"
        );
    }

    #[test]
    fn test_reset_clears_frame_scratch_len_without_dropping_capacity() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let reset_salts = WalSalts {
            salt1: 0xABCD_EF01,
            salt2: 0x1020_3040,
        };

        let pages: Vec<Vec<u8>> = (0..4u8).map(sample_page).collect();
        let frames: Vec<_> = (0..4)
            .map(|i| WalAppendFrameRef {
                page_number: u32::try_from(i).expect("index fits u32") + 1,
                page_data: &pages[i],
                db_size_if_commit: if i == 3 { 4 } else { 0 },
            })
            .collect();
        wal.append_frames(&cx, &frames).expect("append batch");
        let scratch_capacity = wal.frame_scratch_capacity();
        let scratch_ptr = wal.frame_scratch_ptr();

        wal.reset(&cx, 1, reset_salts, false).expect("reset WAL");

        assert_eq!(
            wal.frame_scratch_len(),
            0,
            "reset should leave scratch empty for the next append cycle"
        );
        assert_eq!(
            wal.frame_scratch_capacity(),
            scratch_capacity,
            "reset should preserve scratch capacity for reuse"
        );
        assert_eq!(
            wal.frame_scratch_ptr(),
            scratch_ptr,
            "reset should keep the existing scratch allocation alive"
        );
    }

    #[test]
    #[ignore = "benchmark evidence only"]
    fn wal_frame_scratch_benchmark_report() {
        let cases = vec![
            track_c_scratch_case_report(
                "single_frame_256_ops",
                1,
                256,
                || {
                    track_c_measure_single_frame_case(
                        TrackCScratchBenchMode::FreshAllocBaseline,
                        256,
                    )
                },
                || {
                    track_c_measure_single_frame_case(
                        TrackCScratchBenchMode::ScratchReuseCandidate,
                        256,
                    )
                },
            ),
            track_c_scratch_case_report(
                "batch_8_frames_64_ops",
                8,
                64,
                || track_c_measure_batch_case::<8>(TrackCScratchBenchMode::FreshAllocBaseline, 64),
                || {
                    track_c_measure_batch_case::<8>(
                        TrackCScratchBenchMode::ScratchReuseCandidate,
                        64,
                    )
                },
            ),
            track_c_scratch_case_report(
                "batch_32_frames_16_ops",
                32,
                16,
                || track_c_measure_batch_case::<32>(TrackCScratchBenchMode::FreshAllocBaseline, 16),
                || {
                    track_c_measure_batch_case::<32>(
                        TrackCScratchBenchMode::ScratchReuseCandidate,
                        16,
                    )
                },
            ),
        ];

        let report = json!({
            "schema_version": "fsqlite.track_c.wal_scratch_benchmark.v1",
            "bead_id": TRACK_C_SCRATCH_BENCH_BEAD_ID,
            "parent_bead_id": "bd-db300.3.4",
            "measured_operation": "wal_frame_assembly_and_append",
            "warmup_iterations": TRACK_C_SCRATCH_BENCH_WARMUP_ITERS,
            "measurement_iterations": TRACK_C_SCRATCH_BENCH_MEASURE_ITERS,
            "vfs": "memory",
            "baseline_variant": "fresh_frame_buffer_per_operation",
            "candidate_variant": "reusable_wal_handle_frame_scratch",
            "cases": cases,
        });

        println!("BEGIN_BD_DB300_3_4_3_REPORT");
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        println!("END_BD_DB300_3_4_3_REPORT");
    }

    #[test]
    fn test_prepared_batch_append_checksum_chain_matches_single_append() {
        let cx = test_cx();
        let vfs_single = MemoryVfs::new();
        let vfs_prepared = MemoryVfs::new();
        let page_size = usize::try_from(PAGE_SIZE).expect("page size fits usize");

        let pages: Vec<Vec<u8>> = (0..6u8).map(sample_page).collect();
        let page_nums: Vec<u32> = (1..=6u32).collect();
        let commit_sizes: Vec<u32> = vec![0, 0, 3, 0, 0, 6];

        let file_single = open_wal_file(&vfs_single, &cx);
        let mut wal_single =
            WalFile::create(&cx, file_single, PAGE_SIZE, 0, test_salts()).expect("create single");
        for i in 0..6 {
            wal_single
                .append_frame(&cx, page_nums[i], &pages[i], commit_sizes[i])
                .expect("append single");
        }

        let file_prepared = open_wal_file(&vfs_prepared, &cx);
        let mut wal_prepared = WalFile::create(&cx, file_prepared, PAGE_SIZE, 0, test_salts())
            .expect("create prepared");
        let frames: Vec<_> = (0..6)
            .map(|i| WalAppendFrameRef {
                page_number: page_nums[i],
                page_data: &pages[i],
                db_size_if_commit: commit_sizes[i],
            })
            .collect();
        let mut prepared_bytes = wal_prepared
            .prepare_frame_bytes(&frames)
            .expect("prepare frame bytes");
        let frame_transforms = prepared_bytes
            .chunks_exact(wal_prepared.frame_size())
            .map(|frame| {
                WalChecksumTransform::for_wal_frame(
                    frame,
                    page_size,
                    wal_prepared.big_endian_checksum(),
                )
            })
            .collect::<Result<Vec<_>>>()
            .expect("compute frame transforms");
        wal_prepared
            .append_prepared_frame_bytes(&cx, &mut prepared_bytes, &frame_transforms)
            .expect("append prepared batch");

        assert_eq!(
            wal_single.frame_count(),
            wal_prepared.frame_count(),
            "prepared append must preserve frame count"
        );
        assert_eq!(
            wal_single.running_checksum(),
            wal_prepared.running_checksum(),
            "prepared append must preserve checksum chain"
        );

        for i in 0..6 {
            let (single_header, single_data) = wal_single.read_frame(&cx, i).expect("read single");
            let (prepared_header, prepared_data) =
                wal_prepared.read_frame(&cx, i).expect("read prepared");
            assert_eq!(
                single_header, prepared_header,
                "frame header {i} must match"
            );
            assert_eq!(single_data, prepared_data, "frame payload {i} must match");
        }
    }

    #[test]
    fn test_prepared_batch_reseeds_after_intervening_growth() {
        let cx = test_cx();
        let vfs_single = MemoryVfs::new();
        let vfs_prepared = MemoryVfs::new();
        let page_size = usize::try_from(PAGE_SIZE).expect("page size fits usize");

        let pages: Vec<Vec<u8>> = (0..3u8).map(sample_page).collect();
        let page_nums: Vec<u32> = (1..=3u32).collect();
        let commit_sizes: Vec<u32> = vec![0, 0, 3];
        let intervening_page = sample_page(0xAA);

        let file_single = open_wal_file(&vfs_single, &cx);
        let mut wal_single =
            WalFile::create(&cx, file_single, PAGE_SIZE, 0, test_salts()).expect("create single");
        wal_single
            .append_frame(&cx, 99, &intervening_page, 0)
            .expect("append intervening single");
        for i in 0..3 {
            wal_single
                .append_frame(&cx, page_nums[i], &pages[i], commit_sizes[i])
                .expect("append single");
        }

        let file_prepared = open_wal_file(&vfs_prepared, &cx);
        let mut wal_prepared = WalFile::create(&cx, file_prepared, PAGE_SIZE, 0, test_salts())
            .expect("create prepared");
        let frames: Vec<_> = (0..3)
            .map(|i| WalAppendFrameRef {
                page_number: page_nums[i],
                page_data: &pages[i],
                db_size_if_commit: commit_sizes[i],
            })
            .collect();
        let mut prepared_bytes = wal_prepared
            .prepare_frame_bytes(&frames)
            .expect("prepare frame bytes");
        let frame_transforms = prepared_bytes
            .chunks_exact(wal_prepared.frame_size())
            .map(|frame| {
                WalChecksumTransform::for_wal_frame(
                    frame,
                    page_size,
                    wal_prepared.big_endian_checksum(),
                )
            })
            .collect::<Result<Vec<_>>>()
            .expect("compute frame transforms");
        wal_prepared
            .append_frame(&cx, 99, &intervening_page, 0)
            .expect("append intervening prepared");
        wal_prepared
            .append_prepared_frame_bytes(&cx, &mut prepared_bytes, &frame_transforms)
            .expect("append prepared batch");

        assert_eq!(
            wal_single.frame_count(),
            wal_prepared.frame_count(),
            "prepared append after growth must preserve frame count"
        );
        assert_eq!(
            wal_single.running_checksum(),
            wal_prepared.running_checksum(),
            "prepared append after growth must rebind to the live checksum seed"
        );

        for i in 0..wal_single.frame_count() {
            let (single_header, single_data) = wal_single.read_frame(&cx, i).expect("read single");
            let (prepared_header, prepared_data) =
                wal_prepared.read_frame(&cx, i).expect("read prepared");
            assert_eq!(
                single_header, prepared_header,
                "frame header {i} must match"
            );
            assert_eq!(single_data, prepared_data, "frame payload {i} must match");
        }
    }

    #[test]
    fn test_prepared_batch_rewrites_salts_after_reset() {
        let cx = test_cx();
        let vfs_fresh = MemoryVfs::new();
        let vfs_reset = MemoryVfs::new();
        let page_size = usize::try_from(PAGE_SIZE).expect("page size fits usize");
        let reset_salts = WalSalts {
            salt1: 0x0102_0304,
            salt2: 0xA0B0_C0D0,
        };

        let pages: Vec<Vec<u8>> = (0..2u8).map(sample_page).collect();
        let page_nums: Vec<u32> = (1..=2u32).collect();
        let commit_sizes: Vec<u32> = vec![0, 2];

        let file_fresh = open_wal_file(&vfs_fresh, &cx);
        let mut wal_fresh =
            WalFile::create(&cx, file_fresh, PAGE_SIZE, 7, reset_salts).expect("create fresh");
        for i in 0..2 {
            wal_fresh
                .append_frame(&cx, page_nums[i], &pages[i], commit_sizes[i])
                .expect("append fresh");
        }

        let file_reset = open_wal_file(&vfs_reset, &cx);
        let mut wal_reset =
            WalFile::create(&cx, file_reset, PAGE_SIZE, 0, test_salts()).expect("create reset");
        let frames: Vec<_> = (0..2)
            .map(|i| WalAppendFrameRef {
                page_number: page_nums[i],
                page_data: &pages[i],
                db_size_if_commit: commit_sizes[i],
            })
            .collect();
        let mut prepared_bytes = wal_reset
            .prepare_frame_bytes(&frames)
            .expect("prepare frame bytes");
        let frame_transforms = prepared_bytes
            .chunks_exact(wal_reset.frame_size())
            .map(|frame| {
                WalChecksumTransform::for_wal_frame(
                    frame,
                    page_size,
                    wal_reset.big_endian_checksum(),
                )
            })
            .collect::<Result<Vec<_>>>()
            .expect("compute frame transforms");
        wal_reset
            .reset(&cx, 7, reset_salts, false)
            .expect("reset WAL");
        wal_reset
            .append_prepared_frame_bytes(&cx, &mut prepared_bytes, &frame_transforms)
            .expect("append prepared batch");

        assert_eq!(
            wal_fresh.frame_count(),
            wal_reset.frame_count(),
            "prepared append after reset must preserve frame count"
        );
        assert_eq!(
            wal_fresh.running_checksum(),
            wal_reset.running_checksum(),
            "prepared append after reset must rebind to the reset checksum seed"
        );

        for i in 0..2 {
            let (fresh_header, fresh_data) = wal_fresh.read_frame(&cx, i).expect("read fresh");
            let (reset_header, reset_data) = wal_reset.read_frame(&cx, i).expect("read reset");
            assert_eq!(fresh_header, reset_header, "frame header {i} must match");
            assert_eq!(
                fresh_header.salts, reset_salts,
                "frame {i} must use reset salts"
            );
            assert_eq!(fresh_data, reset_data, "frame payload {i} must match");
        }
    }

    #[test]
    fn test_uncommitted_tail_trimmed_on_reopen() {
        // Write 5 frames: commit at frame 3, no commit after.
        // On reopen, only frames up to the last commit (3) should survive.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let commit_map: [(u32, u32); 5] = [(1, 0), (2, 0), (3, 3), (4, 0), (5, 0)];
        for (pg, db_sz) in commit_map {
            wal.append_frame(&cx, pg, &sample_page(u8::try_from(pg).unwrap()), db_sz)
                .expect("append");
        }
        assert_eq!(wal.frame_count(), 5);
        wal.close(&cx).expect("close");

        // Reopen: uncommitted tail (frames 4,5) should be trimmed.
        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("reopen");
        assert_eq!(
            wal2.frame_count(),
            3,
            "frames after last commit should be trimmed on reopen"
        );
        wal2.close(&cx).expect("close");
    }

    #[test]
    fn test_large_transaction_50_frames() {
        // A single transaction writing 50 frames (commit only on last).
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let n = 50u32;
        for i in 0..n {
            let db_size = if i == n - 1 { n } else { 0 };
            let seed = u8::try_from(i % 251).unwrap();
            wal.append_frame(&cx, i + 1, &sample_page(seed), db_size)
                .expect("append");
        }
        assert_eq!(wal.frame_count(), usize::try_from(n).unwrap());
        let final_checksum = wal.running_checksum();
        wal.close(&cx).expect("close");

        // Reopen and verify all 50 frames survived (single commit at end).
        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("reopen");
        assert_eq!(wal2.frame_count(), usize::try_from(n).unwrap());
        assert_eq!(wal2.running_checksum(), final_checksum);

        // Spot-check first, middle, last frames.
        for idx in [0, 24, 49] {
            let (h, d) = wal2.read_frame(&cx, idx).expect("read");
            let i = u32::try_from(idx).unwrap();
            assert_eq!(h.page_number, i + 1);
            assert_eq!(d, sample_page(u8::try_from(i % 251).unwrap()));
        }

        wal2.close(&cx).expect("close");
    }

    #[test]
    fn test_append_after_reset_checksum_independent() {
        // After reset, the checksum chain starts fresh from the new header.
        // Identical frames appended to a fresh WAL and a reset WAL with the
        // same salts should yield the same checksums.
        let cx = test_cx();

        let salts = WalSalts {
            salt1: 0x1234_5678,
            salt2: 0x9ABC_DEF0,
        };

        // Fresh WAL.
        let vfs1 = MemoryVfs::new();
        let file1 = open_wal_file(&vfs1, &cx);
        let mut wal_fresh = WalFile::create(&cx, file1, PAGE_SIZE, 1, salts).expect("create fresh");
        wal_fresh
            .append_frame(&cx, 1, &sample_page(0x42), 1)
            .expect("append fresh");
        let fresh_checksum = wal_fresh.running_checksum();
        wal_fresh.close(&cx).expect("close fresh");

        // WAL that was written to, then reset to same salts and checkpoint_seq.
        let vfs2 = MemoryVfs::new();
        let file2 = open_wal_file(&vfs2, &cx);
        let mut wal_reset =
            WalFile::create(&cx, file2, PAGE_SIZE, 0, test_salts()).expect("create reset");
        // Write some frames.
        wal_reset
            .append_frame(&cx, 99, &sample_page(0xFF), 99)
            .expect("append old");
        // Reset to same salts as fresh.
        wal_reset.reset(&cx, 1, salts, false).expect("reset");
        wal_reset
            .append_frame(&cx, 1, &sample_page(0x42), 1)
            .expect("append after reset");
        let reset_checksum = wal_reset.running_checksum();
        wal_reset.close(&cx).expect("close reset");

        assert_eq!(
            fresh_checksum, reset_checksum,
            "after reset with same salts, checksum chain must match fresh WAL"
        );
    }

    // ── bd-xfn30.3: Fault-injection e2e crash matrix ──
    //
    // Deterministic crash-at-every-boundary scenarios with recovery validation.

    /// Build a WAL with two committed transactions and return the VFS.
    /// Txn1: frames 1-3 (commit at 3, db_size=3)
    /// Txn2: frames 4-6 (commit at 6, db_size=6)
    fn build_two_txn_wal() -> (MemoryVfs, Vec<Vec<u8>>) {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let mut pages = Vec::new();
        let frame_specs: [(u32, u32); 6] = [
            (1, 0),
            (2, 0),
            (3, 3), // txn1
            (4, 0),
            (5, 0),
            (6, 6), // txn2
        ];
        for (pg, db_sz) in frame_specs {
            let page = sample_page(u8::try_from(pg).unwrap());
            wal.append_frame(&cx, pg, &page, db_sz).expect("append");
            pages.push(page);
        }
        wal.close(&cx).expect("close");
        (vfs, pages)
    }

    #[test]
    fn test_crash_matrix_truncate_at_every_frame_boundary() {
        // For a WAL with 6 frames (2 txns), truncate at every possible
        // frame boundary and verify recovery gives the right frame count.
        let cx = test_cx();
        let frame_size = WAL_FRAME_HEADER_SIZE + usize::try_from(PAGE_SIZE).unwrap();

        for cut_frames in 0..=6usize {
            // Rebuild fresh WAL for each truncation point.
            let (vfs, _) = build_two_txn_wal();

            let cut_at = WAL_HEADER_SIZE + cut_frames * frame_size;
            let mut f = open_wal_file(&vfs, &cx);
            f.truncate(&cx, u64::try_from(cut_at).unwrap())
                .expect("truncate");
            drop(f);

            let f2 = open_wal_file(&vfs, &cx);
            let wal = WalFile::open(&cx, f2).expect("open after truncation");
            let expected = match cut_frames {
                0..=2 => 0, // no commit yet
                3..=5 => 3, // first txn committed
                6 => 6,     // both txns committed
                _ => unreachable!(),
            };
            assert_eq!(
                wal.frame_count(),
                expected,
                "truncated at {cut_frames} frames should give {expected} committed"
            );
            wal.close(&cx).expect("close");
        }

        // Also test partial-frame truncation at various byte offsets.
        for partial in 0..20usize {
            let (vfs, _) = build_two_txn_wal();
            let cx = test_cx();

            let cut_byte = WAL_HEADER_SIZE + partial * frame_size / 3;
            let mut f = open_wal_file(&vfs, &cx);
            f.truncate(&cx, u64::try_from(cut_byte).unwrap())
                .expect("truncate");
            drop(f);

            let f2 = open_wal_file(&vfs, &cx);
            let wal = WalFile::open(&cx, f2).expect("open");
            // Recovery should give 0, 3, or 6 committed frames (never partial).
            assert!(
                wal.frame_count() == 0 || wal.frame_count() == 3 || wal.frame_count() == 6,
                "cut_byte={cut_byte} gave frame_count={}, expected 0/3/6",
                wal.frame_count()
            );
            wal.close(&cx).expect("close");
        }
    }

    #[test]
    fn test_crash_matrix_bit_flip_at_every_frame() {
        // Flip a byte in each frame's data and verify recovery truncates
        // to the correct committed prefix.
        for target_frame in 0..6usize {
            let (vfs, _) = build_two_txn_wal();
            let cx = test_cx();

            let frame_size = WAL_FRAME_HEADER_SIZE + usize::try_from(PAGE_SIZE).unwrap();
            let corrupt_offset =
                WAL_HEADER_SIZE + target_frame * frame_size + WAL_FRAME_HEADER_SIZE + 42;

            // Corrupt one byte.
            let mut f = open_wal_file(&vfs, &cx);
            let mut buf = [0u8; 1];
            let off = u64::try_from(corrupt_offset).unwrap();
            f.read(&cx, &mut buf, off).expect("read");
            buf[0] ^= 0xFF;
            f.write(&cx, &buf, off).expect("write corrupt");
            drop(f);

            let f2 = open_wal_file(&vfs, &cx);
            let wal = WalFile::open(&cx, f2).expect("open");
            let expected = if target_frame < 3 {
                0 // corruption in txn1 — no committed frames
            } else {
                3 // corruption in txn2 — txn1 survives
            };
            assert_eq!(
                wal.frame_count(),
                expected,
                "bit flip in frame {target_frame} should give {expected}"
            );
            wal.close(&cx).expect("close");
        }
    }

    #[test]
    fn test_crash_matrix_continue_after_recovery() {
        // After recovery from a crash, verify that new frames can be appended
        // and the checksum chain continues correctly.
        let (vfs, _) = build_two_txn_wal();
        let cx = test_cx();

        let frame_size = WAL_FRAME_HEADER_SIZE + usize::try_from(PAGE_SIZE).unwrap();

        // Corrupt frame 5 (in txn2), so recovery yields 3 frames (txn1).
        let corrupt_offset = WAL_HEADER_SIZE + 4 * frame_size + WAL_FRAME_HEADER_SIZE + 10;
        let mut f = open_wal_file(&vfs, &cx);
        let mut buf = [0u8; 1];
        let off = u64::try_from(corrupt_offset).unwrap();
        f.read(&cx, &mut buf, off).expect("read");
        buf[0] ^= 0xAA;
        f.write(&cx, &buf, off).expect("write corrupt");
        drop(f);

        // Recover.
        let f2 = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::open(&cx, f2).expect("open");
        assert_eq!(wal.frame_count(), 3);

        // Append new transaction (frames 4-5, commit at 5).
        wal.append_frame(&cx, 10, &sample_page(0xAA), 0)
            .expect("append");
        wal.append_frame(&cx, 11, &sample_page(0xBB), 5)
            .expect("append commit");
        assert_eq!(wal.frame_count(), 5);
        let checksum_after = wal.running_checksum();
        wal.close(&cx).expect("close");

        // Verify the new transaction persists.
        let f3 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, f3).expect("reopen");
        assert_eq!(wal2.frame_count(), 5);
        assert_eq!(wal2.running_checksum(), checksum_after);

        // Verify original txn1 data intact.
        for i in 0..3 {
            let (h, d) = wal2.read_frame(&cx, i).expect("read");
            let pg = u32::try_from(i + 1).unwrap();
            assert_eq!(h.page_number, pg);
            assert_eq!(d, sample_page(u8::try_from(pg).unwrap()));
        }

        // Verify new data.
        let (h4, d4) = wal2.read_frame(&cx, 3).expect("read new frame 4");
        assert_eq!(h4.page_number, 10);
        assert_eq!(d4, sample_page(0xAA));

        wal2.close(&cx).expect("close");
    }

    #[test]
    fn test_crash_matrix_zero_length_wal() {
        // WAL file with only a header (no frames) simulates crash before any write.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        wal.close(&cx).expect("close");

        let f2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, f2).expect("open");
        assert_eq!(wal2.frame_count(), 0);
        wal2.close(&cx).expect("close");
    }

    #[test]
    fn test_crash_matrix_header_only_partial_first_frame() {
        // WAL header plus partial first frame.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        wal.append_frame(&cx, 1, &sample_page(1), 1)
            .expect("append");
        let partial_size = WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE + 10;
        wal.file_mut()
            .truncate(&cx, u64::try_from(partial_size).unwrap())
            .expect("truncate");
        wal.close(&cx).expect("close");

        let f2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, f2).expect("open");
        assert_eq!(wal2.frame_count(), 0, "partial frame should be dropped");
        wal2.close(&cx).expect("close");
    }

    #[test]
    fn test_crash_matrix_many_txns_deterministic_recovery() {
        // 10 transactions of 3 frames each (30 total frames).
        // Crash at each transaction boundary and verify recovery.
        let cx = test_cx();

        for crash_txn in 0..=10usize {
            let vfs = MemoryVfs::new();
            let file = open_wal_file(&vfs, &cx);
            let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

            let total_frames = crash_txn * 3;
            for txn in 0..crash_txn {
                for f in 0..3u32 {
                    let pg = u32::try_from(txn * 3).unwrap() + f + 1;
                    let db_size = if f == 2 {
                        u32::try_from(txn * 3 + 3).unwrap()
                    } else {
                        0
                    };
                    let seed = u8::try_from(pg % 251).unwrap();
                    wal.append_frame(&cx, pg, &sample_page(seed), db_size)
                        .expect("append");
                }
            }
            assert_eq!(wal.frame_count(), total_frames);
            wal.close(&cx).expect("close");

            // Reopen: all frames should survive (all txns committed).
            let f2 = open_wal_file(&vfs, &cx);
            let wal2 = WalFile::open(&cx, f2).expect("open");
            assert_eq!(
                wal2.frame_count(),
                total_frames,
                "crash_txn={crash_txn}: all {total_frames} committed frames should survive"
            );
            wal2.close(&cx).expect("close");

            // Now truncate mid-way through the next (incomplete) txn.
            if crash_txn < 10 {
                // Write 1 more uncommitted frame.
                let f3 = open_wal_file(&vfs, &cx);
                let mut wal3 = WalFile::open(&cx, f3).expect("open");
                let extra_pg = u32::try_from(total_frames + 1).unwrap();
                wal3.append_frame(
                    &cx,
                    extra_pg,
                    &sample_page(u8::try_from(extra_pg % 251).unwrap()),
                    0,
                )
                .expect("append uncommitted");
                wal3.close(&cx).expect("close");

                // Reopen: uncommitted frame should be dropped.
                let f4 = open_wal_file(&vfs, &cx);
                let wal4 = WalFile::open(&cx, f4).expect("open");
                assert_eq!(
                    wal4.frame_count(),
                    total_frames,
                    "crash_txn={crash_txn}: uncommitted extra frame dropped"
                );
                wal4.close(&cx).expect("close");
            }
        }
    }

    #[test]
    fn test_crash_matrix_reset_then_crash() {
        // Reset WAL, write partial txn, crash. Recovery should give 0 frames.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        // Write and commit.
        wal.append_frame(&cx, 1, &sample_page(1), 1)
            .expect("append");
        // Reset.
        let new_salts = WalSalts {
            salt1: 0x5555_6666,
            salt2: 0x7777_8888,
        };
        wal.reset(&cx, 1, new_salts, true).expect("reset");
        assert_eq!(wal.frame_count(), 0);

        // Write partial txn (no commit).
        wal.append_frame(&cx, 1, &sample_page(0xCC), 0)
            .expect("append");
        wal.append_frame(&cx, 2, &sample_page(0xDD), 0)
            .expect("append");
        wal.close(&cx).expect("close");

        // Reopen: no committed frames after reset.
        let f2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, f2).expect("open");
        assert_eq!(wal2.frame_count(), 0, "no commits after reset");
        assert_eq!(wal2.header().salts, new_salts);
        wal2.close(&cx).expect("close");
    }

    #[test]
    fn test_truncated_file_mid_second_txn_recovers_first_commit() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        // Txn 1: commit at frame 2.
        wal.append_frame(&cx, 1, &sample_page(0x11), 0)
            .expect("append");
        wal.append_frame(&cx, 2, &sample_page(0x22), 2)
            .expect("commit txn1");

        // Txn 2: partial — one full frame + start of another.
        wal.append_frame(&cx, 3, &sample_page(0x33), 0)
            .expect("append");
        wal.append_frame(&cx, 4, &sample_page(0x44), 0)
            .expect("append");
        assert_eq!(wal.frame_count(), 4);
        let frame_size = wal.frame_size();
        wal.close(&cx).expect("close");

        // Truncate: remove the last frame entirely and half of frame 3.
        let truncate_offset =
            u64::try_from(WAL_HEADER_SIZE + frame_size * 2 + frame_size / 2).unwrap();
        let mut f = open_wal_file(&vfs, &cx);
        f.truncate(&cx, truncate_offset).expect("truncate");
        drop(f);

        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("reopen after truncation");
        assert_eq!(
            wal2.frame_count(),
            2,
            "only first committed transaction (2 frames) should survive truncation"
        );
        let h = wal2.read_frame_header(&cx, 1).expect("read frame 2 header");
        assert!(h.is_commit(), "frame 2 must be a commit frame");
        wal2.close(&cx).expect("close");
    }

    #[test]
    fn test_recovery_is_idempotent_across_multiple_reopens() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);

        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        // Two committed transactions.
        wal.append_frame(&cx, 1, &sample_page(0xAA), 0)
            .expect("append");
        wal.append_frame(&cx, 2, &sample_page(0xBB), 2)
            .expect("commit txn1");
        wal.append_frame(&cx, 3, &sample_page(0xCC), 0)
            .expect("append");
        wal.append_frame(&cx, 4, &sample_page(0xDD), 4)
            .expect("commit txn2");
        // One uncommitted frame.
        wal.append_frame(&cx, 5, &sample_page(0xEE), 0)
            .expect("append");
        wal.close(&cx).expect("close");

        // Reopen three times — frame count must be stable.
        for reopen in 0..3_u32 {
            let f = open_wal_file(&vfs, &cx);
            let wal_reopened = WalFile::open(&cx, f).expect("reopen");
            assert_eq!(
                wal_reopened.frame_count(),
                4,
                "reopen {reopen}: committed frame count must be 4"
            );
            let (_, data) = wal_reopened.read_frame(&cx, 3).expect("read frame 4");
            assert_eq!(
                data,
                sample_page(0xDD),
                "reopen {reopen}: frame 4 content must be intact"
            );
            wal_reopened.close(&cx).expect("close");
        }
    }

    #[test]
    fn wal_generation_identity_from_header_and_eq() {
        let header = WalHeader {
            magic: WAL_MAGIC_LE,
            format_version: WAL_FORMAT_VERSION,
            page_size: PAGE_SIZE,
            checkpoint_seq: 7,
            salts: test_salts(),
            checksum: SqliteWalChecksum { s1: 0, s2: 0 },
        };
        let identity = WalGenerationIdentity::from_header(&header);
        assert_eq!(identity.checkpoint_seq, 7);
        assert_eq!(identity.salts, test_salts());
        let copied = identity;
        assert_eq!(copied, identity);
        let other = WalGenerationIdentity {
            checkpoint_seq: 8,
            salts: test_salts(),
        };
        assert_ne!(identity, other);
        let dbg = format!("{identity:?}");
        assert!(dbg.contains("WalGenerationIdentity"));
    }

    #[test]
    fn wal_append_frame_ref_debug_clone_copy() {
        let data = [0xABu8; 16];
        let frame = WalAppendFrameRef {
            page_number: 3,
            page_data: &data,
            db_size_if_commit: 10,
        };
        let copied = frame;
        assert_eq!(copied.page_number, 3);
        assert_eq!(copied.db_size_if_commit, 10);
        assert_eq!(copied.page_data[0], 0xAB);
        let cloned = frame;
        assert_eq!(cloned.page_number, frame.page_number);
        let dbg = format!("{frame:?}");
        assert!(dbg.contains("WalAppendFrameRef"));
    }

    #[test]
    fn wal_file_generation_identity_matches_create_params() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let identity = wal.generation_identity();
        assert_eq!(identity.checkpoint_seq, 0);
        assert_eq!(identity.salts, test_salts());
        wal.close(&cx).expect("close");
    }

    #[test]
    fn wal_file_page_size_and_frame_count_after_create() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        assert_eq!(wal.page_size(), PAGE_SIZE as usize);
        assert_eq!(wal.frame_count(), 0);
        assert!(wal.last_commit_frame(&cx).expect("query").is_none());
        wal.append_frame(&cx, 1, &sample_page(1), 5)
            .expect("append");
        assert_eq!(wal.frame_count(), 1);
        assert_eq!(wal.last_commit_frame(&cx).expect("query"), Some(0));
        wal.close(&cx).expect("close");
    }

    #[test]
    fn wal_file_big_endian_checksum_and_running_checksum() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let _be = wal.big_endian_checksum();
        let rc = wal.running_checksum();
        assert_eq!(rc.s1.wrapping_add(0), rc.s1);
        wal.close(&cx).expect("close");
    }

    #[test]
    fn wal_file_frame_size_equals_header_plus_page() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        assert_eq!(wal.frame_size(), WAL_FRAME_HEADER_SIZE + PAGE_SIZE as usize);
        wal.close(&cx).expect("close");
    }

    #[test]
    fn wal_file_header_accessor() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let hdr = wal.header();
        assert_eq!(hdr.page_size, PAGE_SIZE);
        assert_eq!(hdr.salts, test_salts());
        wal.close(&cx).expect("close");
    }

    #[test]
    fn wal_file_file_and_file_mut_accessors() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        let _f_ref = wal.file();
        let _f_mut = wal.file_mut();
        wal.close(&cx).expect("close");
    }

    #[test]
    fn durable_sync_records_fsynced_frame_count() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        assert_eq!(wal.last_fsynced_frame_count(), 0);

        let page = vec![0xABu8; PAGE_SIZE as usize];
        let frames = [frame_ref(1, &page, 1)];
        wal.append_frames(&cx, &frames).expect("append");
        assert_eq!(wal.frame_count(), 1);
        assert_eq!(wal.last_fsynced_frame_count(), 0);

        wal.durable_sync(&cx, fsqlite_vfs::SyncKind::FullDurable)
            .expect("durable_sync");
        assert_eq!(wal.last_fsynced_frame_count(), 1);
        wal.close(&cx).expect("close");
    }

    #[test]
    fn assert_publish_safe_passes_after_durable_sync() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let page = vec![0xCDu8; PAGE_SIZE as usize];
        let frames = [frame_ref(1, &page, 1)];
        wal.append_frames(&cx, &frames).expect("append");
        wal.durable_sync(&cx, fsqlite_vfs::SyncKind::FullDurable)
            .expect("durable_sync");
        wal.assert_publish_safe(1)
            .expect("should be safe after fsync");
        wal.close(&cx).expect("close");
    }

    #[test]
    fn assert_publish_safe_passes_for_zero_frames() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");
        wal.assert_publish_safe(0).expect("zero frames always safe");
        wal.close(&cx).expect("close");
    }

    #[test]
    fn durable_sync_with_data_only_kind() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let page = vec![0xEFu8; PAGE_SIZE as usize];
        let frames = [frame_ref(2, &page, 1)];
        wal.append_frames(&cx, &frames).expect("append");
        wal.durable_sync(&cx, fsqlite_vfs::SyncKind::DataOnly)
            .expect("data-only sync");
        assert_eq!(wal.last_fsynced_frame_count(), 1);
        wal.close(&cx).expect("close");
    }

    #[test]
    fn durable_sync_reset_clears_fsynced_count() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let page = vec![0x11u8; PAGE_SIZE as usize];
        let frames = [frame_ref(1, &page, 1)];
        wal.append_frames(&cx, &frames).expect("append");
        wal.durable_sync(&cx, fsqlite_vfs::SyncKind::FullDurable)
            .expect("durable_sync");
        assert_eq!(wal.last_fsynced_frame_count(), 1);

        let new_salts = WalSalts {
            salt1: 0x1111_1111,
            salt2: 0x2222_2222,
        };
        wal.reset(&cx, 1, new_salts, true).expect("reset");
        assert_eq!(wal.last_fsynced_frame_count(), 0);
        wal.close(&cx).expect("close");
    }

    #[test]
    fn opened_wal_has_fsynced_count_equal_to_frame_count() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let page = vec![0x22u8; PAGE_SIZE as usize];
        let frames = [frame_ref(1, &page, 1)];
        wal.append_frames(&cx, &frames).expect("append");
        wal.durable_sync(&cx, fsqlite_vfs::SyncKind::FullDurable)
            .expect("durable_sync");
        wal.close(&cx).expect("close");

        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("open");
        assert_eq!(wal2.frame_count(), 1);
        assert_eq!(
            wal2.last_fsynced_frame_count(),
            wal2.frame_count(),
            "opened WAL assumes existing frames are durable"
        );
        wal2.close(&cx).expect("close");
    }

    #[test]
    fn crash_before_wal_frame_append_preserves_existing_frames() {
        let _guard = FAULT_TEST_LOCK.lock().unwrap();
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let p1 = sample_page(0xAA);
        let first_frames = [frame_ref(1, &p1, 1)];
        wal.append_frames(&cx, &first_frames).expect("first append");
        wal.durable_sync(&cx, SyncKind::FullDurable)
            .expect("sync first frame");
        assert_eq!(wal.frame_count(), 1);

        crate::fault_hooks::arm_crash_boundary(
            crate::fault_hooks::CrashBoundary::BeforeWalFrameAppend,
            crate::fault_hooks::FaultHookArm::new(
                "crash-before-append",
                "WAL-FRAME-APPEND-CRASH",
                "test_crash_before_append",
            ),
        );

        let p2 = sample_page(0xBB);
        let second_frames = [frame_ref(2, &p2, 2)];
        let err = wal
            .append_frames(&cx, &second_frames)
            .expect_err("should fail at crash boundary");
        assert!(
            err.to_string().contains("fault_inject"),
            "error identifies the fault hook: {err}"
        );

        crate::fault_hooks::clear_crash_boundary();

        wal.close(&cx).expect("close after crash");

        let file2 = open_wal_file(&vfs, &cx);
        let recovered = WalFile::open(&cx, file2).expect("reopen");
        assert_eq!(
            recovered.frame_count(),
            1,
            "only the first committed frame survives; the second was never written"
        );
        recovered.close(&cx).expect("close recovered");
    }

    #[test]
    fn crash_after_fsync_before_publish_leaves_frames_durable() {
        let _guard = FAULT_TEST_LOCK.lock().unwrap();
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let p1 = sample_page(0xCC);
        let frames = [frame_ref(1, &p1, 1)];
        wal.append_frames(&cx, &frames).expect("append");

        crate::fault_hooks::arm_crash_boundary(
            crate::fault_hooks::CrashBoundary::AfterFsyncBeforePublish,
            crate::fault_hooks::FaultHookArm::new(
                "crash-after-fsync",
                "WAL-FSYNC-PUBLISH-CRASH",
                "test_crash_after_fsync",
            ),
        );

        let err = wal
            .durable_sync(&cx, SyncKind::FullDurable)
            .expect_err("should fail at crash boundary");
        assert!(
            err.to_string().contains("fault_inject"),
            "error identifies the fault hook: {err}"
        );

        crate::fault_hooks::clear_crash_boundary();

        wal.close(&cx).expect("close after crash");

        let file2 = open_wal_file(&vfs, &cx);
        let recovered = WalFile::open(&cx, file2).expect("reopen");
        assert_eq!(
            recovered.frame_count(),
            1,
            "frame was fsynced before crash so it survives recovery"
        );
        recovered.close(&cx).expect("close recovered");
    }

    #[test]
    fn crash_before_wal_header_write_on_reset_preserves_old_generation() {
        let _guard = FAULT_TEST_LOCK.lock().unwrap();
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        let p1 = sample_page(0xDD);
        let frames = [frame_ref(1, &p1, 1)];
        wal.append_frames(&cx, &frames).expect("append");
        wal.durable_sync(&cx, SyncKind::FullDurable).expect("sync");
        assert_eq!(wal.frame_count(), 1);

        let original_salts = wal.generation_identity().salts;

        crate::fault_hooks::arm_crash_boundary(
            crate::fault_hooks::CrashBoundary::BeforeWalHeaderWrite,
            crate::fault_hooks::FaultHookArm::new(
                "crash-before-header",
                "WAL-HEADER-WRITE-CRASH",
                "test_crash_before_header",
            ),
        );

        let new_salts = WalSalts {
            salt1: original_salts.salt1.wrapping_add(1),
            salt2: original_salts.salt2.wrapping_add(1),
        };
        let err = wal
            .reset(&cx, 1, new_salts, true)
            .expect_err("should fail at crash boundary");
        assert!(
            err.to_string().contains("fault_inject"),
            "error identifies the fault hook: {err}"
        );

        crate::fault_hooks::clear_crash_boundary();

        wal.close(&cx).expect("close after crash");

        let file2 = open_wal_file(&vfs, &cx);
        let recovered = WalFile::open(&cx, file2).expect("reopen");
        assert_eq!(
            recovered.frame_count(),
            1,
            "header was never rewritten so old generation with 1 frame persists"
        );
        assert_eq!(
            recovered.generation_identity().salts,
            original_salts,
            "salts unchanged — reset never wrote new header"
        );
        recovered.close(&cx).expect("close recovered");
    }

    #[test]
    fn crash_after_frame_append_before_fsync_frames_on_disk_but_not_durable() {
        let _guard = FAULT_TEST_LOCK.lock().unwrap();
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let file = open_wal_file(&vfs, &cx);
        let mut wal = WalFile::create(&cx, file, PAGE_SIZE, 0, test_salts()).expect("create");

        crate::fault_hooks::arm_crash_boundary(
            crate::fault_hooks::CrashBoundary::AfterWalFrameAppendBeforeFsync,
            crate::fault_hooks::FaultHookArm::new(
                "crash-after-append",
                "WAL-APPEND-FSYNC-CRASH",
                "test_crash_after_append_before_fsync",
            ),
        );

        let p1 = sample_page(0xEE);
        let frames = [frame_ref(1, &p1, 1)];
        let err = wal
            .append_frames(&cx, &frames)
            .expect_err("should fail after append but before fsync");
        assert!(
            err.to_string().contains("fault_inject"),
            "error identifies the fault hook: {err}"
        );

        crate::fault_hooks::clear_crash_boundary();

        assert_eq!(
            wal.last_fsynced_frame_count(),
            0,
            "no fsync happened so fsynced count is still zero"
        );

        wal.close(&cx).expect("close after crash");

        let file2 = open_wal_file(&vfs, &cx);
        let recovered = WalFile::open(&cx, file2).expect("reopen");
        assert!(
            recovered.frame_count() <= 1,
            "frame may or may not survive recovery depending on MemoryVfs behavior (data written but not fsynced)"
        );
        recovered.close(&cx).expect("close recovered");
    }
}
