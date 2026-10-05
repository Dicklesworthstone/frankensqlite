//! Checkpoint admission and source validation before database mutation.
//!
//! The checkpoint owner must hold its generation/append coordination gate
//! throughout validation, copying, and publication. These checks do not
//! replace that gate or certify already-backfilled database pages.

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_vfs::VfsFile;

use super::CheckpointState;
use crate::checksum::{
    SqliteWalChecksum, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalFrameHeader, WalHeader,
    Xxh3Checksum128, compute_wal_frame_checksum, wal_header_checksum,
};
use crate::wal::WalFile;

pub(super) fn validate_checkpoint_state<F: VfsFile>(
    wal: &WalFile<F>,
    state: CheckpointState,
) -> Result<()> {
    // bd-km8qs: do not turn a stale plan into permission to discard a newer tail.
    let live_frame_count = u32::try_from(wal.frame_count()).unwrap_or(u32::MAX);
    if state.total_frames != live_frame_count {
        return Err(FrankenError::CheckpointFailed {
            detail: format!(
                "checkpoint state is stale: total_frames={} but the live WAL has \
                 {live_frame_count} frames — a coordination guard was violated between \
                 planning and execution",
                state.total_frames
            ),
        });
    }
    Ok(())
}

/// Verify the physical generation against the handle's accepted header.
/// A checksum-torn header receives one retry, as in the WAL replay path.
async fn checkpoint_header_checksum<F: VfsFile>(wal: &WalFile<F>, cx: &Cx) -> Result<SqliteWalChecksum> {
    for _ in 0..2 {
        let mut bytes = [0_u8; WAL_HEADER_SIZE];
        let read = wal.file().read(cx, &mut bytes, 0).await?;
        if read != WAL_HEADER_SIZE {
            return Err(FrankenError::WalCorrupt {
                detail: format!("short checkpoint WAL header: got {read}, need {WAL_HEADER_SIZE}"),
            });
        }
        let header = WalHeader::from_bytes(&bytes)?;
        let checksum = wal_header_checksum(&bytes, header.big_endian_checksum())?;
        if header.checksum != checksum {
            continue;
        }
        if &header != wal.header() {
            return Err(FrankenError::WalCorrupt {
                detail: "WAL generation changed before checkpoint validation".to_owned(),
            });
        }
        return Ok(checksum);
    }
    Err(FrankenError::WalCorrupt {
        detail: "WAL header checksum mismatch during checkpoint".to_owned(),
    })
}

/// Validate every frame in the new backfill window before deduplication and
/// retain its digest for the later copy pass. The predecessor of `start` is an
/// already-backfilled boundary supplied by the checkpoint owner; only its
/// header supplies the rolling seed, avoiding a repeated scan from frame zero.
pub(super) async fn read_checkpoint_frame_headers<F: VfsFile>(
    wal: &WalFile<F>,
    cx: &Cx,
    start: usize,
    end: usize,
) -> Result<Vec<(WalFrameHeader, Xxh3Checksum128)>> {
    const MAX_READ_BYTES: usize = 64 * 1024;
    if start > end || end > wal.frame_count() {
        return Err(FrankenError::WalCorrupt {
            detail: format!("invalid checkpoint frame range {start}..{end}"),
        });
    }
    let header_checksum = checkpoint_header_checksum(wal, cx).await?;
    if start == end {
        return Ok(Vec::new());
    }

    let mut previous = if start == 0 {
        header_checksum
    } else {
        let predecessor = wal.read_frame_header(cx, start - 1).await?;
        if predecessor.salts != wal.header().salts || predecessor.page_number == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "checkpoint checksum seed belongs to an invalid frame".to_owned(),
            });
        }
        predecessor.checksum
    };
    let frame_size = wal.frame_size();
    let frames_per_read = (MAX_READ_BYTES / frame_size).max(1);
    let mut buffer = vec![0_u8; frame_size * frames_per_read.min(end - start)];
    let mut headers = Vec::with_capacity(end - start);
    let mut frame = start;
    while frame < end {
        let count = frames_per_read.min(end - frame);
        // Include the last payload as well, unlike the header-only scan.
        let wanted = count * frame_size;
        let bytes = &mut buffer[..wanted];
        let read = wal.file().read(cx, bytes, wal.frame_offset(frame)).await?;
        if read != wanted {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "short checkpoint read at frame {frame}: got {read}, need {wanted}"
                ),
            });
        }
        for (offset, bytes) in bytes.chunks_exact(frame_size).enumerate() {
            let header = WalFrameHeader::from_bytes(&bytes[..WAL_FRAME_HEADER_SIZE])?;
            let checksum = compute_wal_frame_checksum(
                bytes,
                wal.page_size(),
                previous,
                wal.header().big_endian_checksum(),
            )?;
            if header.page_number == 0
                || header.salts != wal.header().salts
                || header.checksum != checksum
            {
                return Err(FrankenError::WalCorrupt {
                    detail: format!("invalid WAL source at checkpoint frame {}", frame + offset),
                });
            }
            previous = checksum;
            headers.push((header, Xxh3Checksum128::compute(bytes)));
        }
        frame += count;
    }
    if end == wal.frame_count() && previous != wal.running_checksum() {
        return Err(FrankenError::WalCorrupt {
            detail: "checkpoint checksum tail differs from the accepted WAL tail".to_owned(),
        });
    }
    Ok(headers)
}
