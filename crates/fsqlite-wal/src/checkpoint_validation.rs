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
    let live_frame_count =
        u32::try_from(wal.frame_count()).map_err(|_| FrankenError::CheckpointFailed {
            detail: "WAL frame count exceeds the checkpoint counter domain".to_owned(),
        })?;
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

    // A physically appended suffix is not a committed checkpoint image.
    // Refuse it rather than copying it into the database or discarding it on
    // reset. The caller must settle/refresh the append owner before retrying.
    let ends_at_commit = wal.frame_count() == 0
        || wal
            .last_commit_frame_header()
            .is_some_and(|(index, _)| index == wal.frame_count() - 1);
    if !ends_at_commit || state.backfilled_frames > state.total_frames {
        return Err(FrankenError::CheckpointFailed {
            detail: if ends_at_commit {
                "checkpoint backfill prefix exceeds the live WAL".to_owned()
            } else {
                "checkpoint refused an uncommitted WAL suffix".to_owned()
            },
        });
    }
    Ok(())
}

/// Verify the physical generation against the handle's accepted header.
/// A checksum-torn header receives one retry, as in the WAL replay path.
pub(super) async fn checkpoint_header_checksum<F: VfsFile>(
    wal: &WalFile<F>,
    cx: &Cx,
) -> Result<SqliteWalChecksum> {
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

/// One validated frame of a checkpoint window: its header, and the digest of
/// its bytes unless a later frame of the window carries the same page.
pub(super) type CheckpointSourceFrame = (WalFrameHeader, Option<Xxh3Checksum128>);

/// Validate every frame in the new backfill window before deduplication and
/// retain the digest the later copy pass checks. The predecessor of `start` is
/// an already-backfilled boundary supplied by the checkpoint owner; only its
/// header supplies the rolling seed, avoiding a repeated scan from frame zero.
///
/// bd-fuwx0 (3b-i): the copy pass writes only the last frame of each page in
/// the window, so only that frame's digest is ever read. A frame that a later
/// frame of the same page supersedes is validated as before but gets no
/// digest. Which frames are superseded is decided from the page numbers this
/// handle recorded for the window, and the pass checks each frame's on-disk
/// page number against its recorded one. If the handle holds no header for
/// some frame of the window, or any of them differs, the pass runs again with
/// a digest for every frame. Either way the digests handed to the copy pass
/// are those of the bytes this pass validated.
pub(super) async fn read_checkpoint_frame_headers<F: VfsFile>(
    wal: &WalFile<F>,
    cx: &Cx,
    start: usize,
    end: usize,
) -> Result<Vec<CheckpointSourceFrame>> {
    if start > end || end > wal.frame_count() {
        return Err(FrankenError::WalCorrupt {
            detail: format!("invalid checkpoint frame range {start}..{end}"),
        });
    }
    let header_checksum = checkpoint_header_checksum(wal, cx).await?;
    if start == end {
        return Ok(Vec::new());
    }
    if let Some(superseded) = recorded_superseded_frames(wal, start, end)
        && let Some(frames) =
            validate_checkpoint_frames(wal, cx, start, end, header_checksum, Some(&superseded))
                .await?
    {
        return Ok(frames);
    }
    validate_checkpoint_frames(wal, cx, start, end, header_checksum, None)
        .await?
        .ok_or_else(|| {
            FrankenError::internal("a checkpoint pass that digests every frame stopped early")
        })
}

/// bd-fuwx0 (3b-i): for each frame of `start..end`, whether a later frame of
/// the window carries the same page, by the page numbers this handle recorded;
/// `None` when it holds no header for some frame of the window.
fn recorded_superseded_frames<F: VfsFile>(
    wal: &WalFile<F>,
    start: usize,
    end: usize,
) -> Option<Vec<bool>> {
    let mut later_pages = std::collections::HashSet::new();
    let mut superseded = vec![false; end - start];
    for frame in (start..end).rev() {
        let page = wal.recorded_frame_page_number(frame)?;
        superseded[frame - start] = !later_pages.insert(page);
    }
    Some(superseded)
}

/// One validating pass over `start..end`, the body of
/// [`read_checkpoint_frame_headers`]. With `superseded`, a frame it marks gets
/// no digest, and the pass returns `Ok(None)` at the first valid frame whose
/// on-disk page number differs from the one this handle recorded for it.
async fn validate_checkpoint_frames<F: VfsFile>(
    wal: &WalFile<F>,
    cx: &Cx,
    start: usize,
    end: usize,
    header_checksum: SqliteWalChecksum,
    superseded: Option<&[bool]>,
) -> Result<Option<Vec<CheckpointSourceFrame>>> {
    const MAX_READ_BYTES: usize = 64 * 1024;
    let mut previous = if start == 0 {
        header_checksum
    } else {
        let predecessor = wal.read_frame_header_on_disk(cx, start - 1).await?;
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
            let index = frame + offset;
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
                    detail: format!("invalid WAL source at checkpoint frame {index}"),
                });
            }
            previous = checksum;
            let digest = match superseded {
                None => Some(Xxh3Checksum128::compute(bytes)),
                Some(superseded) => {
                    if wal.recorded_frame_page_number(index) != Some(header.page_number) {
                        return Ok(None);
                    }
                    (!superseded[index - start]).then(|| Xxh3Checksum128::compute(bytes))
                }
            };
            headers.push((header, digest));
        }
        frame += count;
    }
    if end == wal.frame_count() && previous != wal.running_checksum() {
        return Err(FrankenError::WalCorrupt {
            detail: "checkpoint checksum tail differs from the accepted WAL tail".to_owned(),
        });
    }
    Ok(Some(headers))
}
