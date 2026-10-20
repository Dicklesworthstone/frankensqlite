//! Read-only admission of the physical WAL snapshot used by a checkpoint.
//!
//! `CheckpointState` and `WalFile::frame_count` can BOTH be stale when another
//! handle has appended. A same-generation committed suffix must not be lost to
//! a reset based on that shared stale count (the cross-handle case of bd-km8qs).
//! Old-generation bytes and incomplete transactions beyond the accepted prefix
//! are normal WAL recovery residue, however, so file length alone is not proof
//! of a newer commit. Scan only that suffix with the accepted checksum seed.

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_vfs::VfsFile;

use crate::checkpoint::{CheckpointMode, CheckpointState, plan_checkpoint};
use crate::checksum::{
    SqliteWalChecksum, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalFrameHeader, WalHeader,
    compute_wal_frame_checksum, wal_header_checksum,
};
use crate::wal::WalFile;

fn stale(detail: impl Into<String>) -> FrankenError {
    FrankenError::CheckpointFailed {
        detail: detail.into(),
    }
}

fn corrupt(detail: impl Into<String>) -> FrankenError {
    FrankenError::WalCorrupt {
        detail: detail.into(),
    }
}

/// `true` means an accepted, still physically empty headerless WAL. No target
/// callbacks are allowed in that case. This never refreshes or mutates the
/// supplied handle: its backfill receipt belongs to the original snapshot.
pub(super) async fn validate_source<F: VfsFile>(
    cx: &Cx,
    wal: &WalFile<F>,
    mode: CheckpointMode,
    state: CheckpointState,
) -> Result<bool> {
    let count = wal.frame_count();
    let total = u32::try_from(count)
        .map_err(|_| stale("WAL frame count exceeds the checkpoint counter domain"))?;
    if state.total_frames != total || state.backfilled_frames > total {
        return Err(stale(
            "checkpoint counters do not describe the accepted WAL snapshot",
        ));
    }
    if count > 0
        && wal
            .last_commit_frame_header()
            .is_none_or(|(index, _)| index != count - 1)
    {
        return Err(stale("checkpoint refused an uncommitted WAL suffix"));
    }
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
    let file_size = wal.file().file_size(cx)?;
    if wal.is_headerless() {
        if count == 0 && file_size == 0 {
            return Ok(true);
        }
        // A short nonempty file is not an empty WAL, and a newly installed
        // header is not this handle's accepted generation. The owner must
        // reconcile it without blessing its old backfill counters.
        return Err(FrankenError::BusyRecovery);
    }
    let header_seed = validate_header(cx, wal).await?;
    let accepted_end = wal.frame_offset(count);
    if file_size < accepted_end {
        return Err(corrupt("WAL shrank below the accepted checkpoint prefix"));
    }

    let frame_size = wal.frame_size();
    let mut frame = vec![0_u8; frame_size];
    if count == 0 {
        if wal.running_checksum() != header_seed {
            return Err(corrupt(
                "empty checkpoint WAL has a different checksum seed",
            ));
        }
    } else {
        validate_terminal_frame(cx, wal, header_seed, &mut frame).await?;
    }

    validate_frontiers(cx, wal, header_seed, mode, state, &mut frame).await?;

    let frame_size_u64 = u64::try_from(frame_size).map_err(|_| FrankenError::DatabaseFull)?;
    let extra = usize::try_from((file_size - accepted_end) / frame_size_u64)
        .map_err(|_| FrankenError::DatabaseFull)?;
    let end = count.checked_add(extra).ok_or(FrankenError::DatabaseFull)?;
    let mut previous = wal.running_checksum();
    for index in count..end {
        cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
        let read = wal
            .file()
            .read(cx, &mut frame, wal.frame_offset(index))
            .await?;
        if read != frame_size {
            return Err(corrupt("WAL changed during checkpoint suffix validation"));
        }
        let header = WalFrameHeader::from_bytes(&frame[..WAL_FRAME_HEADER_SIZE])?;
        if header.page_number == 0 || header.salts != wal.header().salts {
            break;
        }
        let checksum = compute_wal_frame_checksum(
            &frame,
            wal.page_size(),
            previous,
            wal.big_endian_checksum(),
        )?;
        if header.checksum != checksum {
            break;
        }
        if header.is_commit() {
            return Err(stale(format!(
                "checkpoint WAL has a newer committed frame at {}; refresh and replan",
                index + 1
            )));
        }
        previous = checksum;
    }
    // A partial frame, invalid chain, or valid noncommitting suffix is not a
    // newer committed snapshot. Recovery likewise stops at the last commit.
    // The caller's append gate is still required, including while resetting.
    Ok(false)
}

async fn validate_header<F: VfsFile>(cx: &Cx, wal: &WalFile<F>) -> Result<SqliteWalChecksum> {
    for _ in 0..2 {
        let mut bytes = [0_u8; WAL_HEADER_SIZE];
        let read = wal.file().read(cx, &mut bytes, 0).await?;
        if read != WAL_HEADER_SIZE {
            return Err(corrupt("short checkpoint WAL header"));
        }
        let header = WalHeader::from_bytes(&bytes)?;
        let checksum = wal_header_checksum(&bytes, header.big_endian_checksum())?;
        if header.checksum != checksum {
            continue;
        }
        if &header != wal.header() {
            return Err(corrupt(
                "WAL generation changed before checkpoint admission",
            ));
        }
        return Ok(checksum);
    }
    Err(corrupt(
        "WAL header checksum mismatch during checkpoint admission",
    ))
}

/// Bind zero-backfill checkpoints to the accepted terminal commit too. This
/// is not a rescan or a certificate for the already-backfilled database pages.
async fn validate_terminal_frame<F: VfsFile>(
    cx: &Cx,
    wal: &WalFile<F>,
    header_seed: SqliteWalChecksum,
    frame: &mut [u8],
) -> Result<()> {
    let index = wal.frame_count() - 1;
    let (_, expected) = wal
        .last_commit_frame_header()
        .ok_or_else(|| stale("checkpoint has no accepted terminal commit"))?;
    let previous = if index == 0 {
        header_seed
    } else {
        let predecessor = wal.read_frame_header(cx, index - 1).await?;
        if predecessor.page_number == 0 || predecessor.salts != wal.header().salts {
            return Err(corrupt("invalid checkpoint terminal checksum seed"));
        }
        predecessor.checksum
    };
    let read = wal.file().read(cx, frame, wal.frame_offset(index)).await?;
    if read != frame.len() {
        return Err(corrupt("short checkpoint terminal frame"));
    }
    let header = WalFrameHeader::from_bytes(&frame[..WAL_FRAME_HEADER_SIZE])?;
    let checksum =
        compute_wal_frame_checksum(frame, wal.page_size(), previous, wal.big_endian_checksum())?;
    if header.page_number == 0
        || !header.is_commit()
        || header.page_number != expected.page_number
        || header.db_size != expected.db_size
        || header.salts != expected.salts
        || header.checksum != expected.checksum
        || checksum != wal.running_checksum()
    {
        return Err(corrupt(
            "checkpoint terminal frame differs from the accepted commit",
        ));
    }
    Ok(())
}

/// Backfill receipts and reader horizons must describe committed snapshots,
/// not arbitrary frame offsets inside a multi-frame transaction. Validate both
/// the old receipt and the actual planned end before copying even one page.
async fn validate_frontiers<F: VfsFile>(
    cx: &Cx,
    wal: &WalFile<F>,
    header_seed: SqliteWalChecksum,
    mode: CheckpointMode,
    state: CheckpointState,
    frame: &mut [u8],
) -> Result<()> {
    let end = state
        .backfilled_frames
        .checked_add(plan_checkpoint(mode, state).frames_to_backfill)
        .ok_or_else(|| stale("checkpoint backfill endpoint overflow"))?;
    validate_commit_boundary(cx, wal, header_seed, state.backfilled_frames, frame).await?;
    if end != state.backfilled_frames {
        validate_commit_boundary(cx, wal, header_seed, end, frame).await?;
    }
    Ok(())
}

async fn validate_commit_boundary<F: VfsFile>(
    cx: &Cx,
    wal: &WalFile<F>,
    header_seed: SqliteWalChecksum,
    boundary: u32,
    frame: &mut [u8],
) -> Result<()> {
    let count = usize::try_from(boundary).map_err(|_| FrankenError::DatabaseFull)?;
    // The empty snapshot is valid; the terminal commit was already checked.
    if count == 0 || count == wal.frame_count() {
        return Ok(());
    }
    if count > wal.frame_count() {
        return Err(stale("checkpoint boundary exceeds the accepted WAL"));
    }
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)?;
    let index = count - 1;
    let previous = if index == 0 {
        header_seed
    } else {
        let predecessor = wal.read_frame_header(cx, index - 1).await?;
        if predecessor.page_number == 0 || predecessor.salts != wal.header().salts {
            return Err(corrupt("invalid checkpoint boundary checksum seed"));
        }
        predecessor.checksum
    };
    let read = wal.file().read(cx, frame, wal.frame_offset(index)).await?;
    if read != frame.len() {
        return Err(corrupt("short checkpoint boundary frame"));
    }
    let header = WalFrameHeader::from_bytes(&frame[..WAL_FRAME_HEADER_SIZE])?;
    let checksum =
        compute_wal_frame_checksum(frame, wal.page_size(), previous, wal.big_endian_checksum())?;
    if header.page_number == 0 || header.salts != wal.header().salts || header.checksum != checksum
    {
        return Err(corrupt("invalid checkpoint boundary frame"));
    }
    if !header.is_commit() {
        return Err(stale(format!(
            "checkpoint boundary {boundary} falls inside a WAL transaction"
        )));
    }
    Ok(())
}
