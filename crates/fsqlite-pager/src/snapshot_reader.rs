//! Pages of one committed WAL snapshot, readable from any thread
//! (bd-w4pr1.13.11).
//!
//! A read transaction pins a WAL snapshot at BEGIN: the last commit frame it
//! sees and, for each page the WAL holds, the latest frame at or below that
//! commit. Every other page comes from the database file, which no checkpoint
//! may overwrite while the transaction's reader mark is published. A
//! [`SnapshotPageReader`] captures those facts once, as shared handles, so
//! threads outside the connection can read pages of exactly that snapshot with
//! positional reads into a buffer of their own: no transaction state, no lock,
//! no allocation per page.
//!
//! The reader is only as long-lived as the transaction that offered it: the
//! reader mark that keeps the database file and the WAL stable is released
//! when that transaction ends, so every thread using a reader must be joined
//! before the transaction commits or rolls back.

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::PageNumber;

use crate::traits::WalPublicationSnapshot;

/// Bytes of the WAL file header that precede frame 0.
const WAL_HEADER_BYTES: u64 = 32;
/// Bytes of the header that precede a page image in a WAL frame.
const WAL_FRAME_HEADER_BYTES: usize = 24;

/// The WAL frames one pinned read snapshot sees.
#[derive(Debug, Clone)]
pub struct WalSnapshotFrames {
    /// The snapshot these frames belong to.
    pub publication: WalPublicationSnapshot,
    /// The WAL file's descriptor.
    pub file: Arc<File>,
    /// For each page the snapshot finds in the WAL, its latest frame
    /// (0-based) at or below the snapshot's last commit frame.
    pub page_index: Arc<HashMap<u32, usize>>,
}

/// Pages of one committed snapshot of a WAL-mode database. Cloning shares it.
#[derive(Debug, Clone)]
pub struct SnapshotPageReader {
    inner: Arc<SnapshotPages>,
}

#[derive(Debug)]
struct SnapshotPages {
    db_file: Arc<File>,
    /// `None` when the snapshot sees no committed WAL frame.
    wal: Option<WalSnapshotFrames>,
    page_size: usize,
    db_size: u32,
}

/// One thread's buffer for [`SnapshotPageReader::read_page`]: room for one
/// WAL frame, reused for every page that thread reads.
#[derive(Debug)]
pub struct SnapshotPageBuf {
    bytes: Vec<u8>,
}

impl SnapshotPageReader {
    /// A reader of the snapshot whose WAL frames are `wal` (`None`: no
    /// committed frame is visible) and whose database holds `db_size` pages
    /// of `page_size` bytes in `db_file`.
    #[must_use]
    pub fn new(
        db_file: Arc<File>,
        wal: Option<WalSnapshotFrames>,
        page_size: usize,
        db_size: u32,
    ) -> Self {
        let wal = wal.filter(|frames| frames.publication.last_commit_frame.is_some());
        Self {
            inner: Arc::new(SnapshotPages {
                db_file,
                wal,
                page_size,
                db_size,
            }),
        }
    }

    /// Page size of the database in bytes.
    #[must_use]
    pub fn page_size(&self) -> usize {
        self.inner.page_size
    }

    /// Number of pages in the snapshot's database image.
    #[must_use]
    pub fn db_size(&self) -> u32 {
        self.inner.db_size
    }

    /// A buffer for [`Self::read_page`].
    #[must_use]
    pub fn new_buffer(&self) -> SnapshotPageBuf {
        SnapshotPageBuf {
            bytes: vec![0_u8; self.inner.page_size + WAL_FRAME_HEADER_BYTES],
        }
    }

    /// The image of `page_no` in this snapshot, read into `buf`.
    ///
    /// A page the snapshot finds in the WAL is read from its frame, whose
    /// header must name the page (the check the connection's own WAL reads
    /// make); any other page is read from the database file, a short read
    /// past its end reading as zeros. A page beyond the snapshot's database
    /// size is an error.
    pub fn read_page<'b>(&self, page_no: PageNumber, buf: &'b mut SnapshotPageBuf) -> Result<&'b [u8]> {
        let pages = &*self.inner;
        let page_size = pages.page_size;
        let pgno = page_no.get();
        if pgno > pages.db_size {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "page {pgno} is beyond the snapshot's {} pages",
                    pages.db_size
                ),
            });
        }
        if let Some(frames) = &pages.wal
            && let Some(&frame) = frames.page_index.get(&pgno)
        {
            if frames
                .publication
                .last_commit_frame
                .is_none_or(|last| frame > last)
            {
                return Err(FrankenError::WalCorrupt {
                    detail: format!("page {pgno} indexed at uncommitted frame {frame}"),
                });
            }
            let frame_size = WAL_FRAME_HEADER_BYTES + page_size;
            let offset = u64::try_from(frame)
                .ok()
                .and_then(|frame| frame.checked_mul(frame_size as u64))
                .and_then(|bytes| bytes.checked_add(WAL_HEADER_BYTES))
                .ok_or_else(|| FrankenError::WalCorrupt {
                    detail: format!("frame {frame} offset overflows"),
                })?;
            let frame_bytes = &mut buf.bytes[..frame_size];
            let read = pread_full(&frames.file, frame_bytes, offset)?;
            if read < frame_size {
                return Err(FrankenError::WalCorrupt {
                    detail: format!("short read at frame {frame}: got {read}, need {frame_size}"),
                });
            }
            let header_page =
                u32::from_be_bytes([frame_bytes[0], frame_bytes[1], frame_bytes[2], frame_bytes[3]]);
            if header_page != pgno {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "WAL page index integrity failure: expected page {pgno} at frame \
                         {frame}, found page {header_page}"
                    ),
                });
            }
            return Ok(&buf.bytes[WAL_FRAME_HEADER_BYTES..frame_size]);
        }
        let offset = u64::from(pgno - 1) * page_size as u64;
        let page = &mut buf.bytes[..page_size];
        let read = pread_full(&pages.db_file, page, offset)?;
        page[read..].fill(0);
        Ok(&buf.bytes[..page_size])
    }
}

/// Fill `buf` from `offset` until it is full or the file ends; returns the
/// bytes read. Interrupted reads are retried.
fn pread_full(file: &File, buf: &mut [u8], offset: u64) -> Result<usize> {
    let mut total = 0_usize;
    while total < buf.len() {
        let at = offset + total as u64;
        match pread(file, &mut buf[total..], at) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(FrankenError::Io(error)),
        }
    }
    Ok(total)
}

#[cfg(unix)]
fn pread(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn pread(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

#[cfg(not(any(unix, windows)))]
fn pread(_file: &File, _buf: &mut [u8], _offset: u64) -> std::io::Result<usize> {
    Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use fsqlite_wal::WalGenerationIdentity;
    use fsqlite_wal::checksum::WalSalts;

    fn temp_file(bytes: &[u8]) -> Arc<File> {
        let mut file = tempfile::tempfile().expect("tempfile");
        file.write_all(bytes).expect("write");
        Arc::new(file)
    }

    fn publication(last_commit_frame: Option<usize>) -> WalPublicationSnapshot {
        WalPublicationSnapshot {
            publication_seq: 1,
            generation: WalGenerationIdentity {
                checkpoint_seq: 0,
                salts: WalSalts::default(),
            },
            last_commit_frame,
            commit_count: 1,
            latest_frame_entries: 1,
            index_is_partial: false,
        }
    }

    /// A WAL of `frames`, each `(page number, fill byte)`, with page size 16.
    fn wal_file(frames: &[(u32, u8)]) -> Arc<File> {
        let mut bytes = vec![0xAA_u8; WAL_HEADER_BYTES as usize];
        for &(pgno, fill) in frames {
            let mut header = [0_u8; WAL_FRAME_HEADER_BYTES];
            header[..4].copy_from_slice(&pgno.to_be_bytes());
            bytes.extend_from_slice(&header);
            bytes.extend_from_slice(&[fill; 16]);
        }
        temp_file(&bytes)
    }

    #[test]
    fn pages_resolve_through_the_snapshot_frames_then_the_database_file() {
        // Database: three pages of 16 bytes, filled 1, 2, 3.
        let db: Vec<u8> = (1_u8..=3).flat_map(|fill| [fill; 16]).collect();
        // WAL: frame 0 = page 2 (fill 20), frame 1 = page 2 (fill 21),
        // frame 2 = page 3 (fill 30, after the snapshot's last commit).
        let wal = wal_file(&[(2, 20), (2, 21), (3, 30)]);
        let frames = WalSnapshotFrames {
            publication: publication(Some(1)),
            file: wal,
            page_index: Arc::new(HashMap::from([(2_u32, 1_usize)])),
        };
        let reader = SnapshotPageReader::new(temp_file(&db), Some(frames), 16, 4);
        let mut buf = reader.new_buffer();
        let page = |n: u32, buf: &mut SnapshotPageBuf| {
            reader
                .read_page(PageNumber::new(n).unwrap(), buf)
                .map(<[u8]>::to_vec)
        };
        assert_eq!(page(1, &mut buf).unwrap(), vec![1; 16]);
        // The snapshot's latest frame of page 2, not the database image.
        assert_eq!(page(2, &mut buf).unwrap(), vec![21; 16]);
        // Page 3's frame is past the snapshot: the database image.
        assert_eq!(page(3, &mut buf).unwrap(), vec![3; 16]);
        // Page 4 is inside the snapshot but past the file's end: zeros.
        assert_eq!(page(4, &mut buf).unwrap(), vec![0; 16]);
        // Page 5 is past the snapshot.
        assert!(page(5, &mut buf).is_err());
    }

    #[test]
    fn a_frame_that_names_another_page_or_lies_past_the_commit_is_refused() {
        let db = vec![9_u8; 32];
        let wal = wal_file(&[(1, 10), (2, 20)]);
        // The index claims page 2 lives in frame 0, which holds page 1.
        let wrong_page = WalSnapshotFrames {
            publication: publication(Some(1)),
            file: Arc::clone(&wal),
            page_index: Arc::new(HashMap::from([(2_u32, 0_usize)])),
        };
        let reader = SnapshotPageReader::new(temp_file(&db), Some(wrong_page), 16, 2);
        let mut buf = reader.new_buffer();
        assert!(reader.read_page(PageNumber::new(2).unwrap(), &mut buf).is_err());
        // The index names a frame past the snapshot's last commit frame.
        let past_commit = WalSnapshotFrames {
            publication: publication(Some(0)),
            file: wal,
            page_index: Arc::new(HashMap::from([(2_u32, 1_usize)])),
        };
        let reader = SnapshotPageReader::new(temp_file(&db), Some(past_commit), 16, 2);
        assert!(reader.read_page(PageNumber::new(2).unwrap(), &mut buf).is_err());
        // With no committed frame visible, every page is the database's.
        let empty = WalSnapshotFrames {
            publication: publication(None),
            file: wal_file(&[(2, 20)]),
            page_index: Arc::new(HashMap::from([(2_u32, 0_usize)])),
        };
        let reader = SnapshotPageReader::new(temp_file(&db), Some(empty), 16, 2);
        assert_eq!(
            reader.read_page(PageNumber::new(2).unwrap(), &mut buf).unwrap(),
            &[9_u8; 16]
        );
    }

    #[test]
    fn the_reader_is_send_and_sync() {
        fn shareable<T: Send + Sync>() {}
        shareable::<SnapshotPageReader>();
    }
}
