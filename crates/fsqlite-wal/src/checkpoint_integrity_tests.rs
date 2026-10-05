// Included by checkpoint_executor.rs::tests. Reuse the actual MemoryVfs and
// existing recording/readback targets rather than a parallel executor model.

use super::validation::read_checkpoint_frame_headers;

fn checkpoint_integrity_wal_bytes(wal: &WalFile<impl VfsFile>, cx: &Cx) -> Vec<u8> {
    let len = usize::try_from(wal.file().file_size(cx).expect("WAL size"))
        .expect("test WAL fits usize");
    let mut bytes = vec![0; len];
    assert_eq!(wal.file().read(cx, &mut bytes, 0).expect("read WAL"), len);
    bytes
}

fn checkpoint_integrity_flip(wal: &WalFile<impl VfsFile>, cx: &Cx, frame: usize, byte: usize) {
    let offset = u64::try_from(crate::checksum::WAL_HEADER_SIZE + frame * wal.frame_size() + byte)
        .expect("test offset fits u64");
    let mut value = [0];
    assert_eq!(wal.file().read(cx, &mut value, offset).expect("read byte"), 1);
    value[0] ^= 0x80;
    wal.file().write(cx, &value, offset).expect("corrupt byte");
}

fn checkpoint_integrity_assert_no_effects(target: &ReadbackTarget) {
    assert!(target.written_pages.is_empty());
    assert!(target.published_prefixes.is_empty());
    assert!(target.truncate_to.is_none());
    assert_eq!(target.sync_count, 0);
    assert_eq!(target.gate_acquired, 0);
    assert_eq!(target.gate_released, 0);
}

#[test]
fn checkpoint_integrity_rejects_corruption_before_any_database_write() {
    for mode in [
        CheckpointMode::Passive,
        CheckpointMode::Full,
        CheckpointMode::Restart,
        CheckpointMode::Truncate,
    ] {
        // Page number, commit size, salt, stored checksum, and page payload.
        for byte in [0, 4, 8, 16, crate::checksum::WAL_FRAME_HEADER_SIZE + 123] {
            let cx = test_cx();
            let vfs = MemoryVfs::new();
            let mut wal = WalFile::create(&cx, open_wal_file(&vfs, &cx), PAGE_SIZE, 0, test_salts())
                .expect("create WAL");
            populate_wal(&mut wal, &cx, 3);
            let good = checkpoint_integrity_wal_bytes(&wal, &cx);
            // Corrupt after append, while cached WalFile metadata stays valid.
            checkpoint_integrity_flip(&wal, &cx, 2, byte);
            let corrupt = checkpoint_integrity_wal_bytes(&wal, &cx);
            let header = *wal.header();
            let mut target = ReadbackTarget::new(&vfs, &cx);
            let state = CheckpointState {
                total_frames: 3,
                backfilled_frames: 0,
                oldest_reader_frame: None,
            };
            let error = execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
                .expect_err("corrupt WAL must not become a checkpoint baseline");
            assert!(matches!(error, FrankenError::WalCorrupt { .. }), "{error:?}");
            checkpoint_integrity_assert_no_effects(&target);
            assert_eq!(wal.frame_count(), 3);
            assert_eq!(*wal.header(), header);
            assert_eq!(checkpoint_integrity_wal_bytes(&wal, &cx), corrupt);

            // Refusal must not poison the handle or consume the retry window.
            wal.file().write(&cx, &good, 0).expect("restore source");
            let result = execute_checkpoint(&cx, &mut wal, mode, state, &mut target)
                .expect("retry verified source");
            assert_eq!(result.frames_backfilled, 3);
            assert_eq!(target.written_pages.len(), 3);
            assert_eq!(target.published_prefixes, vec![3]);
        }
    }
}

#[test]
fn checkpoint_integrity_validates_superseded_frames_before_deduplication() {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let mut wal = WalFile::create(&cx, open_wal_file(&vfs, &cx), PAGE_SIZE, 0, test_salts())
        .expect("create WAL");
    wal.append_frame(&cx, 1, &sample_page(1), 1)
        .expect("old page");
    wal.append_frame(&cx, 1, &sample_page(2), 1)
        .expect("new page");
    checkpoint_integrity_flip(&wal, &cx, 0, crate::checksum::WAL_FRAME_HEADER_SIZE + 17);
    let mut target = RecordingTarget::new(); // No optional database readback.
    let error = execute_checkpoint(
        &cx,
        &mut wal,
        CheckpointMode::Truncate,
        CheckpointState {
            total_frames: 2,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        },
        &mut target,
    )
    .expect_err("deduplication must not hide a broken source checksum chain");
    assert!(matches!(error, FrankenError::WalCorrupt { .. }));
    assert!(target.pages.is_empty());
    assert_eq!(target.sync_count, 0);
    assert_eq!(wal.frame_count(), 2);
}

#[test]
fn checkpoint_integrity_rejects_short_final_payload_before_writing_earlier_pages() {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let mut wal = WalFile::create(&cx, open_wal_file(&vfs, &cx), PAGE_SIZE, 0, test_salts())
        .expect("create WAL");
    populate_wal(&mut wal, &cx, 3);
    let len = wal.file().file_size(&cx).expect("WAL length");
    wal.file_mut()
        .truncate(&cx, len - 1)
        .expect("truncate one payload byte");
    let mut target = ReadbackTarget::new(&vfs, &cx);
    let error = execute_checkpoint(
        &cx,
        &mut wal,
        CheckpointMode::Full,
        CheckpointState {
            total_frames: 3,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        },
        &mut target,
    )
    .expect_err("preflight must include the final payload");
    assert!(matches!(error, FrankenError::WalCorrupt { .. }));
    checkpoint_integrity_assert_no_effects(&target);
    assert_eq!(wal.file().file_size(&cx).expect("unchanged WAL length"), len - 1);
}

#[test]
fn checkpoint_integrity_verified_windows_cover_page_sizes_and_read_boundaries() {
    for page_size in [512_u32, 4096, 65_536] {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut wal = WalFile::create(&cx, open_wal_file(&vfs, &cx), page_size, 0, test_salts())
            .expect("create WAL");
        let payload = vec![0x5a; usize::try_from(page_size).expect("page size")];
        let count = (64 * 1024 / wal.frame_size()).max(1) + 1;
        for frame in 0..count {
            let page_no = u32::try_from(frame + 1).expect("test page");
            wal.append_frame(&cx, page_no, &payload, page_no)
                .expect("append committed page");
        }
        for start in [0, 1, count - 1] {
            let headers = read_checkpoint_frame_headers(&wal, &cx, start, count)
                .expect("verified window");
            assert_eq!(headers.len(), count - start);
            assert_eq!(
                headers[0].0.page_number,
                u32::try_from(start + 1).expect("page")
            );
        }
        // The last payload byte in the first I/O chunk must be checked.
        let chunk_last = count - 2;
        checkpoint_integrity_flip(&wal, &cx, chunk_last, wal.frame_size() - 1);
        assert!(matches!(
            read_checkpoint_frame_headers(&wal, &cx, 0, count).wait(),
            Err(FrankenError::WalCorrupt { .. })
        ));
    }
}

#[test]
fn checkpoint_integrity_rejects_replaced_generation_and_bad_resume_seed() {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let mut wal = WalFile::create(&cx, open_wal_file(&vfs, &cx), PAGE_SIZE, 0, test_salts())
        .expect("create WAL");
    populate_wal(&mut wal, &cx, 3);
    let good = checkpoint_integrity_wal_bytes(&wal, &cx);
    checkpoint_integrity_flip(&wal, &cx, 0, 16);
    assert!(matches!(
        read_checkpoint_frame_headers(&wal, &cx, 1, 3).wait(),
        Err(FrankenError::WalCorrupt { .. })
    ));
    wal.file()
        .write(&cx, &good, 0)
        .expect("restore checksum seed");
    let mut peer = WalFile::open(&cx, open_wal_file(&vfs, &cx)).expect("open peer");
    peer.reset(&cx, 1, test_salts(), false)
        .expect("replace generation with same salts");
    assert!(matches!(
        read_checkpoint_frame_headers(&wal, &cx, 0, 3).wait(),
        Err(FrankenError::WalCorrupt { .. })
    ));
}

/// Change the second source frame after the executor has written the first
/// page. A separate MemoryVfs handle avoids borrowing the executing WalFile.
struct CheckpointSourceMutationTarget {
    inner: ReadbackTarget,
    peer: <MemoryVfs as Vfs>::File,
    replacement: Option<(u64, Vec<u8>)>,
}

impl CheckpointTarget for CheckpointSourceMutationTarget {
    fn write_page<'a>(
        &'a mut self,
        cx: &'a Cx,
        page: PageNumber,
        data: &'a [u8],
    ) -> CheckpointTargetFuture<'a, ()> {
        Box::pin(async move {
            self.inner.write_page(cx, page, data).await?;
            if let Some((offset, bytes)) = self.replacement.take() {
                self.peer.write(cx, &bytes, offset).await?;
            }
            Ok(())
        })
    }

    fn truncate_db<'a>(&'a mut self, cx: &'a Cx, pages: u32) -> CheckpointTargetFuture<'a, ()> {
        self.inner.truncate_db(cx, pages)
    }

    fn sync_db<'a>(&'a mut self, cx: &'a Cx) -> CheckpointTargetFuture<'a, ()> {
        self.inner.sync_db(cx)
    }

    fn publish_backfill<'a>(
        &'a mut self,
        cx: &'a Cx,
        header: &'a WalHeader,
        frames: u32,
    ) -> CheckpointTargetFuture<'a, ()> {
        self.inner.publish_backfill(cx, header, frames)
    }
}

#[test]
fn checkpoint_integrity_copy_pass_is_bound_to_validated_source_bytes() {
    let cx = test_cx();
    let vfs = MemoryVfs::new();
    let mut wal = WalFile::create(&cx, open_wal_file(&vfs, &cx), PAGE_SIZE, 0, test_salts())
        .expect("create WAL");
    populate_wal(&mut wal, &cx, 2);
    let mut replacement = checkpoint_integrity_wal_bytes(&wal, &cx)
        [crate::checksum::WAL_HEADER_SIZE + wal.frame_size()..]
        .to_vec();
    replacement[crate::checksum::WAL_FRAME_HEADER_SIZE + 123] ^= 0x80;
    // Even a newly self-consistent frame must not replace the preflight receipt.
    let seed = wal.read_frame_header(&cx, 0).expect("predecessor").checksum;
    crate::checksum::write_wal_frame_checksum(
        &mut replacement,
        usize::try_from(PAGE_SIZE).expect("page size"),
        seed,
        wal.header().big_endian_checksum(),
    )
    .expect("recompute replacement checksum");
    let mut target = CheckpointSourceMutationTarget {
        inner: ReadbackTarget::new(&vfs, &cx),
        peer: open_wal_file(&vfs, &cx),
        replacement: Some((
            u64::try_from(crate::checksum::WAL_HEADER_SIZE + wal.frame_size()).expect("offset"),
            replacement,
        )),
    };
    let error = execute_checkpoint(
        &cx,
        &mut wal,
        CheckpointMode::Truncate,
        CheckpointState {
            total_frames: 2,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        },
        &mut target,
    )
    .expect_err("copy pass must reject changed source before writing it");
    assert!(matches!(error, FrankenError::WalCorrupt { .. }));
    assert_eq!(target.inner.written_pages, vec![PageNumber::ONE]);
    assert!(target.inner.published_prefixes.is_empty());
    assert_eq!(target.inner.sync_count, 0);
    assert!(target.inner.truncate_to.is_none());
    assert_eq!(wal.frame_count(), 2);
}
