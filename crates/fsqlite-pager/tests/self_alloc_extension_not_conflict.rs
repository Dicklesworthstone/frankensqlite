use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::Result;
use fsqlite_pager::traits::{
    CheckpointPageWriter, CheckpointResult, JournalMode, MvccPager, TransactionHandle,
    TransactionMode, WalBackend, WalFuture,
};
use fsqlite_pager::{CheckpointMode, SimplePager};
use fsqlite_types::PageSize;
use fsqlite_types::cx::Cx;
use fsqlite_vfs::MemoryVfs;

type SharedFrames = Arc<Mutex<Vec<(u32, Vec<u8>, u32)>>>;

fn block_on_test<F: Future>(future: F) -> F::Output {
    RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .expect("pager integration-test runtime should build")
        .block_on(future)
}

#[derive(Default)]
struct NoopWalBackend;

impl WalBackend for NoopWalBackend {
    fn append_frame<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _page_number: u32,
        _page_data: &'a [u8],
        _db_size_if_commit: u32,
    ) -> WalFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn read_page<'a>(
        &'a mut self,
        _cx: &'a Cx,
        _page_number: u32,
    ) -> WalFuture<'a, Option<Vec<u8>>> {
        Box::pin(async { Ok(None) })
    }

    fn sync(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    fn frame_count(&self) -> usize {
        0
    }

    fn checkpoint<'a>(
        &'a mut self,
        _cx: &'a Cx,
        mode: CheckpointMode,
        _writer: &'a mut dyn CheckpointPageWriter,
        _backfilled_frames: u32,
        _oldest_reader_frame: Option<u32>,
    ) -> WalFuture<'a, CheckpointResult> {
        Box::pin(async move {
            Ok(CheckpointResult {
                total_frames: 0,
                frames_backfilled: 0,
                completed: true,
                wal_was_reset: matches!(mode, CheckpointMode::Restart | CheckpointMode::Truncate),
                requested_mode: mode,
                effective_mode: mode,
            })
        })
    }
}

struct SharedWalBackend {
    frames: SharedFrames,
}

impl SharedWalBackend {
    fn with_shared_frames(frames: SharedFrames) -> Self {
        Self { frames }
    }
}

impl WalBackend for SharedWalBackend {
    fn append_frame<'a>(
        &'a mut self,
        _cx: &'a Cx,
        page_number: u32,
        page_data: &'a [u8],
        db_size_if_commit: u32,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            self.frames
                .lock()
                .expect("shared wal frames lock should not poison")
                .push((page_number, page_data.to_vec(), db_size_if_commit));
            Ok(())
        })
    }

    fn append_frames<'a>(
        &'a mut self,
        _cx: &'a Cx,
        frames: &'a [fsqlite_pager::traits::WalFrameRef<'a>],
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            let mut written = self
                .frames
                .lock()
                .expect("shared wal frames lock should not poison");
            for frame in frames {
                written.push((
                    frame.page_number,
                    frame.page_data.to_vec(),
                    frame.db_size_if_commit,
                ));
            }
            Ok(())
        })
    }

    fn read_page<'a>(
        &'a mut self,
        _cx: &'a Cx,
        page_number: u32,
    ) -> WalFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let frames = self
                .frames
                .lock()
                .expect("shared wal frames lock should not poison");
            Ok(frames
                .iter()
                .rev()
                .find(|(pn, _, _)| *pn == page_number)
                .map(|(_, data, _)| data.clone()))
        })
    }

    fn committed_txns_since_page<'a>(
        &'a mut self,
        _cx: &'a Cx,
        page_number: u32,
    ) -> WalFuture<'a, u64> {
        Box::pin(async move {
            let frames = self
                .frames
                .lock()
                .expect("shared wal frames lock should not poison");
            let last_page_frame = frames.iter().rposition(|(pn, _, _)| *pn == page_number);
            let Some(last_page_frame) = last_page_frame else {
                return Ok(frames
                    .iter()
                    .filter(|(_, _, db_size_if_commit)| *db_size_if_commit > 0)
                    .count() as u64);
            };

            let mut page_commit_seen = false;
            let mut committed_txns_after_page = 0_u64;
            for (frame_index, (_, _, db_size_if_commit)) in frames.iter().enumerate() {
                if *db_size_if_commit == 0 {
                    continue;
                }
                if !page_commit_seen && frame_index >= last_page_frame {
                    page_commit_seen = true;
                    continue;
                }
                if page_commit_seen {
                    committed_txns_after_page = committed_txns_after_page.saturating_add(1);
                }
            }
            Ok(committed_txns_after_page)
        })
    }

    fn committed_txn_count<'a>(&'a mut self, _cx: &'a Cx) -> WalFuture<'a, u64> {
        Box::pin(async move {
            let frames = self
                .frames
                .lock()
                .expect("shared wal frames lock should not poison");
            Ok(frames
                .iter()
                .filter(|(_, _, db_size_if_commit)| *db_size_if_commit > 0)
                .count() as u64)
        })
    }

    fn sync(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    fn frame_count(&self) -> usize {
        self.frames
            .lock()
            .expect("shared wal frames lock should not poison")
            .len()
    }

    fn checkpoint<'a>(
        &'a mut self,
        _cx: &'a Cx,
        mode: CheckpointMode,
        _writer: &'a mut dyn CheckpointPageWriter,
        _backfilled_frames: u32,
        _oldest_reader_frame: Option<u32>,
    ) -> WalFuture<'a, CheckpointResult> {
        Box::pin(async move {
            let total_frames = u32::try_from(self.frame_count()).unwrap_or(u32::MAX);
            Ok(CheckpointResult {
                total_frames,
                frames_backfilled: 0,
                completed: false,
                wal_was_reset: false,
                requested_mode: mode,
                effective_mode: mode,
            })
        })
    }
}

async fn wal_pager_pair() -> (Cx, SimplePager<MemoryVfs>, SimplePager<MemoryVfs>) {
    let cx = Cx::new();
    let vfs = MemoryVfs::new();
    let path = PathBuf::from("/self_alloc_extension_peer_interleave.db");
    let pager_a = SimplePager::open_with_cx(&cx, vfs.clone(), &path, PageSize::DEFAULT)
        .await
        .expect("pager A open");
    let pager_b = SimplePager::open_with_cx(&cx, vfs, &path, PageSize::DEFAULT)
        .await
        .expect("pager B open");

    let frames: SharedFrames = Arc::new(Mutex::new(Vec::new()));
    pager_a
        .set_wal_backend(Box::new(SharedWalBackend::with_shared_frames(Arc::clone(
            &frames,
        ))))
        .expect("pager A WAL backend should install");
    pager_b
        .set_wal_backend(Box::new(SharedWalBackend::with_shared_frames(frames)))
        .expect("pager B WAL backend should install");
    pager_a
        .set_journal_mode(&cx, JournalMode::Wal)
        .await
        .expect("pager A WAL mode should be available");
    pager_b
        .set_journal_mode(&cx, JournalMode::Wal)
        .await
        .expect("pager B WAL mode should be available");

    (cx, pager_a, pager_b)
}

#[test]
fn self_allocated_eof_page_stays_out_of_conflict_surface() {
    block_on_test(async {
        let cx = Cx::new();
        let pager = SimplePager::open_with_cx(
            &cx,
            MemoryVfs::new(),
            Path::new("/self_alloc_extension.db"),
            PageSize::DEFAULT,
        )
        .await
        .expect("pager should open");
        pager
            .set_wal_backend(Box::new(NoopWalBackend))
            .expect("no-op WAL backend should install");
        pager
            .set_journal_mode(&cx, JournalMode::Wal)
            .await
            .expect("WAL mode should be available");

        let mut txn = pager
            .begin(&cx, TransactionMode::Concurrent)
            .await
            .expect("concurrent transaction should begin");
        let page = txn
            .allocate_page(&cx)
            .await
            .expect("allocation should extend EOF");
        assert_eq!(
            page.get(),
            2,
            "fresh database should extend from page 1 to page 2"
        );
        txn.write_page(&cx, page, &[0xA5; 64])
            .await
            .expect("newly allocated page should accept writes");
        let read_back = txn
            .get_page(&cx, page)
            .await
            .expect("same transaction should be able to read its own newly allocated page");
        assert_eq!(
            read_back.as_ref()[0],
            0xA5,
            "self-allocated extension page should remain readable inside the allocating transaction"
        );

        let pending_commit = txn
            .pending_commit_pages()
            .expect("pending commit surface should be available");
        assert!(
            pending_commit.contains(&page),
            "self-allocated extension page must be written at commit"
        );
    });
}

#[test]
fn self_allocated_extension_page_survives_peer_writer_interleaving() {
    block_on_test(async {
        let (cx, pager_a, pager_b) = wal_pager_pair().await;
        let page_size = PageSize::DEFAULT.as_usize();

        {
            let mut seed = pager_a
                .begin(&cx, TransactionMode::Immediate)
                .await
                .expect("seed transaction should begin");
            let durable_page = seed.allocate_page(&cx).await.expect("seed page allocation");
            assert_eq!(durable_page.get(), 2, "seed should create durable page 2");
            seed.write_page(&cx, durable_page, &vec![0x11; page_size])
                .await
                .expect("seed page should accept writes");
            seed.commit(&cx).await.expect("seed commit should succeed");
        }

        let mut txn_a = pager_a
            .begin(&cx, TransactionMode::Concurrent)
            .await
            .expect("pager A concurrent transaction should begin");
        let extension_page = txn_a
            .allocate_page(&cx)
            .await
            .expect("pager A should allocate page beyond durable db_size");
        assert_eq!(
            extension_page.get(),
            3,
            "pager A should extend from durable page count 2 to 3"
        );
        txn_a
            .write_page(&cx, extension_page, &vec![0xA5; page_size])
            .await
            .expect("pager A should be able to stage writes to its extension page");

        {
            let mut txn_b = pager_b
                .begin(&cx, TransactionMode::Concurrent)
                .await
                .expect("pager B concurrent transaction should begin");
            txn_b
                .write_page(
                    &cx,
                    fsqlite_types::PageNumber::new(2).expect("page 2 should be valid"),
                    &vec![0x22; page_size],
                )
                .await
                .expect("pager B should be able to update an existing durable page");
            txn_b
                .commit(&cx)
                .await
                .expect("pager B unrelated commit should succeed");
        }

        let read_back = txn_a.get_page(&cx, extension_page).await.expect(
            "peer WAL publication must not make pager A lose visibility to its own extension page",
        );
        assert_eq!(
            read_back.as_ref()[0],
            0xA5,
            "pager A must still read back its own staged extension-page contents after peer commit"
        );
    });
}

#[test]
fn peer_growth_commit_refreshes_reader_snapshot_boundary() {
    block_on_test(async {
        let (cx, pager_a, pager_b) = wal_pager_pair().await;
        let page_size = PageSize::DEFAULT.as_usize();

        {
            let mut seed = pager_a
                .begin(&cx, TransactionMode::Immediate)
                .await
                .expect("seed transaction should begin");
            let durable_page = seed.allocate_page(&cx).await.expect("seed page allocation");
            assert_eq!(durable_page.get(), 2, "seed should create durable page 2");
            seed.write_page(&cx, durable_page, &vec![0x11; page_size])
                .await
                .expect("seed page should accept writes");
            seed.commit(&cx).await.expect("seed commit should succeed");
        }

        let grown_page = {
            let mut grow = pager_a
                .begin(&cx, TransactionMode::Immediate)
                .await
                .expect("growth transaction should begin");
            let grown_page = grow
                .allocate_page(&cx)
                .await
                .expect("growth page allocation");
            assert_eq!(
                grown_page.get(),
                3,
                "growth commit should create durable page 3"
            );
            grow.write_page(&cx, grown_page, &vec![0x33; page_size])
                .await
                .expect("growth page should accept writes");
            grow.commit(&cx)
                .await
                .expect("growth commit should succeed");
            grown_page
        };

        let reader = pager_b
            .begin(&cx, TransactionMode::ReadOnly)
            .await
            .expect("peer reader should begin after growth commit");
        let read_back = reader
            .get_page(&cx, grown_page)
            .await
            .expect("peer reader must refresh its snapshot boundary to include committed growth");
        assert_eq!(
            read_back.as_ref()[0],
            0x33,
            "peer reader must see the committed contents of the grown page"
        );
    });
}
