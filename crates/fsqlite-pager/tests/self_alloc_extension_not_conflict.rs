// bd-h9o9r: integration tests are separate crates, so fsqlite-pager's
// crate-level rationale applies here too — the pager's futures are
// deliberately not `Send` (thread-local runtime design).
#![allow(clippy::future_not_send)]

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::traits::{
    CheckpointPageWriter, CheckpointResult, JournalMode, MvccPager, TransactionHandle,
    TransactionMode, WalBackend, WalFuture,
};
use fsqlite_pager::{CheckpointMode, SimplePager};
use fsqlite_types::cx::Cx;
use fsqlite_types::{PageNumber, PageSize};
use fsqlite_vfs::MemoryVfs;
use fsqlite_wal::{ParallelWalCommitCertificate, ParallelWalFramePayloadDigestBuilder};

type WalFrame = (u32, Vec<u8>, u32);

#[derive(Clone)]
struct PendingParallelWalCommit {
    certificate: ParallelWalCommitCertificate,
    wal_frame_start: u64,
    wal_frame_end: u64,
}

#[derive(Default)]
struct SharedWalState {
    frames: Vec<WalFrame>,
    pending_commit: Option<PendingParallelWalCommit>,
}

type SharedWalStateRef = Arc<Mutex<SharedWalState>>;

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

/// Ready-only, process-local WAL fixture for the pager visibility scenarios.
///
/// Certificate persistence and batch append contain no suspension point and
/// share one state lock. The pending proof is therefore consumed in the same
/// poll that admits its exact frame batch. Crash recovery and durable sidecar
/// reconciliation belong to the production adapter tests, not this fixture.
struct SharedWalBackend {
    state: SharedWalStateRef,
}

impl SharedWalBackend {
    fn with_shared_state(state: SharedWalStateRef) -> Self {
        Self { state }
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
            let mut state = self
                .state
                .lock()
                .expect("shared WAL state lock should not poison");
            if state.pending_commit.is_some() {
                return Err(FrankenError::internal(
                    "test WAL backend requires certified commits to use one exact frame batch",
                ));
            }
            state
                .frames
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
            let mut state = self
                .state
                .lock()
                .expect("shared WAL state lock should not poison");
            let pending = state.pending_commit.as_ref().ok_or_else(|| {
                FrankenError::internal(
                    "test WAL backend received a frame batch without a pending certificate",
                )
            })?;
            let expected_start = u64::try_from(state.frames.len())
                .map_err(|_| FrankenError::internal("synthetic WAL frame count exceeds u64"))?
                .checked_add(1)
                .ok_or_else(|| FrankenError::internal("synthetic WAL frame count overflow"))?;
            if pending.wal_frame_start != expected_start {
                return Err(FrankenError::internal(
                    "test WAL backend certificate does not begin after the current frame prefix",
                ));
            }
            let expected_end = expected_start
                .checked_add(
                    u64::try_from(frames.len())
                        .map_err(|_| FrankenError::internal("frame batch length exceeds u64"))?
                        .saturating_sub(1),
                )
                .ok_or_else(|| FrankenError::internal("synthetic WAL frame interval overflow"))?;
            if pending.wal_frame_end != expected_end
                || frames.len()
                    != usize::try_from(pending.certificate.page_set_size)
                        .map_err(|_| FrankenError::internal("certificate page set exceeds usize"))?
            {
                return Err(FrankenError::internal(
                    "test WAL backend frame batch does not match its pending certificate interval",
                ));
            }
            let final_frame = frames.last().ok_or_else(|| {
                FrankenError::internal("test WAL backend rejected an empty certified frame batch")
            })?;
            if final_frame.db_size_if_commit == 0
                || final_frame.db_size_if_commit != pending.certificate.db_size_pages
            {
                return Err(FrankenError::internal(
                    "test WAL backend frame batch lacks the certified commit marker",
                ));
            }
            let mut digest = ParallelWalFramePayloadDigestBuilder::new();
            for frame in frames {
                digest.update(
                    PageNumber::new(frame.page_number).ok_or_else(|| {
                        FrankenError::internal("test WAL frame batch contains page zero")
                    })?,
                    frame.db_size_if_commit,
                    frame.page_data,
                );
            }
            if digest.finalize() != pending.certificate.wal_frame_payload_digest {
                return Err(FrankenError::internal(
                    "test WAL backend frame batch does not match the certificate payload digest",
                ));
            }

            for frame in frames {
                state.frames.push((
                    frame.page_number,
                    frame.page_data.to_vec(),
                    frame.db_size_if_commit,
                ));
            }
            state.pending_commit = None;
            Ok(())
        })
    }

    fn persist_parallel_wal_commit_certificate<'a>(
        &'a mut self,
        _cx: &'a Cx,
        certificate: &'a ParallelWalCommitCertificate,
        wal_frame_start: u64,
        wal_frame_end: u64,
        _sync: bool,
    ) -> WalFuture<'a, ()> {
        Box::pin(async move {
            if !certificate.checksum_is_valid() {
                return Err(FrankenError::internal(
                    "test WAL backend rejected a damaged parallel-WAL certificate",
                ));
            }
            let covered_frame_count = wal_frame_end
                .checked_sub(wal_frame_start)
                .and_then(|distance| distance.checked_add(1))
                .ok_or_else(|| {
                    FrankenError::internal(
                        "test WAL backend rejected an invalid certificate frame interval",
                    )
                })?;
            if covered_frame_count != u64::from(certificate.page_set_size) {
                return Err(FrankenError::internal(
                    "test WAL backend certificate frame interval does not match its page set",
                ));
            }
            let expected_committed_prefix = wal_frame_start.checked_sub(1).ok_or_else(|| {
                FrankenError::internal(
                    "test WAL backend certificate interval must use one-based frame indexes",
                )
            })?;
            let mut state = self
                .state
                .lock()
                .expect("shared WAL state lock should not poison");
            let current_frame_count = u64::try_from(state.frames.len())
                .map_err(|_| FrankenError::internal("synthetic WAL frame count exceeds u64"))?;
            if current_frame_count != expected_committed_prefix {
                return Err(FrankenError::internal(
                    "test WAL backend certificate does not start after the committed WAL prefix",
                ));
            }
            if state.pending_commit.is_some() {
                return Err(FrankenError::internal(
                    "test WAL backend already has an unconsumed commit certificate",
                ));
            }
            state.pending_commit = Some(PendingParallelWalCommit {
                certificate: certificate.clone(),
                wal_frame_start,
                wal_frame_end,
            });
            Ok(())
        })
    }

    fn read_page<'a>(
        &'a mut self,
        _cx: &'a Cx,
        page_number: u32,
    ) -> WalFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .expect("shared WAL state lock should not poison");
            Ok(state
                .frames
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
            let state = self
                .state
                .lock()
                .expect("shared WAL state lock should not poison");
            let last_page_frame = state
                .frames
                .iter()
                .rposition(|(pn, _, _)| *pn == page_number);
            let Some(last_page_frame) = last_page_frame else {
                return Ok(state
                    .frames
                    .iter()
                    .filter(|(_, _, db_size_if_commit)| *db_size_if_commit > 0)
                    .count() as u64);
            };

            let mut page_commit_seen = false;
            let mut committed_txns_after_page = 0_u64;
            for (frame_index, (_, _, db_size_if_commit)) in state.frames.iter().enumerate() {
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
            let state = self
                .state
                .lock()
                .expect("shared WAL state lock should not poison");
            Ok(state
                .frames
                .iter()
                .filter(|(_, _, db_size_if_commit)| *db_size_if_commit > 0)
                .count() as u64)
        })
    }

    fn sync(&mut self, _cx: &Cx) -> Result<()> {
        Ok(())
    }

    fn frame_count(&self) -> usize {
        self.state
            .lock()
            .expect("shared WAL state lock should not poison")
            .frames
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

fn assert_no_pending_certificate(state: &SharedWalStateRef) {
    assert!(
        state
            .lock()
            .expect("shared WAL state lock should not poison")
            .pending_commit
            .is_none(),
        "every successful test commit must consume its exact certificate"
    );
}

async fn wal_pager_pair(
    path: &Path,
) -> (
    Cx,
    SimplePager<MemoryVfs>,
    SimplePager<MemoryVfs>,
    SharedWalStateRef,
) {
    let cx = Cx::new();
    let vfs = MemoryVfs::new();
    let pager_a = SimplePager::open_with_cx(&cx, vfs.clone(), path, PageSize::DEFAULT)
        .await
        .expect("pager A open");
    let pager_b = SimplePager::open_with_cx(&cx, vfs, path, PageSize::DEFAULT)
        .await
        .expect("pager B open");

    let state = Arc::new(Mutex::new(SharedWalState::default()));
    pager_a
        .set_wal_backend(Box::new(SharedWalBackend::with_shared_state(Arc::clone(
            &state,
        ))))
        .expect("pager A WAL backend should install");
    pager_b
        .set_wal_backend(Box::new(SharedWalBackend::with_shared_state(Arc::clone(
            &state,
        ))))
        .expect("pager B WAL backend should install");
    pager_a
        .set_journal_mode(&cx, JournalMode::Wal)
        .await
        .expect("pager A WAL mode should be available");
    pager_b
        .set_journal_mode(&cx, JournalMode::Wal)
        .await
        .expect("pager B WAL mode should be available");

    (cx, pager_a, pager_b, state)
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
        let (cx, pager_a, pager_b, state) =
            wal_pager_pair(Path::new("/self_alloc_extension_peer_interleave.db")).await;
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
        txn_a
            .commit(&cx)
            .await
            .expect("pager A extension-page commit should survive peer interleaving");
        let peer_reader = pager_b
            .begin(&cx, TransactionMode::ReadOnly)
            .await
            .expect("peer reader should begin after pager A commit");
        let peer_read_back = peer_reader
            .get_page(&cx, extension_page)
            .await
            .expect("peer reader should see pager A's committed extension page");
        assert_eq!(
            peer_read_back.as_ref()[0],
            0xA5,
            "pager A's extension-page contents must become peer-visible after commit"
        );
        assert_no_pending_certificate(&state);
    });
}

#[test]
fn peer_growth_commit_refreshes_reader_snapshot_boundary() {
    block_on_test(async {
        let (cx, pager_a, pager_b, state) =
            wal_pager_pair(Path::new("/self_alloc_extension_peer_growth.db")).await;
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
        assert_no_pending_certificate(&state);
    });
}
