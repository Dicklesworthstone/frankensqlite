//! GH #598: batch replay I/O without weakening checksum recovery, commit
//! boundaries, sampled horizons, or observational-reader neutrality.
#![cfg(not(target_arch = "wasm32"))]
#![allow(clippy::future_not_send)]

use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use asupersync::runtime::RuntimeBuilder;
use fsqlite_error::{FrankenError, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
use fsqlite_vfs::{MemoryVfs, ShmRegion, Vfs, VfsFile};
use fsqlite_wal::wal::WalAppendFrameRef;
use fsqlite_wal::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalFile, WalSalts};

type MemoryFile = <MemoryVfs as Vfs>::File;
const READ_BUDGET: usize = 64 * 1024;
const PAGE_SIZE: u32 = 4096;
const SALTS: WalSalts = WalSalts {
    salt1: 0x73A6_C921,
    salt2: 0x28D5_B407,
};

fn run(future: impl Future<Output = ()>) {
    RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .expect("test runtime")
        .block_on(future);
}

fn open_file(vfs: &MemoryVfs, cx: &Cx) -> MemoryFile {
    vfs.open(
        cx,
        Some(Path::new("wal-reopen.db-wal")),
        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
    )
    .expect("open WAL")
    .0
}

#[derive(Clone, Copy, Debug)]
struct Read {
    offset: u64,
    requested: usize,
    returned: usize,
}

#[derive(Clone, Copy, Default)]
struct ReadFault {
    visible_end: Option<u64>,
    fail_from: Option<u64>,
    max_read: Option<usize>,
}

struct PeerAppend {
    file: MemoryFile,
    offset: u64,
    bytes: Vec<u8>,
}

#[derive(Default)]
struct Audit {
    reads: Mutex<Vec<Read>>,
    fault: Mutex<ReadFault>,
    append_on_data_read: Mutex<Option<PeerAppend>>,
    mutations: AtomicUsize,
}

impl Audit {
    fn data_reads(&self) -> Vec<Read> {
        self.reads
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|read| read.offset >= u64::try_from(WAL_HEADER_SIZE).unwrap())
            .collect()
    }
}

struct ObservedFile {
    inner: MemoryFile,
    audit: Arc<Audit>,
}

impl VfsFile for ObservedFile {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.inner.close(cx)
    }

    async fn read<'a>(&'a self, cx: &'a Cx, buf: &'a mut [u8], offset: u64) -> Result<usize> {
        // A distinct descriptor appends only after the reader has sampled its
        // file size. This is deterministic scheduling of a concurrent writer.
        if offset >= u64::try_from(WAL_HEADER_SIZE).unwrap() {
            let append = self.audit.append_on_data_read.lock().unwrap().take();
            if let Some(append) = append {
                append.file.write(cx, &append.bytes, append.offset).await?;
            }
        }
        let fault = *self.audit.fault.lock().unwrap();
        if fault
            .fail_from
            .is_some_and(|end| offset.saturating_add(u64::try_from(buf.len()).unwrap()) > end)
        {
            return Err(std::io::Error::other("injected replay read failure").into());
        }
        let requested = buf.len();
        let visible = fault.visible_end.map_or(requested, |end| {
            usize::try_from(end.saturating_sub(offset))
                .unwrap_or(usize::MAX)
                .min(requested)
        });
        let visible = visible.min(fault.max_read.unwrap_or(requested));
        let returned = self.inner.read(cx, &mut buf[..visible], offset).await?;
        buf[returned..].fill(0);
        self.audit.reads.lock().unwrap().push(Read {
            offset,
            requested,
            returned,
        });
        Ok(returned)
    }

    async fn write<'a>(&'a self, cx: &'a Cx, buf: &'a [u8], offset: u64) -> Result<()> {
        self.audit.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.write(cx, buf, offset).await
    }

    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.audit.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.truncate(cx, size)
    }

    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.audit.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.sync(cx, flags)
    }

    fn file_size(&self, cx: &Cx) -> Result<u64> {
        self.inner.file_size(cx)
    }

    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.lock(cx, level)
    }

    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.unlock(cx, level)
    }

    fn lock_external_wal_append(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_wal_append(cx)
    }

    fn owns_external_wal_append_write(&self, cx: &Cx) -> Result<bool> {
        self.inner.owns_external_wal_append_write(cx)
    }

    fn restore_external_wal_append_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_wal_append_attempt(cx)
    }

    fn lock_external_shared_snapshot(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_shared_snapshot(cx)
    }

    fn restore_external_shared_snapshot_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_shared_snapshot_attempt(cx)
    }

    fn lock_external_maintenance(&mut self, cx: &Cx, wal: bool) -> Result<()> {
        self.inner.lock_external_maintenance(cx, wal)
    }

    fn lock_external_wal_recovery(&mut self, cx: &Cx) -> Result<()> {
        self.inner.lock_external_wal_recovery(cx)
    }

    fn restore_external_maintenance_attempt(&mut self, cx: &Cx) -> Result<()> {
        self.inner.restore_external_maintenance_attempt(cx)
    }

    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
        self.inner.check_reserved_lock(cx)
    }

    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
        self.inner.shm_map(cx, region, size, extend)
    }

    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        self.inner.shm_lock(cx, offset, n, flags)
    }

    fn shm_barrier(&self) {
        self.inner.shm_barrier();
    }

    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
        self.inner.shm_unmap(cx, delete)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Replay {
    Open,
    Rebuild,
    Refresh,
}

const REPLAYS: [Replay; 3] = [Replay::Open, Replay::Rebuild, Replay::Refresh];

struct Fixture {
    cx: Cx,
    vfs: MemoryVfs,
    writer: WalFile<MemoryFile>,
    reader: Option<WalFile<ObservedFile>>,
    audit: Arc<Audit>,
}

impl Fixture {
    async fn new(mode: Replay, page_size: u32, frames: usize, commit_every: usize) -> Self {
        let cx = Cx::default();
        let vfs = MemoryVfs::new();
        let writer = WalFile::create(&cx, open_file(&vfs, &cx), page_size, 17, SALTS)
            .await
            .unwrap();
        let mut fixture = Self {
            cx,
            vfs,
            writer,
            reader: None,
            audit: Arc::new(Audit::default()),
        };
        if mode == Replay::Refresh {
            fixture.open_reader().await.unwrap();
        }
        fixture.append(frames, commit_every, 0x31).await;
        if mode == Replay::Rebuild {
            fixture.open_reader().await.unwrap();
        }
        fixture.audit.reads.lock().unwrap().clear();
        fixture
    }

    async fn append(&mut self, frames: usize, commit_every: usize, seed: u8) {
        let page_size = self.writer.frame_size() - WAL_FRAME_HEADER_SIZE;
        for frame in 0..frames {
            let page = vec![seed.wrapping_add(u8::try_from(frame % 251).unwrap()); page_size];
            let committed = commit_every != 0 && (frame + 1) % commit_every == 0;
            self.writer
                .append_frame(
                    &self.cx,
                    u32::try_from(frame % 7 + 1).unwrap(),
                    &page,
                    if committed { 7 } else { 0 },
                )
                .await
                .unwrap();
        }
        self.writer.sync(&self.cx, SyncFlags::NORMAL).unwrap();
    }

    async fn open_reader(&mut self) -> Result<()> {
        self.reader = Some(
            WalFile::open(
                &self.cx,
                ObservedFile {
                    inner: open_file(&self.vfs, &self.cx),
                    audit: Arc::clone(&self.audit),
                },
            )
            .await?,
        );
        Ok(())
    }

    async fn replay(&mut self, mode: Replay) -> Result<()> {
        match mode {
            Replay::Open => self.open_reader().await,
            Replay::Rebuild => {
                self.reader
                    .as_mut()
                    .unwrap()
                    .rebuild_state_from_file(&self.cx)
                    .await
            }
            Replay::Refresh => self.reader.as_mut().unwrap().refresh(&self.cx).await,
        }
    }

    fn frame_offset(&self, index: usize) -> u64 {
        u64::try_from(WAL_HEADER_SIZE + index * self.writer.frame_size()).unwrap()
    }

    async fn flip(&self, frame: usize, within: usize) {
        let offset = self.frame_offset(frame) + u64::try_from(within).unwrap();
        let mut byte = [0];
        assert_eq!(
            self.writer
                .file()
                .read(&self.cx, &mut byte, offset)
                .await
                .unwrap(),
            1
        );
        byte[0] ^= 0x80;
        self.writer
            .file()
            .write(&self.cx, &byte, offset)
            .await
            .unwrap();
    }

    async fn assert_prefix(&self, frames: usize) {
        let reader = self.reader.as_ref().unwrap();
        assert_eq!(reader.frame_count(), frames);
        let expected_commit = if frames == 0 {
            None
        } else {
            Some((
                frames - 1,
                self.writer
                    .read_frame_header(&self.cx, frames - 1)
                    .await
                    .unwrap(),
            ))
        };
        assert_eq!(reader.last_commit_frame_header(), expected_commit);
        assert_eq!(
            reader.running_checksum(),
            expected_commit.map_or(self.writer.header().checksum, |(_, header)| header.checksum)
        );
        assert_eq!(self.audit.mutations.load(Ordering::SeqCst), 0);
    }
}

fn prepare_suffix<F: VfsFile>(
    wal: &WalFile<F>,
    frames: usize,
    page: &[u8],
    commit_last: bool,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut transforms = Vec::new();
    wal.prepare_frame_bytes_with_transforms_into(
        frames,
        (0..frames).map(|index| WalAppendFrameRef {
            page_number: 7,
            page_data: page,
            db_size_if_commit: if commit_last && index + 1 == frames {
                7
            } else {
                0
            },
        }),
        &mut bytes,
        &mut transforms,
    )
    .unwrap();
    wal.finalize_prepared_frame_bytes(&mut bytes, &transforms)
        .unwrap();
    bytes
}

#[test]
fn replay_batches_io_but_reads_every_frame_byte() {
    run(async {
        for mode in REPLAYS {
            for page_size in [512, PAGE_SIZE, 65_536] {
                let frames = 257;
                let mut fixture = Fixture::new(mode, page_size, frames, frames).await;
                fixture.replay(mode).await.unwrap();
                fixture.assert_prefix(frames).await;
                let reads = fixture.audit.data_reads();
                let mut next_offset = u64::try_from(WAL_HEADER_SIZE).unwrap();
                for read in &reads {
                    assert_eq!(read.offset, next_offset, "{mode:?}/{page_size}: {reads:?}");
                    assert_eq!(read.returned, read.requested);
                    assert!(read.requested <= READ_BUDGET.max(fixture.writer.frame_size()));
                    next_offset += u64::try_from(read.returned).unwrap();
                }
                assert_eq!(next_offset, fixture.frame_offset(frames));
                if fixture.writer.frame_size() < READ_BUDGET {
                    assert!(
                        reads.len() < frames / 2,
                        "unbatched replay: {mode:?}/{page_size}"
                    );
                }
            }
        }
    });
}

#[test]
fn append_during_batched_replay_waits_for_the_next_sampled_horizon() {
    run(async {
        for mode in REPLAYS {
            let original = 32;
            let appended = 18;
            let mut fixture = Fixture::new(mode, PAGE_SIZE, original, original).await;
            let original_end = fixture.frame_offset(original);
            let page = vec![0xD7; usize::try_from(PAGE_SIZE).unwrap()];
            let bytes = prepare_suffix(&fixture.writer, appended, &page, true);
            *fixture.audit.append_on_data_read.lock().unwrap() = Some(PeerAppend {
                file: open_file(&fixture.vfs, &fixture.cx),
                offset: original_end,
                bytes,
            });

            fixture.replay(mode).await.unwrap();
            fixture.assert_prefix(original).await;
            assert!(fixture.audit.append_on_data_read.lock().unwrap().is_none());
            assert_eq!(
                fixture.writer.file().file_size(&fixture.cx).unwrap(),
                fixture.frame_offset(original + appended)
            );
            let reads = fixture.audit.data_reads();
            assert!(reads[0].requested > fixture.writer.frame_size());
            assert!(reads.iter().all(|read| {
                read.offset + u64::try_from(read.requested).unwrap() <= original_end
            }));

            fixture
                .reader
                .as_mut()
                .unwrap()
                .refresh(&fixture.cx)
                .await
                .unwrap();
            fixture.writer.refresh(&fixture.cx).await.unwrap();
            fixture.assert_prefix(original + appended).await;
            let (_, actual) = fixture
                .reader
                .as_ref()
                .unwrap()
                .read_frame(&fixture.cx, original + appended - 1)
                .await
                .unwrap();
            assert_eq!(actual, page);
        }
    });
}

#[test]
fn corruption_at_batch_edges_stops_at_the_previous_commit() {
    run(async {
        for mode in REPLAYS {
            for bad_frame in [14, 15, 16, 29, 30, 31] {
                for within in [WAL_FRAME_HEADER_SIZE + 137, 8, 16] {
                    let mut fixture = Fixture::new(mode, PAGE_SIZE, 45, 5).await;
                    fixture.flip(bad_frame, within).await;
                    let size = fixture.writer.file().file_size(&fixture.cx).unwrap();
                    fixture.replay(mode).await.unwrap();
                    fixture.assert_prefix((bad_frame / 5) * 5).await;
                    assert_eq!(fixture.writer.file().file_size(&fixture.cx).unwrap(), size);
                }
            }
        }
    });
}

#[test]
fn short_batch_read_keeps_complete_commits_before_the_torn_frame() {
    run(async {
        for mode in REPLAYS {
            for complete in [7, 22, 37] {
                for partial in [0, 1, 23, 24, 4119] {
                    let mut fixture = Fixture::new(mode, PAGE_SIZE, 45, 3).await;
                    fixture.audit.fault.lock().unwrap().visible_end =
                        Some(fixture.frame_offset(complete) + partial);
                    fixture.replay(mode).await.unwrap();
                    fixture.assert_prefix((complete / 3) * 3).await;
                    assert!(
                        fixture
                            .audit
                            .data_reads()
                            .iter()
                            .any(|read| read.returned < read.requested)
                    );
                }
            }
        }
    });
}

#[test]
fn capped_reads_resume_at_the_first_unconsumed_frame() {
    run(async {
        for mode in REPLAYS {
            let frame_size = usize::try_from(PAGE_SIZE).unwrap() + WAL_FRAME_HEADER_SIZE;
            for cap in [
                frame_size,
                frame_size + 17,
                frame_size * 2,
                frame_size * 2 + 17,
            ] {
                let mut fixture = Fixture::new(mode, PAGE_SIZE, 45, 5).await;
                fixture.audit.fault.lock().unwrap().max_read = Some(cap);
                fixture.replay(mode).await.unwrap();
                fixture.assert_prefix(45).await;

                let mut next_offset = u64::try_from(WAL_HEADER_SIZE).unwrap();
                for read in fixture.audit.data_reads() {
                    assert_eq!(read.offset, next_offset, "{mode:?}/{cap}");
                    assert!(read.returned <= cap);
                    // Partial fragments may be reread, but no complete frame
                    // may be skipped or accepted twice.
                    next_offset +=
                        u64::try_from((read.returned / frame_size) * frame_size).unwrap();
                }
                assert_eq!(next_offset, fixture.frame_offset(45));
            }
        }
    });
}

#[test]
fn read_errors_are_not_accepted_as_a_recoverable_tail() {
    run(async {
        for mode in REPLAYS {
            let mut fixture = Fixture::new(mode, PAGE_SIZE, 45, 3).await;
            if mode == Replay::Rebuild {
                assert_eq!(
                    fixture.reader.as_ref().unwrap().last_fsynced_frame_count(),
                    45
                );
            }
            fixture.audit.fault.lock().unwrap().fail_from = Some(fixture.frame_offset(17));
            assert!(matches!(
                fixture.replay(mode).await,
                Err(FrankenError::Io(_))
            ));
            if mode == Replay::Rebuild {
                assert_eq!(
                    fixture.reader.as_ref().unwrap().last_fsynced_frame_count(),
                    0
                );
            }
            assert_eq!(fixture.audit.mutations.load(Ordering::SeqCst), 0);
            *fixture.audit.fault.lock().unwrap() = ReadFault::default();
            fixture.replay(mode).await.unwrap();
            fixture.assert_prefix(45).await;
        }
    });
}

#[test]
fn checksum_valid_uncommitted_suffix_crossing_a_batch_is_not_published() {
    run(async {
        for mode in REPLAYS {
            let mut fixture = Fixture::new(mode, PAGE_SIZE, 34, 16).await;
            fixture.replay(mode).await.unwrap();
            fixture.assert_prefix(32).await;
            let page = vec![0xC3; usize::try_from(PAGE_SIZE).unwrap()];
            fixture
                .writer
                .append_frame(&fixture.cx, 7, &page, 7)
                .await
                .unwrap();
            fixture
                .reader
                .as_mut()
                .unwrap()
                .refresh(&fixture.cx)
                .await
                .unwrap();
            fixture.assert_prefix(35).await;
        }
    });
}

#[test]
fn equal_length_same_salt_reset_revalidates_across_batches() {
    run(async {
        let mut fixture = Fixture::new(Replay::Rebuild, PAGE_SIZE, 45, 5).await;
        let before = fixture.reader.as_ref().unwrap().generation_identity();
        fixture
            .writer
            .reset(&fixture.cx, 18, SALTS, true)
            .await
            .unwrap();
        fixture.append(45, 5, 0x93).await;
        fixture
            .reader
            .as_mut()
            .unwrap()
            .refresh(&fixture.cx)
            .await
            .unwrap();
        fixture.assert_prefix(45).await;
        let reader = fixture.reader.as_ref().unwrap();
        assert_ne!(reader.generation_identity(), before);
        assert_eq!(reader.generation_identity().salts, before.salts);
        let (_, page) = reader.read_frame(&fixture.cx, 17).await.unwrap();
        assert_eq!(page, vec![0xA4; usize::try_from(PAGE_SIZE).unwrap()]);
        assert!(fixture.audit.data_reads().iter().any(|read| {
            read.offset == u64::try_from(WAL_HEADER_SIZE).unwrap()
                && read.requested > fixture.writer.frame_size()
        }));
    });
}

#[test]
fn full_recovery_rejects_middle_payload_damage_despite_unchanged_tail() {
    run(async {
        let mut fixture = Fixture::new(Replay::Rebuild, PAGE_SIZE, 45, 5).await;
        assert_eq!(
            fixture.reader.as_ref().unwrap().last_fsynced_frame_count(),
            45
        );
        let header = *fixture.writer.header();
        let tail = fixture
            .writer
            .read_frame_header(&fixture.cx, 44)
            .await
            .unwrap();
        fixture.flip(17, WAL_FRAME_HEADER_SIZE + 99).await;
        // Refresh is deliberately incremental, not a replacement for explicit
        // recovery. Same header, length, and tail do not prove the middle bytes.
        fixture
            .reader
            .as_mut()
            .unwrap()
            .refresh(&fixture.cx)
            .await
            .unwrap();
        fixture.assert_prefix(45).await;
        assert_eq!(*fixture.reader.as_ref().unwrap().header(), header);
        assert_eq!(
            fixture
                .reader
                .as_ref()
                .unwrap()
                .read_frame_header(&fixture.cx, 44)
                .await
                .unwrap(),
            tail
        );

        fixture.replay(Replay::Rebuild).await.unwrap();
        fixture.assert_prefix(15).await;
        assert_eq!(
            fixture.reader.as_ref().unwrap().last_fsynced_frame_count(),
            0
        );
    });
}

#[test]
fn retained_uncommitted_prefix_requires_every_byte_across_batch_boundaries() {
    run(async {
        let mut fixture = Fixture::new(Replay::Refresh, PAGE_SIZE, 0, 0).await;
        let page = vec![0xB6; usize::try_from(PAGE_SIZE).unwrap()];
        let reader = fixture.reader.as_mut().unwrap();
        for _ in 0..22 {
            reader.append_frame(&fixture.cx, 1, &page, 0).await.unwrap();
        }
        let generation = reader.generation_identity();
        let checksum = reader.running_checksum();
        let suffix = prepare_suffix(reader, 12, &page, false);
        fixture
            .writer
            .file()
            .write(&fixture.cx, &suffix, fixture.frame_offset(22))
            .await
            .unwrap();
        fixture.audit.mutations.store(0, Ordering::SeqCst);

        fixture.audit.fault.lock().unwrap().visible_end = Some(fixture.frame_offset(21) + 17);
        assert!(matches!(
            fixture
                .reader
                .as_mut()
                .unwrap()
                .refresh_preserving_append_prefix(&fixture.cx, generation, 22, checksum)
                .await,
            Err(FrankenError::WalCorrupt { .. })
        ));
        assert_eq!(fixture.reader.as_ref().unwrap().frame_count(), 22);
        assert_eq!(
            fixture.reader.as_ref().unwrap().running_checksum(),
            checksum
        );

        fixture.audit.fault.lock().unwrap().visible_end = Some(fixture.frame_offset(25) + 17);
        fixture
            .reader
            .as_mut()
            .unwrap()
            .refresh_preserving_append_prefix(&fixture.cx, generation, 22, checksum)
            .await
            .unwrap();
        let reader = fixture.reader.as_ref().unwrap();
        assert_eq!(reader.frame_count(), 22);
        assert_eq!(reader.running_checksum(), checksum);
        assert_eq!(reader.last_commit_frame_header(), None);
        assert_eq!(fixture.audit.mutations.load(Ordering::SeqCst), 0);
    });
}
