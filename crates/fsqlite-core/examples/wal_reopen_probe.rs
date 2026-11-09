//! Native-file probe for GH #598. Keep fixture creation and instrumented I/O
//! diagnostics separate from timed runs. Build this identical example against
//! both revisions, then alternate the binaries on the same immutable fixture.
//!
//! `wal_reopen_probe create PATH FRAMES` creates a new checksum-valid WAL.
//! `wal_reopen_probe open PATH FRAMES ITERATIONS CHECKSUM1 CHECKSUM2` reopens it.
//! `publication` in place of `open` also materializes the adapter's page index.
//! Output is JSON; a parent process can measure each run's child CPU usage.

#[cfg(all(unix, feature = "native"))]
mod native {
    use std::path::Path;
    use std::time::Instant;

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_core::wal_adapter::WalBackendAdapter;
    use fsqlite_types::cx::Cx;
    use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
    use fsqlite_vfs::{UnixVfs, Vfs, VfsFile};
    use fsqlite_wal::wal::WalAppendFrameRef;
    use fsqlite_wal::{SqliteWalChecksum, WalFile, WalSalts};

    const PAGE_SIZE: u32 = 4096;
    const INDEX_PAGES: usize = 4096;

    pub fn run() {
        let args: Vec<_> = std::env::args().collect();
        assert!(
            args.len() == 4 || args.len() == 7,
            "usage: wal_reopen_probe create PATH FRAMES | open/publication PATH FRAMES ITERATIONS CHECKSUM1 CHECKSUM2"
        );
        let mode = &args[1];
        let path = Path::new(&args[2]);
        let frames: usize = args[3].parse().expect("frame count");
        assert!(matches!(mode.as_str(), "create" | "open" | "publication"));
        let runtime = RuntimeBuilder::current_thread()
            .blocking_threads(1, 2)
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let cx = Cx::default();
            let vfs = UnixVfs::new();
            if mode == "create" {
                assert_eq!(args.len(), 4);
                assert!(!path.exists(), "fixture must be new");
                let (file, _) = vfs
                    .open(
                        &cx,
                        Some(path),
                        VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
                    )
                    .expect("create native WAL");
                let mut wal = WalFile::create(
                    &cx,
                    file,
                    PAGE_SIZE,
                    17,
                    WalSalts {
                        salt1: 0x73A6_C921,
                        salt2: 0x28D5_B407,
                    },
                )
                .await
                .expect("WAL header");
                let page = vec![0x5A; usize::try_from(PAGE_SIZE).expect("page size")];
                // Allocate only small frame descriptors, with one shared page.
                // The WAL writer itself assembles bounded serialized chunks.
                let batch: Vec<_> = (0..frames)
                    .map(|index| WalAppendFrameRef {
                        page_number: u32::try_from(index % INDEX_PAGES + 1).expect("page number"),
                        page_data: page.as_slice(),
                        db_size_if_commit: if index + 1 == frames {
                            u32::try_from(INDEX_PAGES).expect("database pages")
                        } else {
                            0
                        },
                    })
                    .collect();
                wal.append_frames(&cx, &batch).await.expect("write fixture");
                wal.sync(&cx, SyncFlags::NORMAL).expect("sync fixture");
                let checksum = wal.running_checksum();
                let bytes = wal.file().file_size(&cx).expect("fixture size");
                println!(
                    "{{\"mode\":\"create\",\"frames\":{frames},\"bytes\":{bytes},\"page_size\":{PAGE_SIZE},\"index_pages\":{INDEX_PAGES},\"checksum1\":{},\"checksum2\":{}}}",
                    checksum.s1, checksum.s2
                );
                return;
            }

            assert_eq!(args.len(), 7);
            let iterations: usize = args[4].parse().expect("iterations");
            assert!(iterations > 0);
            let expected_checksum = SqliteWalChecksum {
                s1: args[5].parse().expect("checksum1"),
                s2: args[6].parse().expect("checksum2"),
            };
            let mut open_ns = Vec::with_capacity(iterations);
            let mut publication_ns = Vec::with_capacity(iterations);
            let mut total_ns = Vec::with_capacity(iterations);
            for _ in 0..iterations {
                let start = Instant::now();
                let (file, _) = vfs
                    .open(&cx, Some(path), VfsOpenFlags::READONLY | VfsOpenFlags::WAL)
                    .expect("open native WAL");
                let wal = WalFile::open(&cx, file).await.expect("replay WAL");
                let opened = start.elapsed().as_nanos();
                assert_eq!(wal.frame_count(), frames);
                assert_eq!(wal.running_checksum(), expected_checksum);
                if mode == "publication" {
                    let mut adapter = WalBackendAdapter::new(wal);
                    let publication = Instant::now();
                    let snapshot = adapter
                        .refresh_published_snapshot(&cx)
                        .await
                        .expect("publish index");
                    publication_ns.push(publication.elapsed().as_nanos());
                    assert_eq!(snapshot.last_commit_frame, frames.checked_sub(1));
                    assert_eq!(snapshot.commit_count, u64::from(frames != 0));
                    assert_eq!(snapshot.latest_frame_entries, frames.min(INDEX_PAGES));
                    assert!(!snapshot.index_is_partial);
                    std::hint::black_box(&adapter);
                } else {
                    std::hint::black_box(&wal);
                }
                open_ns.push(opened);
                total_ns.push(start.elapsed().as_nanos());
            }
            println!(
                "{{\"mode\":\"{mode}\",\"frames\":{frames},\"iterations\":{iterations},\"open_ns\":{open_ns:?},\"publication_ns\":{publication_ns:?},\"total_ns\":{total_ns:?}}}"
            );
        });
    }
}

fn main() {
    #[cfg(all(unix, feature = "native"))]
    native::run();
    #[cfg(not(all(unix, feature = "native")))]
    eprintln!("wal_reopen_probe requires Unix and the native feature");
}
