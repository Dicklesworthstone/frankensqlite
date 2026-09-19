//! Native recovery regression suite, moved with the implementation from the CLI.
use super::*;
use std::io::Cursor;
use fsqlite_types::{ObjectId, Oti};
use crate::{
    SqliteWalChecksum, WAL_FORMAT_VERSION, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
    WAL_MAGIC_LE, WalFecGroupMeta, WalFecGroupMetaInit, WalFecGroupRecord,
    WalFrameHeader, WalHeader, WalSalts, append_wal_fec_group,
    build_source_page_hashes, generate_wal_fec_repair_symbols,
};
use crate::checksum::WalChecksumTransform;

const PAGE_SIZE: usize = 512;
const FRAME_SIZE: usize = PAGE_SIZE + WAL_FRAME_HEADER_SIZE;
const SOURCE_ID: [u8; 16] = [0x6d; 16];

fn empty_database(version: u32) -> Vec<u8> {
    let mut page = vec![0_u8; PAGE_SIZE];
    page[..16].copy_from_slice(b"SQLite format 3\0");
    page[16..18].copy_from_slice(&512_u16.to_be_bytes());
    page[18..20].copy_from_slice(&[2, 2]);
    page[21..24].copy_from_slice(&[64, 32, 32]);
    page[24..28].copy_from_slice(&7_u32.to_be_bytes());
    page[28..32].copy_from_slice(&1_u32.to_be_bytes());
    page[44..48].copy_from_slice(&4_u32.to_be_bytes());
    page[56..60].copy_from_slice(&1_u32.to_be_bytes());
    page[60..64].copy_from_slice(&version.to_be_bytes());
    page[92..96].copy_from_slice(&7_u32.to_be_bytes());
    page[100] = 13;
    page[105..107].copy_from_slice(&512_u16.to_be_bytes());
    page
}

fn fixture(repairs: u32, corrupt_frames: &[usize]) -> (Options, Vec<u8>) {
    fixture_with_identity(repairs, corrupt_frames, [0; 16])
}

fn fixture_with_identity(repairs: u32, corrupt_frames: &[usize], identity: [u8; 16]) -> (Options, Vec<u8>) {
    let directory = tempfile::tempdir().unwrap().keep();
    let options = Options::new(directory.join("source.db"), directory.join("output.db"));
    let page_with_identity = |version| {
        let mut page = empty_database(version);
        page[76..92].copy_from_slice(&identity);
        page
    };
    host_fs::write(&options.source, page_with_identity(0)).unwrap();
    let pages: Vec<_> = (1..=4).map(page_with_identity).collect();
    let header = WalHeader {
        magic: WAL_MAGIC_LE, format_version: WAL_FORMAT_VERSION, page_size: 512,
        checkpoint_seq: 1, salts: WalSalts { salt1: 123, salt2: 456 },
        checksum: SqliteWalChecksum::default(),
    };
    let mut wal = header.to_bytes().unwrap().to_vec();
    let mut running = WalHeader::from_bytes(&wal).unwrap().checksum;
    for (index, page) in pages.iter().enumerate() {
        let start = wal.len();
        wal.extend_from_slice(&WalFrameHeader {
            page_number: 1, db_size: u32::from(index == 3), salts: header.salts,
            checksum: SqliteWalChecksum::default(),
        }.to_bytes());
        wal.extend_from_slice(page);
        running = WalChecksumTransform::for_wal_frame(&wal[start..], PAGE_SIZE, false).unwrap().apply(running);
        wal[start + 16..start + 20].copy_from_slice(&running.s1.to_be_bytes());
        wal[start + 20..start + 24].copy_from_slice(&running.s2.to_be_bytes());
    }
    let meta = WalFecGroupMeta::from_init(WalFecGroupMetaInit {
        wal_salt1: 123, wal_salt2: 456, start_frame_no: 1, end_frame_no: 4,
        db_size_pages: 1, page_size: 512, k_source: 4, r_repair: repairs,
        oti: Oti { f: 2048, al: 1, t: 512, z: 1, n: 1 },
        object_id: ObjectId::derive_from_canonical_bytes(b"native-recovery-export"),
        page_numbers: vec![1; 4], source_page_xxh3_128: build_source_page_hashes(&pages),
    }).unwrap();
    let symbols = generate_wal_fec_repair_symbols(&meta, &pages).unwrap();
    append_wal_fec_group(&companion(&options.source, "-wal-fec"),
        &WalFecGroupRecord::new(meta, symbols).unwrap()).unwrap();
    for frame in corrupt_frames {
        wal[WAL_HEADER_SIZE + frame * FRAME_SIZE + WAL_FRAME_HEADER_SIZE + 60] ^= 0xff;
    }
    host_fs::write(&companion(&options.source, "-wal"), wal).unwrap();
    (options, pages.last().unwrap().clone())
}

fn run_test<F: std::future::Future>(future: F) -> F::Output {
    RuntimeBuilder::current_thread().blocking_threads(1, 2).build().unwrap().block_on(future)
}

#[test]
fn native_export_repairs_real_fec_without_changing_source_data() {
    run_test(async {
        let (options, expected) = fixture(8, &[1]);
        let originals: Vec<_> = ["", "-wal", "-wal-fec"].into_iter().map(|suffix| {
            let path = companion(&options.source, suffix);
            let bytes = host_fs::read(&path).unwrap();
            (path, bytes)
        }).collect();
        let cx = request_context().unwrap();
        let report = export_database(&cx, &options).await.unwrap();
        assert_eq!(report.wal_frames, 4);
        assert_eq!(report.repaired_frames, 1);
        assert_eq!(report.pages, 1);
        assert_eq!(host_fs::read(&options.destination).unwrap(), expected);
        assert_eq!(report.digest, blake3::hash(&expected));
        for (path, bytes) in originals { assert_eq!(host_fs::read(&path).unwrap(), bytes); }
    });
}

#[test]
fn native_export_recovers_an_erased_commit_header() {
    run_test(async {
        let (options, expected) = fixture(8, &[1]);
        let wal_path = companion(&options.source, "-wal");
        let mut damaged = host_fs::read(&wal_path).unwrap();
        let terminal = WAL_HEADER_SIZE + 3 * FRAME_SIZE;
        damaged[terminal..terminal + 16].fill(0);
        host_fs::write(&wal_path, &damaged).unwrap();
        let main_before = host_fs::read(&options.source).unwrap();
        let sidecar = companion(&options.source, "-wal-fec");
        let sidecar_before = host_fs::read(&sidecar).unwrap();
        let cx = request_context().unwrap();
        let report = export_database(&cx, &options).await.unwrap();
        assert_eq!(report.wal_frames, 4);
        assert_eq!(report.repaired_frames, 2);
        assert_eq!(host_fs::read(&options.destination).unwrap(), expected);
        assert_eq!(report.digest, blake3::hash(&expected));
        assert_eq!(host_fs::read(&wal_path).unwrap(), damaged);
        assert_eq!(host_fs::read(&options.source).unwrap(), main_before);
        assert_eq!(host_fs::read(&sidecar).unwrap(), sidecar_before);
    });
}

#[test]
fn native_export_uses_a_later_commit_to_anchor_a_repaired_terminal() {
    run_test(async {
        let (options, _) = fixture(8, &[1]);
        let wal_path = companion(&options.source, "-wal");
        let mut damaged = host_fs::read(&wal_path).unwrap();
        let terminal = WAL_HEADER_SIZE + 3 * FRAME_SIZE;
        let previous = WalFrameHeader::from_bytes(&damaged[terminal..]).unwrap().checksum;
        let header = WalHeader::from_bytes(&damaged).unwrap();
        let expected = empty_database(99);
        let mut successor = WalFrameHeader {
            page_number: 1, db_size: 1, salts: header.salts, checksum: SqliteWalChecksum::default(),
        }.to_bytes().to_vec();
        successor.extend_from_slice(&expected);
        let checksum = WalChecksumTransform::for_wal_frame(&successor, PAGE_SIZE, header.big_endian_checksum())
            .unwrap().apply(previous);
        successor[16..20].copy_from_slice(&checksum.s1.to_be_bytes());
        successor[20..24].copy_from_slice(&checksum.s2.to_be_bytes());
        damaged.extend_from_slice(&successor);
        damaged[terminal..terminal + WAL_FRAME_HEADER_SIZE].fill(0);
        host_fs::write(&wal_path, &damaged).unwrap();
        let cx = request_context().unwrap();
        let report = export_database(&cx, &options).await.unwrap();
        assert_eq!(report.wal_frames, 5);
        assert_eq!(report.repaired_frames, 2);
        assert_eq!(host_fs::read(&options.destination).unwrap(), expected);
        assert_eq!(report.digest, blake3::hash(&expected));
        assert_eq!(host_fs::read(&wal_path).unwrap(), damaged);
    });
}

#[test]
fn native_export_never_publishes_an_unanchored_reconstruction() {
    run_test(async {
        let (options, _) = fixture(8, &[1]);
        let wal_path = companion(&options.source, "-wal");
        let mut damaged = host_fs::read(&wal_path).unwrap();
        damaged[WAL_HEADER_SIZE + 3 * FRAME_SIZE + 16] ^= 1;
        host_fs::write(&wal_path, &damaged).unwrap();
        let cx = request_context().unwrap();
        assert!(export_database(&cx, &options).await.is_err());
        assert!(!NativeVfs::new().path_entry_exists(&cx, &options.destination).unwrap());
        assert_eq!(host_fs::read(&wal_path).unwrap(), damaged);
    });
}

#[test]
fn failed_recovery_and_input_budget_do_not_create_output() {
    run_test(async {
        let (mut options, _) = fixture(2, &[0, 1, 2]);
        let cx = request_context().unwrap();
        let vfs = NativeVfs::new();
        assert!(export_database(&cx, &options).await.is_err());
        assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
        options.replay.max_wal_bytes = 31;
        assert!(matches!(export_database(&cx, &options).await, Err(FrankenError::TooBig)));
        assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
    });
}

#[test]
fn output_and_stale_sidecars_are_never_overwritten() {
    run_test(async {
        let (mut options, _) = fixture(8, &[]);
        let cx = request_context().unwrap();
        let vfs = NativeVfs::new();
        host_fs::write(&options.destination, b"existing owner").unwrap();
        assert!(export_database(&cx, &options).await.is_err());
        assert_eq!(host_fs::read(&options.destination).unwrap(), b"existing owner");
        options.destination = options.destination.with_file_name("other.db");
        let stale = companion(&options.destination, "-wal");
        host_fs::write(&stale, b"stale committed bytes").unwrap();
        assert!(export_database(&cx, &options).await.is_err());
        assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
        assert_eq!(host_fs::read(&stale).unwrap(), b"stale committed bytes");
    });
}

#[test]
fn source_lock_contention_refuses_then_recovers_after_release() {
    run_test(async {
        let (options, expected) = fixture(8, &[1]);
        let cx = request_context().unwrap();
        let vfs = NativeVfs::new();
        let (file, _) = vfs.open(&cx, Some(&options.source), VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB).unwrap();
        let mut owner = SourceFile::new(file, &cx);
        owner.acquire_recovery(&cx).unwrap();
        assert!(export_database(&cx, &options).await.is_err());
        assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
        owner.finish().unwrap();
        export_database(&cx, &options).await.unwrap();
        assert_eq!(host_fs::read(&options.destination).unwrap(), expected);
    });
}

#[test]
fn sidecar_writer_contention_does_not_leak_source_recovery_locks() {
    run_test(async {
        let (options, _) = fixture(8, &[1]);
        let cx = request_context().unwrap();
        let guard = host_fs::open_existing_regular_file_no_follow(&companion(&options.source, "-wal-fec.lock")).unwrap();
        guard.try_lock().unwrap();
        assert!(matches!(export_database(&cx, &options).await, Err(FrankenError::Busy)));
        assert!(!NativeVfs::new().path_entry_exists(&cx, &options.destination).unwrap());
        drop(guard);
        export_database(&cx, &options).await.unwrap();
    });
}

#[test]
fn export_header_is_published_only_after_body_sync() {
    let cx = Cx::new();
    let image = empty_database(17);
    let mut file = Cursor::new(Vec::new());
    let mut synced = Vec::new();
    write_image(&mut file, &cx, &image, |file| { synced.push(file.get_ref().clone()); Ok(()) }).unwrap();
    assert_eq!(synced.len(), 2);
    assert_eq!(&synced[0][..100], &[0; 100]);
    assert_eq!(&synced[0][100..], &image[100..]);
    assert_eq!(synced[1], image);
}

#[test]
fn failed_body_sync_never_writes_the_valid_header() {
    let mut file = Cursor::new(Vec::new());
    assert!(write_image(&mut file, &Cx::new(), &empty_database(17), |_| {
        Err(io::Error::other("injected body sync failure"))
    }).is_err());
    assert_eq!(&file.get_ref()[..100], &[0; 100]);
}

#[test]
fn bounded_reader_refuses_short_and_growing_snapshots() {
    let cx = Cx::new();
    assert!(read_exact_snapshot(&mut Cursor::new(vec![1; 3]), &cx, 4).is_err());
    assert!(read_exact_snapshot(&mut Cursor::new(vec![1; 5]), &cx, 4).is_err());
    assert_eq!(read_exact_snapshot(&mut Cursor::new(vec![1; 4]), &cx, 4).unwrap(), vec![1; 4]);
    cx.cancel();
    assert!(matches!(read_exact_snapshot(&mut Cursor::new(vec![]), &cx, 0), Err(FrankenError::Interrupt)));
}

#[test]
fn options_preserve_literal_paths_and_bound_resources() {
    let options = Options::new("-a", "-b");
    assert_eq!(options.source, Path::new("-a"));
    assert_eq!(options.destination, Path::new("-b"));
    assert_eq!(options.max_database_bytes, 256 * 1024 * 1024);
    assert_eq!(options.replay.max_wal_bytes, 64 * 1024 * 1024);
    assert_eq!(options.replay.max_certificate_bytes, 32 * 1024 * 1024);
}

#[test]
fn public_recovery_requires_caller_context_before_admission() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let options = Options::new("missing-source", "never-created");
    let cx = Cx::new();
    let mut future = std::pin::pin!(export_database(&cx, &options));
    let mut task = Context::from_waker(Waker::noop());
    assert!(matches!(future.as_mut().poll(&mut task),
        Poll::Ready(Err(FrankenError::BackgroundWorkerFailed(_)))));
}

#[test]
fn source_mode_guard_rejects_stale_wal_pairings() {
    let database = empty_database(19);
    require_wal_mode(&database).unwrap();
    for modes in [[1, 1], [1, 2], [2, 1], [0, 0], [3, 3]] {
        let mut invalid = database.clone();
        invalid[18..20].copy_from_slice(&modes);
        assert!(require_wal_mode(&invalid).is_err());
    }
    assert!(require_wal_mode(&database[..19]).is_err());
    let mut invalid = database;
    invalid[0] = 0;
    assert!(require_wal_mode(&invalid).is_err());
}

#[test]
fn cancelled_export_does_not_reserve_destination() {
    run_test(async {
        let (options, _) = fixture(8, &[1]);
        let cx = request_context().unwrap();
        cx.cancel();
        assert!(export_database(&cx, &options).await.is_err());
        assert!(!NativeVfs::new().path_entry_exists(&Cx::new(), &options.destination).unwrap());
    });
}

#[test]
fn final_sync_failure_does_not_return_a_success_receipt() {
    let mut file = Cursor::new(Vec::new());
    let mut calls = 0;
    let image = empty_database(19);
    assert!(write_image(&mut file, &Cx::new(), &image, |_| {
        calls += 1;
        if calls == 2 { Err(io::Error::other("injected final sync failure")) } else { Ok(()) }
    }).is_err());
    assert_eq!(calls, 2);
    assert_eq!(file.into_inner(), image);
}

#[test]
fn readback_rejects_changed_bytes_and_unexpected_tail() {
    let cx = Cx::new();
    let image = empty_database(19);
    assert_eq!(verify_image(&mut Cursor::new(image.clone()), &cx, &image).unwrap(), blake3::hash(&image));
    let mut changed = image.clone();
    changed[200] ^= 1;
    assert!(verify_image(&mut Cursor::new(changed), &cx, &image).is_err());
    let mut grown = image.clone();
    grown.push(0);
    assert!(verify_image(&mut Cursor::new(grown), &cx, &image).is_err());
}

#[cfg(unix)]
#[test]
fn dangling_destination_and_source_alias_are_refused() {
    use std::os::unix::fs::symlink;
    run_test(async {
        let (mut options, _) = fixture(8, &[]);
        let original = host_fs::read(&options.source).unwrap();
        let cx = request_context().unwrap();
        let missing = options.destination.with_file_name("absent.db");
        symlink(&missing, &options.destination).unwrap();
        assert!(export_database(&cx, &options).await.is_err());
        assert_eq!(host_fs::read_link(&options.destination).unwrap(), missing);
        options.destination = options.destination.with_file_name("alias.db");
        symlink(&options.source, &options.destination).unwrap();
        assert!(export_database(&cx, &options).await.is_err());
        assert_eq!(host_fs::read(&options.source).unwrap(), original);
    });
}

#[cfg(unix)]
#[test]
fn native_in_place_repair_preserves_original_backup_and_publishes_index() {
    use crate::wal_index::{WAL_SHM_SEGMENT_BYTES, read_shared_wal_index_header, validate_shared_wal_index_wal_binding};
    run_test(async {
        let (options, _) = fixture(8, &[1]);
        let wal_path = companion(&options.source, "-wal");
        let original = host_fs::read(&wal_path).unwrap();
        let main_before = host_fs::read(&options.source).unwrap();
        let sidecar_path = companion(&options.source, "-wal-fec");
        let sidecar = host_fs::read(&sidecar_path).unwrap();
        let expected = crate::wal_fec::replay::recover_wal_fec_image(&original, &sidecar, options.replay)
            .unwrap().complete_image().unwrap().into_owned();
        let cx = request_context().unwrap();
        let vfs = NativeVfs::new();
        let (file, _) = vfs.open(&cx, Some(&options.source), VfsOpenFlags::READWRITE | VfsOpenFlags::MAIN_DB).unwrap();
        let mut observer = SourceFile::new(file, &cx);
        let region = observer.file.shm_map(&cx, 0, u32::try_from(WAL_SHM_SEGMENT_BYTES).unwrap(), true).unwrap();
        let (wal_observer, _) = vfs.open(&cx, Some(&wal_path), VfsOpenFlags::READONLY | VfsOpenFlags::WAL).unwrap();
        let mut wal_observer = SourceFile::new(wal_observer, &cx);
        let identity = wal_observer.file.file_identity().unwrap();
        let report = repair_wal(&cx, &options).await.unwrap();
        assert!(report.repaired_in_place);
        assert_eq!(report.repaired_frames, 1);
        assert_eq!(host_fs::read(&options.destination).unwrap(), original);
        assert_eq!(host_fs::read(&wal_path).unwrap(), expected);
        assert_eq!(report.digest, blake3::hash(&expected));
        assert_eq!(host_fs::read(&options.source).unwrap(), main_before);
        assert_eq!(host_fs::read(&sidecar_path).unwrap(), sidecar);
        assert_eq!(wal_observer.file.file_identity().unwrap(), identity);
        let index = read_shared_wal_index_header(&region).unwrap().unwrap();
        assert_eq!(index.mx_frame, 4);
        assert_eq!(index.n_page, 1);
        let header = WalHeader::from_bytes(&expected).unwrap();
        let terminal = WalFrameHeader::from_bytes(&expected[expected.len() - FRAME_SIZE..]).unwrap();
        validate_shared_wal_index_wal_binding(&index, &header, Some((4, terminal))).unwrap();
        drop(region);
        wal_observer.finish().unwrap();
        observer.finish().unwrap();
    });
}

#[cfg(unix)]
#[test]
fn native_in_place_refusals_never_change_wal_or_overwrite_backup() {
    run_test(async {
        let (mut options, _) = fixture(2, &[0, 1, 2]);
        let wal_path = companion(&options.source, "-wal");
        let original = host_fs::read(&wal_path).unwrap();
        let cx = request_context().unwrap();
        let vfs = NativeVfs::new();
        assert!(repair_wal(&cx, &options).await.is_err());
        assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
        assert_eq!(host_fs::read(&wal_path).unwrap(), original);
        host_fs::write(&options.destination, b"existing backup").unwrap();
        assert!(repair_wal(&cx, &options).await.is_err());
        assert_eq!(host_fs::read(&options.destination).unwrap(), b"existing backup");
        options.destination = companion(&options.source, "-wal-cert");
        assert!(repair_wal(&cx, &options).await.is_err());
        assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
        assert_eq!(host_fs::read(&wal_path).unwrap(), original);
    });
}

#[cfg(unix)]
#[test]
fn native_in_place_repair_uses_certificate_without_rewriting_it() {
    run_test(async {
        let (options, _) = fixture_with_identity(8, &[], SOURCE_ID);
        let certificate = write_fixture_certificate(&options, SOURCE_ID);
        let wal_path = companion(&options.source, "-wal");
        let expected = host_fs::read(&wal_path).unwrap();
        let mut damaged = expected.clone();
        let terminal = WAL_HEADER_SIZE + 3 * FRAME_SIZE;
        damaged[terminal..terminal + WAL_FRAME_HEADER_SIZE].fill(0);
        damaged[WAL_HEADER_SIZE + FRAME_SIZE + WAL_FRAME_HEADER_SIZE + 60] ^= 1;
        host_fs::write(&wal_path, &damaged).unwrap();
        let cx = request_context().unwrap();
        let report = repair_wal(&cx, &options).await.unwrap();
        assert_eq!(report.certificate_anchors, 1);
        assert_eq!(host_fs::read(&wal_path).unwrap(), expected);
        assert_eq!(host_fs::read(&options.destination).unwrap(), damaged);
        assert_eq!(host_fs::read(&companion(&options.source, "-wal-cert")).unwrap(), certificate);
    });
}

#[cfg(unix)]
#[test]
fn native_in_place_contention_and_precancellation_do_not_reserve_backup() {
    run_test(async {
        let (options, _) = fixture(8, &[1]);
        let cx = request_context().unwrap();
        let vfs = NativeVfs::new();
        let mut owner = capture_held(&vfs, &cx, &options).await.unwrap();
        assert!(repair_wal(&cx, &options).await.is_err());
        assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
        owner.finish().unwrap();
        cx.cancel();
        assert!(repair_wal(&cx, &options).await.is_err());
        assert!(!vfs.path_entry_exists(&Cx::new(), &options.destination).unwrap());
    });
}

#[cfg(unix)]
#[test]
fn transferred_capture_keeps_fences_until_the_worker_finishes() {
    use std::sync::mpsc;
    use std::time::Duration;
    run_test(async {
        let (options, _) = fixture(8, &[1]);
        let cx = request_context().unwrap();
        let vfs = NativeVfs::new();
        let mut captured = capture_held(&vfs, &cx, &options).await.unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            captured.finish().unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(repair_wal(&cx, &options).await.is_err());
        assert!(!vfs.path_entry_exists(&cx, &options.destination).unwrap());
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        repair_wal(&cx, &options).await.unwrap();
    });
}

fn write_fixture_certificate(options: &Options, identity: [u8; 16]) -> Vec<u8> {
    use fsqlite_types::{CommitSeq, PageNumber};
    use crate::{PARALLEL_WAL_COMMIT_CERTIFICATE_VERSION, ParallelWalCommitCertificate,
        ParallelWalDurableCertificateRecord, ParallelWalFramePayloadDigestBuilder,
        ParallelWalOrderedResidue, WalGenerationIdentity};
    let wal = host_fs::read(&companion(&options.source, "-wal")).unwrap();
    let header = WalHeader::from_bytes(&wal).unwrap();
    let mut digest = ParallelWalFramePayloadDigestBuilder::new();
    for frame in wal[WAL_HEADER_SIZE..].chunks_exact(FRAME_SIZE) {
        let header = WalFrameHeader::from_bytes(frame).unwrap();
        digest.update(PageNumber::new(header.page_number).unwrap(), header.db_size, &frame[WAL_FRAME_HEADER_SIZE..]);
    }
    let mut certificate = ParallelWalCommitCertificate {
        format_version: PARALLEL_WAL_COMMIT_CERTIFICATE_VERSION,
        residue: ParallelWalOrderedResidue::CommitCertificateThenPublish,
        certificate_epoch: 7, commit_seq_lo: CommitSeq::new(7), commit_seq_hi: CommitSeq::new(7),
        durable_segment_epoch: 7, lane_count: 1, lane_record_counts: vec![4], db_size_pages: 1,
        page_set_size: 1, wal_frame_payload_digest: digest.finalize(), certificate_crc32c: 0, fallback_active: false,
    };
    certificate.certificate_crc32c = certificate.computed_crc32c();
    let record = ParallelWalDurableCertificateRecord::new(
        WalGenerationIdentity::from_header(&header), 1, 4, identity, certificate,
    ).unwrap();
    let bytes = record.to_bytes();
    host_fs::write(&companion(&options.source, "-wal-cert"), &bytes).unwrap();
    bytes
}

fn erase_final_header(options: &Options) -> Vec<u8> {
    let path = companion(&options.source, "-wal");
    let mut bytes = host_fs::read(&path).unwrap();
    let terminal = WAL_HEADER_SIZE + 3 * FRAME_SIZE;
    bytes[terminal..terminal + WAL_FRAME_HEADER_SIZE].fill(0);
    bytes[WAL_HEADER_SIZE + FRAME_SIZE + WAL_FRAME_HEADER_SIZE + 60] ^= 1;
    host_fs::write(&path, &bytes).unwrap();
    bytes
}

#[test]
fn certificate_backed_export_restores_final_transaction_and_preserves_all_sources() {
    run_test(async {
        let (options, expected) = fixture_with_identity(8, &[], SOURCE_ID);
        write_fixture_certificate(&options, SOURCE_ID);
        erase_final_header(&options);
        let originals: Vec<_> = ["", "-wal", "-wal-fec", "-wal-cert"].into_iter().map(|suffix| {
            let path = companion(&options.source, suffix);
            let bytes = host_fs::read(&path).unwrap();
            (path, bytes)
        }).collect();
        let cx = request_context().unwrap();
        let report = export_database(&cx, &options).await.unwrap();
        assert_eq!(report.wal_frames, 4);
        assert_eq!(report.repaired_frames, 2);
        assert_eq!(report.certificate_anchors, 1);
        assert_eq!(report.digest, blake3::hash(&expected));
        assert_eq!(host_fs::read(&options.destination).unwrap(), expected);
        for (path, bytes) in originals { assert_eq!(host_fs::read(&path).unwrap(), bytes); }
    });
}

#[test]
fn foreign_certificate_refuses_export_without_reserving_output() {
    run_test(async {
        let (options, _) = fixture_with_identity(8, &[], SOURCE_ID);
        let certificate = write_fixture_certificate(&options, [0x77; 16]);
        let wal = erase_final_header(&options);
        let main = host_fs::read(&options.source).unwrap();
        let cx = request_context().unwrap();
        assert!(export_database(&cx, &options).await.is_err());
        assert!(!NativeVfs::new().path_entry_exists(&cx, &options.destination).unwrap());
        assert_eq!(host_fs::read(&options.source).unwrap(), main);
        assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), wal);
        assert_eq!(host_fs::read(&companion(&options.source, "-wal-cert")).unwrap(), certificate);
    });
}

#[test]
fn missing_main_identity_is_not_borrowed_from_the_certificate_or_decoded_wal() {
    run_test(async {
        let (options, _) = fixture_with_identity(8, &[], SOURCE_ID);
        write_fixture_certificate(&options, SOURCE_ID);
        erase_final_header(&options);
        let mut main = host_fs::read(&options.source).unwrap();
        main[76..92].fill(0);
        host_fs::write(&options.source, &main).unwrap();
        let cx = request_context().unwrap();
        assert!(export_database(&cx, &options).await.is_err());
        assert!(!NativeVfs::new().path_entry_exists(&cx, &options.destination).unwrap());
        assert_eq!(host_fs::read(&options.source).unwrap(), main);
    });
}

#[test]
fn optional_bad_or_oversized_certificates_do_not_block_checksum_backed_export() {
    run_test(async {
        for oversized in [false, true] {
            let (mut options, expected) = fixture_with_identity(8, &[1], SOURCE_ID);
            let certificate = companion(&options.source, "-wal-cert");
            host_fs::write(&certificate, b"unusable optional proof").unwrap();
            if oversized { options.replay.max_certificate_bytes = 1; }
            let cx = request_context().unwrap();
            let report = export_database(&cx, &options).await.unwrap();
            assert_eq!(report.certificate_anchors, 0);
            assert_eq!(host_fs::read(&options.destination).unwrap(), expected);
            assert_eq!(host_fs::read(&certificate).unwrap(), b"unusable optional proof");
        }
    });
}

#[test]
fn torn_certificate_stream_cannot_authorize_an_unanchored_export() {
    run_test(async {
        let (options, _) = fixture_with_identity(8, &[], SOURCE_ID);
        let mut certificate = write_fixture_certificate(&options, SOURCE_ID);
        certificate.push(0);
        let path = companion(&options.source, "-wal-cert");
        host_fs::write(&path, &certificate).unwrap();
        let wal = erase_final_header(&options);
        let cx = request_context().unwrap();
        assert!(export_database(&cx, &options).await.is_err());
        assert!(!NativeVfs::new().path_entry_exists(&cx, &options.destination).unwrap());
        assert_eq!(host_fs::read(&path).unwrap(), certificate);
        assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), wal);
    });
}

#[cfg(unix)]
#[test]
fn certificate_alias_of_source_data_is_refused() {
    use std::os::unix::fs::symlink;
    run_test(async {
        for suffix in ["", "-wal"] {
            let (options, _) = fixture_with_identity(8, &[], SOURCE_ID);
            let source = companion(&options.source, suffix);
            let original = host_fs::read(&source).unwrap();
            symlink(&source, companion(&options.source, "-wal-cert")).unwrap();
            let cx = request_context().unwrap();
            assert!(export_database(&cx, &options).await.is_err());
            assert!(!NativeVfs::new().path_entry_exists(&cx, &options.destination).unwrap());
            assert_eq!(host_fs::read(&source).unwrap(), original);
        }
    });
}
