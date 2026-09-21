//! Export must never turn a source companion into an output database.

use super::*;
use crate::checksum::WalChecksumTransform;
use crate::{SqliteWalChecksum, WalFrameHeader, WalHeader, WalSalts};

fn run<F: std::future::Future>(future: F) -> F::Output {
    RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .unwrap()
        .block_on(future)
}

fn directory_entries(directory: &Path) -> Vec<PathBuf> {
    let mut entries = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

/// A healthy, one-frame WAL needs no FEC sidecar or certificate. In particular,
/// source.db-wal-cert is absent: an existence check alone cannot protect it.
fn healthy_source(directory: &Path) -> (Options, Vec<u8>, Vec<u8>) {
    let options = Options::new(directory.join("source.db"), directory.join("export.db"));
    let mut page = vec![0_u8; 512];
    page[..16].copy_from_slice(b"SQLite format 3\0");
    page[16..18].copy_from_slice(&512_u16.to_be_bytes());
    page[18..20].copy_from_slice(&[2, 2]);
    page[21..24].copy_from_slice(&[64, 32, 32]);
    page[24..28].copy_from_slice(&7_u32.to_be_bytes());
    page[28..32].copy_from_slice(&1_u32.to_be_bytes());
    page[44..48].copy_from_slice(&4_u32.to_be_bytes());
    page[56..60].copy_from_slice(&1_u32.to_be_bytes());
    page[92..96].copy_from_slice(&7_u32.to_be_bytes());
    page[100] = 13;
    page[105..107].copy_from_slice(&512_u16.to_be_bytes());
    host_fs::write(&options.source, &page).unwrap();
    page[60..64].copy_from_slice(&42_u32.to_be_bytes());
    let header = WalHeader {
        magic: crate::WAL_MAGIC_LE,
        format_version: crate::WAL_FORMAT_VERSION,
        page_size: 512,
        checkpoint_seq: 1,
        salts: WalSalts { salt1: 123, salt2: 456 },
        checksum: SqliteWalChecksum::default(),
    };
    let mut wal = header.to_bytes().unwrap().to_vec();
    let seed = WalHeader::from_bytes(&wal).unwrap().checksum;
    let offset = wal.len();
    wal.extend_from_slice(&WalFrameHeader {
        page_number: 1,
        db_size: 1,
        salts: header.salts,
        checksum: SqliteWalChecksum::default(),
    }.to_bytes());
    wal.extend_from_slice(&page);
    let checksum = WalChecksumTransform::for_wal_frame(&wal[offset..], 512, false)
        .unwrap()
        .apply(seed);
    wal[offset + 16..offset + 20].copy_from_slice(&checksum.s1.to_be_bytes());
    wal[offset + 20..offset + 24].copy_from_slice(&checksum.s2.to_be_bytes());
    host_fs::write(&companion(&options.source, "-wal"), &wal).unwrap();
    (options, page, wal)
}

#[test]
fn reserved_source_destinations_fail_before_any_source_admission() {
    run(async {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("missing.db");
        let cx = request_context().unwrap();
        let vfs = NativeVfs::new();
        // Independent list: omitting a production suffix must break this test.
        for suffix in ["", "-journal", "-wal", "-shm", "-wal-fec", "-wal-fec.lock", "-wal-cert", ".fsqlite-shm"] {
            let destination = companion(&source, suffix);
            let expected = vfs.full_pathname(&cx, &destination).unwrap();
            let options = Options::new(&source, destination);
            assert!(
                matches!(export_database(&cx, &options).await,
                    Err(FrankenError::CannotOpen { path }) if path == expected),
                "reserved destination {suffix:?} must fail before opening a missing source"
            );
            assert!(directory_entries(directory.path()).is_empty());
        }
    });
}

#[test]
fn healthy_export_cannot_create_an_absent_source_certificate() {
    run(async {
        let directory = tempfile::tempdir().unwrap();
        let (mut options, _, wal) = healthy_source(directory.path());
        let original = host_fs::read(&options.source).unwrap();
        let entries = directory_entries(directory.path());
        options.destination = companion(&options.source, "-wal-cert");
        let cx = request_context().unwrap();
        let expected = NativeVfs::new().full_pathname(&cx, &options.destination).unwrap();
        assert!(matches!(export_database(&cx, &options).await,
            Err(FrankenError::CannotOpen { path }) if path == expected));
        assert_eq!(directory_entries(directory.path()), entries);
        assert_eq!(host_fs::read(&options.source).unwrap(), original);
        assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), wal);
    });
}

#[test]
fn unrelated_destination_still_exports_the_latest_committed_page() {
    run(async {
        let directory = tempfile::tempdir().unwrap();
        let (options, expected, wal) = healthy_source(directory.path());
        let original = host_fs::read(&options.source).unwrap();
        let cx = request_context().unwrap();
        let report = export_database(&cx, &options).await.unwrap();
        assert_eq!(report.pages, 1);
        assert_eq!(report.wal_frames, 1);
        assert_eq!(report.repaired_frames, 0);
        assert_eq!(report.certificate_anchors, 0);
        assert!(!report.repaired_in_place);
        assert_eq!(report.digest, blake3::hash(&expected));
        assert_eq!(host_fs::read(&options.destination).unwrap(), expected);
        assert_eq!(host_fs::read(&options.source).unwrap(), original);
        assert_eq!(host_fs::read(&companion(&options.source, "-wal")).unwrap(), wal);
        assert!(!NativeVfs::new().path_entry_exists(&cx, &companion(&options.source, "-wal-cert")).unwrap());
    });
}

#[test]
fn orphaned_destination_fec_lock_prevents_export_without_side_effects() {
    run(async {
        let directory = tempfile::tempdir().unwrap();
        let (options, _, _) = healthy_source(directory.path());
        let lock = companion(&options.destination, "-wal-fec.lock");
        host_fs::write(&lock, b"another recovery owner").unwrap();
        let entries = directory_entries(directory.path());
        let cx = request_context().unwrap();
        let expected = NativeVfs::new().full_pathname(&cx, &lock).unwrap();
        assert!(matches!(export_database(&cx, &options).await,
            Err(FrankenError::CannotOpen { path }) if path == expected));
        assert_eq!(directory_entries(directory.path()), entries);
        assert_eq!(host_fs::read(&lock).unwrap(), b"another recovery owner");
    });
}

#[test]
fn source_namespace_guard_is_not_a_filename_prefix_ban() {
    let source = Path::new("source.db");
    for destination in ["source.db.export", "source.db-wal-cert.backup", "source.db-copy"] {
        assert!(!is_source_artifact(source, Path::new(destination)));
    }
}
