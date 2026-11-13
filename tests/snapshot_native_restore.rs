#![recursion_limit = "512"]
#![cfg(all(feature = "native", unix, not(target_arch = "wasm32")))]

//! Public journal restore exercises the real codec, replay, native file and
//! bounded engine validator. Stock SQLite creates independent oracle images.

use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_core::replication_sender::{CHANGESET_HEADER_SIZE, SenderConfig};
use fsqlite_core::snapshot_shipping::manifest::{
    ManifestSnapshotReceiver, SnapshotFileSender, SnapshotManifest, SnapshotSourceLimits,
    SnapshotSpool, SnapshotSpoolState,
};
use fsqlite_error::FrankenError;
use fsqlite_types::{cx::Cx, flags::VfsOpenFlags};
use fsqlite_vfs::{MemoryVfs, memory::MemoryFile, traits::{Vfs, VfsFile}};

const KEY: [u8; 32] = [0x73; 32];
const CAP: u64 = 16 * 1024 * 1024;

fn run<F: std::future::Future<Output = ()>>(future: F) {
    asupersync::runtime::RuntimeBuilder::current_thread().blocking_threads(1, 2)
        .build().unwrap().block_on(future);
}

fn cx() -> Cx {
    let cx = Cx::new();
    cx.set_native_cx(asupersync::Cx::current().expect("runtime context"));
    cx
}

fn memory_file(vfs: &MemoryVfs, cx: &Cx, path: &str) -> MemoryFile {
    vfs.open(cx, Some(Path::new(path)), VfsOpenFlags::CREATE | VfsOpenFlags::READWRITE)
        .unwrap().0
}

fn receiver(manifest: &SnapshotManifest) -> ManifestSnapshotReceiver {
    ManifestSnapshotReceiver::new(manifest.clone(), manifest.id(), KEY, 64 * 1024).unwrap()
}

fn sql_image(path: &Path, encoding: &str) -> (Vec<u8>, u32) {
    let stock = rusqlite::Connection::open(path).unwrap();
    stock.execute_batch(&format!(
        "PRAGMA page_size=512; PRAGMA encoding='{encoding}';
         CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL, payload BLOB);
         CREATE INDEX items_name ON items(name);
         INSERT INTO items VALUES(1,'café',zeroblob(1200)),(2,'日本語',zeroblob(1200));"
    )).unwrap();
    let root = stock.query_row(
        "SELECT rootpage FROM sqlite_schema WHERE name='items'", [], |row| row.get(0),
    ).unwrap();
    stock.close().unwrap();
    (std::fs::read(path).unwrap(), root)
}

async fn journal(
    cx: &Cx,
    bytes: &[u8],
    packets: Option<usize>,
) -> (SnapshotManifest, SnapshotSpool<MemoryFile>) {
    let vfs = MemoryVfs::new();
    let source = memory_file(&vfs, cx, "source");
    source.write(cx, bytes, 0).await.unwrap();
    let mut sender = SnapshotFileSender::open(
        cx, source, SenderConfig { symbol_size: 512, max_isi_multiplier: 1 },
        SnapshotSourceLimits {
            max_image_bytes: CAP,
            max_block_bytes: CHANGESET_HEADER_SIZE + 2 * (512 + 12),
        },
    ).await.unwrap();
    let manifest = sender.manifest().clone();
    let mut spool = SnapshotSpool::create(
        cx, memory_file(&vfs, cx, "journal"), receiver(&manifest), CAP,
    ).await.unwrap();
    let mut emitted = 0;
    while packets.is_none_or(|limit| emitted < limit) {
        let Some(mut packet) = sender.next_packet(cx).await.unwrap() else { break; };
        packet.attach_auth_tag(&KEY);
        spool.append(cx, &packet).await.unwrap();
        drop(spool.take_decoded_blocks());
        emitted += 1;
    }
    if packets.is_none() { assert!(spool.receiver().is_complete()); }
    let checkpoint = spool.checkpoint(cx).unwrap();
    let spool = SnapshotSpool::open(
        cx, spool.into_file(), receiver(&manifest), CAP, Some(checkpoint),
    ).await.unwrap();
    (manifest, spool)
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

#[test]
fn native_restore_roundtrips_indexed_utf8_and_utf16_images() {
    run(async {
        let dir = tempfile::tempdir().unwrap(); let cx = cx();
        for encoding in ["UTF-8", "UTF-16le", "UTF-16be"] {
            let (bytes, _) = sql_image(&dir.path().join(format!("oracle-{encoding}.db")), encoding);
            let (manifest, mut spool) = journal(&cx, &bytes, None).await;
            assert!(manifest.blocks().len() > 1);
            let destination = dir.path().join(format!("restored-{encoding}.db"));
            let receipt = Connection::restore_snapshot_transfer(
                &cx, &mut spool, &destination, manifest.id(), CAP,
            ).await.unwrap();
            assert_eq!(receipt.image_blake3, *blake3::hash(&bytes).as_bytes());
            assert_eq!(receipt.byte_len, bytes.len() as u64);
            assert_eq!(std::fs::read(&destination).unwrap(), bytes);
            for suffix in ["-wal", "-shm", "-journal", "-fsqlite-ns-gate", "-fsqlite-ns-use"] {
                assert!(!sidecar(&destination, suffix).exists(), "{suffix}");
            }
            let stock = rusqlite::Connection::open_with_flags(
                &destination, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            ).unwrap();
            let verdict: String = stock.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
            assert_eq!(verdict, "ok");
            let count: i64 = stock.query_row("SELECT count(*) FROM items", [], |row| row.get(0)).unwrap();
            assert_eq!(count, 2);
            stock.close().unwrap();
        }
    });
}

#[test]
fn native_restore_rejects_admission_errors_before_creating_or_replaying() {
    run(async {
        let dir = tempfile::tempdir().unwrap(); let cx = cx();
        let (bytes, _) = sql_image(&dir.path().join("oracle.db"), "UTF-8");
        let (manifest, mut spool) = journal(&cx, &bytes, None).await;
        let destination = dir.path().join("never-created.db");
        let cancelled = Cx::new(); cancelled.cancel();
        assert!(matches!(Connection::restore_snapshot_transfer(
            &cancelled, &mut spool, &destination, manifest.id(), CAP,
        ).await, Err(FrankenError::Abort)));
        assert!(Connection::restore_snapshot_transfer(
            &cx, &mut spool, &destination, [0; 32], CAP,
        ).await.is_err());
        assert!(matches!(Connection::restore_snapshot_transfer(
            &cx, &mut spool, &destination, manifest.id(), bytes.len() as u64 - 1,
        ).await, Err(FrankenError::TooBig)));
        assert!(!destination.exists());
        assert_eq!(spool.state(), SnapshotSpoolState::Replaying);
        assert_eq!(spool.record_count(), 0);
        for (index, sentinel) in [b"".as_slice(), b"existing data".as_slice()].iter().enumerate() {
            let existing = dir.path().join(format!("existing-{index}.db"));
            std::fs::write(&existing, sentinel).unwrap();
            assert!(Connection::restore_snapshot_transfer(
                &cx, &mut spool, &existing, manifest.id(), CAP,
            ).await.is_err());
            assert_eq!(std::fs::read(&existing).unwrap(), *sentinel);
        }
        for suffix in ["-wal", "-shm", "-journal", "-fsqlite-ns-gate", "-fsqlite-ns-use"] {
            let destination = dir.path().join(format!("reserved{suffix}.db"));
            let saved = sidecar(&destination, suffix);
            std::fs::write(&saved, b"owned sidecar").unwrap();
            assert!(Connection::restore_snapshot_transfer(
                &cx, &mut spool, &destination, manifest.id(), CAP,
            ).await.is_err());
            assert!(!destination.exists());
            assert_eq!(std::fs::read(saved).unwrap(), b"owned sidecar");
        }
        let dangling = dir.path().join("dangling.db");
        let absent = dir.path().join("symlink-target.db");
        std::os::unix::fs::symlink(&absent, &dangling).unwrap();
        assert!(Connection::restore_snapshot_transfer(
            &cx, &mut spool, &dangling, manifest.id(), CAP,
        ).await.is_err());
        assert!(!absent.exists());
        assert_eq!(std::fs::read_link(dangling).unwrap(), absent);
        assert_eq!(spool.record_count(), 0);
    });
}

#[test]
fn native_restore_refuses_authenticated_btree_corruption_without_repair() {
    run(async {
        let dir = tempfile::tempdir().unwrap(); let cx = cx();
        let (mut bytes, root) = sql_image(&dir.path().join("oracle.db"), "UTF-8");
        bytes[(root as usize - 1) * 512] = 0xff;
        // The damaged image has a valid manifest and valid packet MACs. Only
        // the engine's semantic gate can distinguish it from a valid transfer.
        let (manifest, mut spool) = journal(&cx, &bytes, None).await;
        let destination = dir.path().join("bad.db");
        assert!(Connection::restore_snapshot_transfer(
            &cx, &mut spool, &destination, manifest.id(), CAP,
        ).await.is_err());
        assert_eq!(std::fs::read(&destination).unwrap(), bytes, "never repair or delete failed evidence");
        assert!(Connection::confirm_restored_snapshot(
            &cx, &destination, &manifest, manifest.id(), CAP,
        ).await.is_err(), "a matching manifest cannot bypass semantic validation");
        assert_eq!(std::fs::read(&destination).unwrap(), bytes);
    });
}

#[test]
fn native_restore_incomplete_verified_journal_cannot_return_an_image_receipt() {
    run(async {
        let dir = tempfile::tempdir().unwrap(); let cx = cx();
        let (bytes, _) = sql_image(&dir.path().join("oracle.db"), "UTF-8");
        let (manifest, mut spool) = journal(&cx, &bytes, Some(1)).await;
        let destination = dir.path().join("incomplete.db");
        assert!(Connection::restore_snapshot_transfer(
            &cx, &mut spool, &destination, manifest.id(), CAP,
        ).await.is_err());
        assert!(destination.exists(), "failed reservations are retained, never unlinked");
        assert!(std::fs::read(&destination).unwrap().is_empty());
    });
}

#[test]
fn native_restore_lost_confirmation_is_recovered_without_replay_or_rewrite() {
    run(async {
        let dir = tempfile::tempdir().unwrap(); let cx = cx();
        let (bytes, _) = sql_image(&dir.path().join("oracle.db"), "UTF-8");
        let (manifest, mut spool) = journal(&cx, &bytes, None).await;
        let destination = dir.path().join("candidate.db");
        let first = Connection::restore_snapshot_transfer(
            &cx, &mut spool, &destination, manifest.id(), CAP,
        ).await.unwrap();
        // Model loss of the return value: recovery uses neither the receipt
        // nor the consumed journal. Only original trusted transfer identity.
        drop(spool);
        let identity = {
            let file = std::fs::File::open(&destination).unwrap();
            fsqlite_vfs::FileIdentity::from_file(&file).unwrap()
        };
        let modified = std::fs::metadata(&destination).unwrap().modified().unwrap();
        for _ in 0..2 {
            let confirmed = Connection::confirm_restored_snapshot(
                &cx, &destination, &manifest, manifest.id(), CAP,
            ).await.unwrap();
            assert_eq!(confirmed, first);
            assert_eq!(std::fs::read(&destination).unwrap(), bytes);
            assert_eq!(std::fs::metadata(&destination).unwrap().modified().unwrap(), modified);
            let file = std::fs::File::open(&destination).unwrap();
            assert_eq!(fsqlite_vfs::FileIdentity::from_file(&file).unwrap(), identity);
        }
    });
}

#[test]
fn native_confirmation_rejects_partial_or_changed_images_before_sql_open() {
    run(async {
        let dir = tempfile::tempdir().unwrap(); let cx = cx();
        let (bytes, _) = sql_image(&dir.path().join("oracle.db"), "UTF-8");
        let (manifest, _) = journal(&cx, &bytes, None).await;
        let mut changed = bytes.clone();
        changed[72] ^= 1; // Header-reserved byte: geometry still agrees.
        let destination = dir.path().join("changed.db");
        std::fs::write(&destination, &changed).unwrap();
        let error = Connection::confirm_restored_snapshot(
            &cx, &destination, &manifest, manifest.id(), CAP,
        ).await.unwrap_err();
        assert!(error.to_string().contains("does not match its manifest"), "{error}");
        assert_eq!(std::fs::read(&destination).unwrap(), changed);
        let truncated = dir.path().join("partial.db");
        std::fs::write(&truncated, &bytes[..bytes.len() - 1]).unwrap();
        assert!(Connection::confirm_restored_snapshot(
            &cx, &truncated, &manifest, manifest.id(), CAP,
        ).await.is_err());
        assert_eq!(std::fs::read(&truncated).unwrap(), &bytes[..bytes.len() - 1]);
        let absent = dir.path().join("absent.db");
        assert!(Connection::confirm_restored_snapshot(
            &cx, &absent, &manifest, manifest.id(), CAP,
        ).await.is_err());
        assert!(!absent.exists());
        for candidate in [&destination, &truncated, &absent] {
            for suffix in ["-wal", "-shm", "-journal", "-fsqlite-ns-gate", "-fsqlite-ns-use"] {
                assert!(!sidecar(candidate, suffix).exists());
            }
        }
    });
}

#[test]
fn native_confirmation_rejects_aliases_admitted_namespaces_and_wrong_identity() {
    run(async {
        let dir = tempfile::tempdir().unwrap(); let cx = cx();
        let (bytes, _) = sql_image(&dir.path().join("oracle.db"), "UTF-8");
        let (manifest, _) = journal(&cx, &bytes, None).await;
        let destination = dir.path().join("candidate.db");
        std::fs::write(&destination, &bytes).unwrap();
        let cancelled = Cx::new(); cancelled.cancel();
        assert!(matches!(Connection::confirm_restored_snapshot(
            &cancelled, &destination, &manifest, manifest.id(), CAP,
        ).await, Err(FrankenError::Abort)));
        assert!(Connection::confirm_restored_snapshot(
            &cx, &destination, &manifest, [0; 32], CAP,
        ).await.is_err());
        assert!(matches!(Connection::confirm_restored_snapshot(
            &cx, &destination, &manifest, manifest.id(), bytes.len() as u64 - 1,
        ).await, Err(FrankenError::TooBig)));
        let symlink = dir.path().join("alias.db");
        std::os::unix::fs::symlink(&destination, &symlink).unwrap();
        assert!(Connection::confirm_restored_snapshot(
            &cx, &symlink, &manifest, manifest.id(), CAP,
        ).await.is_err());
        let hardlink = dir.path().join("hardlink.db");
        std::fs::hard_link(&destination, &hardlink).unwrap();
        assert!(Connection::confirm_restored_snapshot(
            &cx, &hardlink, &manifest, manifest.id(), CAP,
        ).await.is_err());
        assert_eq!(std::fs::read(&destination).unwrap(), bytes);
        for suffix in ["-wal", "-shm", "-journal", "-fsqlite-ns-gate", "-fsqlite-ns-use"] {
            let candidate = dir.path().join(format!("admitted{suffix}.db"));
            std::fs::write(&candidate, &bytes).unwrap();
            std::fs::write(sidecar(&candidate, suffix), b"existing namespace").unwrap();
            assert!(Connection::confirm_restored_snapshot(
                &cx, &candidate, &manifest, manifest.id(), CAP,
            ).await.is_err());
            assert_eq!(std::fs::read(&candidate).unwrap(), bytes);
        }
    });
}
