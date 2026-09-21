//! Public Connection -> frozen source -> real repair symbols -> durable spool
//! -> SQLite image. Native acceptance tests; no mock snapshot-authority path.
#![cfg(all(feature = "native", unix, not(target_arch = "wasm32")))]

use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_core::replication_sender::{CHANGESET_HEADER_SIZE, SenderConfig};
use fsqlite_core::snapshot_shipping::manifest::{
    ManifestSnapshotReceiver, SnapshotImageWriter, SnapshotManifest, SnapshotSourceLimits,
    SnapshotSpool,
};
use fsqlite_error::FrankenError;
use fsqlite_types::{SqliteValue, cx::Cx, flags::VfsOpenFlags};
use fsqlite_vfs::{UnixFile, UnixVfs, host_fs, traits::Vfs};

const KEY: [u8; 32] = [0x61; 32];
const FILE_CAP: u64 = 16 * 1024 * 1024;

fn run<F: std::future::Future<Output = ()>>(future: F) {
    asupersync::runtime::RuntimeBuilder::current_thread().blocking_threads(1, 2)
        .build().unwrap().block_on(future);
}

fn cx() -> Cx {
    let cx = Cx::new();
    cx.set_native_cx(asupersync::Cx::current().expect("runtime context"));
    cx
}

fn config() -> SenderConfig {
    SenderConfig { symbol_size: 512, max_isi_multiplier: 8 }
}

fn limits() -> SnapshotSourceLimits {
    SnapshotSourceLimits {
        max_image_bytes: FILE_CAP,
        max_block_bytes: CHANGESET_HEADER_SIZE + 2 * (512 + 12),
    }
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string(); name.push(suffix); PathBuf::from(name)
}

fn file(cx: &Cx, path: &Path, create: bool) -> UnixFile {
    let flags = if create { VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE } else { VfsOpenFlags::READWRITE };
    UnixVfs::new().open(cx, Some(path), flags).unwrap().0
}

fn receiver(manifest: &SnapshotManifest) -> ManifestSnapshotReceiver {
    ManifestSnapshotReceiver::new(manifest.clone(), manifest.id(), KEY, 64 * 1024).unwrap()
}

fn seed_database(path: &Path) {
    let oracle = rusqlite::Connection::open(path).unwrap();
    oracle.execute_batch(
        "PRAGMA page_size=512;
         CREATE TABLE items(id INTEGER PRIMARY KEY, generation INTEGER NOT NULL, payload BLOB);
         CREATE INDEX generation_idx ON items(generation);
         INSERT INTO items VALUES (1,0,zeroblob(1200)),(2,0,zeroblob(1200)),(3,0,zeroblob(1200));"
    ).unwrap();
}

fn assert_sql_image(path: &Path, rows: i64, generation: i64) {
    let oracle = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let integrity: String = oracle.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
    assert_eq!(integrity, "ok");
    let observed: (i64, i64, i64) = oracle.query_row(
        "SELECT count(*), min(generation), max(generation) FROM items", [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).unwrap();
    assert_eq!(observed, (rows, generation, generation));
}

#[test]
fn live_wal_capture_stays_frozen_while_peer_writes_and_repairs_restore_sql() {
    run(async {
        let dir = tempfile::tempdir().unwrap(); let cx = cx();
        let source = dir.path().join("source.db"); seed_database(&source);
        let conn = Connection::open(&*source.to_string_lossy()).await.unwrap();
        conn.execute("PRAGMA journal_mode=WAL").await.unwrap();
        conn.execute("PRAGMA wal_autocheckpoint=0").await.unwrap();
        conn.execute("BEGIN").await.unwrap();
        conn.execute("UPDATE items SET generation=1").await.unwrap();
        conn.execute("COMMIT").await.unwrap();
        assert!(host_fs::metadata(&sidecar(&source, "-wal")).unwrap().len() > 32);
        let frozen = dir.path().join("frozen.db");
        let (report, mut sender) = conn.capture_snapshot_transfer(&cx, &frozen, config(), limits()).await.unwrap();
        assert_eq!(report.byte_len, sender.byte_len());
        let manifest = sender.manifest().clone();
        assert!(manifest.blocks().len() > 1, "real multiblock database required");
        let expected_hash = sender.image_blake3();
        let peer = Connection::open(&*source.to_string_lossy()).await.unwrap();
        let journal = dir.path().join("transfer.spool");
        let mut spool = SnapshotSpool::create(&cx, file(&cx, &journal, true), receiver(&manifest), FILE_CAP).await.unwrap();
        let mut advanced = false;
        let mut erased = 0;
        let mut recovered = 0;
        while let Some(mut packet) = sender.next_packet(&cx).await.unwrap() {
            if !advanced {
                peer.execute_batch(
                    "BEGIN; UPDATE items SET generation=2;
                     INSERT INTO items VALUES (4,2,zeroblob(1200)); COMMIT;"
                ).await.unwrap();
                advanced = true;
            }
            assert!(sender.encoded_payload_bytes() <= limits().max_block_bytes);
            if packet.esi == 0 { erased += 1; continue; }
            packet.attach_auth_tag(&KEY);
            spool.append(&cx, &packet).await.unwrap();
            recovered += spool.take_decoded_blocks().len();
        }
        assert!(advanced && spool.receiver().is_complete());
        assert_eq!(erased, manifest.blocks().len());
        assert_eq!(recovered, manifest.blocks().len());
        let checkpoint = spool.checkpoint(&cx).unwrap();
        drop(spool.into_file());
        let mut reopened = SnapshotSpool::open(
            &cx, file(&cx, &journal, false), receiver(&manifest), FILE_CAP, Some(checkpoint),
        ).await.unwrap();
        let restored = dir.path().join("replica.db");
        let mut image = SnapshotImageWriter::create(
            &cx, file(&cx, &restored, true), manifest.clone(), manifest.id(), FILE_CAP,
        ).unwrap();
        let receipt = reopened.replay_into_image(&cx, &mut image).await.unwrap();
        assert_eq!(receipt.image_blake3, expected_hash);
        drop(image.into_file());
        assert_eq!(host_fs::read(&restored).unwrap(), host_fs::read(&frozen).unwrap());
        assert_sql_image(&restored, 3, 1); // WAL-committed state, not the stale main file or later writes.
        let replica = Connection::open(&*restored.to_string_lossy()).await.unwrap();
        let count = replica.query_row("SELECT count(*) FROM items WHERE generation=1").await.unwrap();
        assert_eq!(count.get(0), Some(&SqliteValue::Integer(3)));
        replica.execute("INSERT INTO items VALUES (4,1,zeroblob(10))").await.unwrap();
        replica.close().await.unwrap();
        assert_sql_image(&restored, 4, 1);
        peer.close().await.unwrap(); conn.close().await.unwrap();
        assert_sql_image(&source, 4, 2);
    });
}

#[test]
fn capture_refuses_own_uncommitted_transaction_without_changing_it() {
    run(async {
        let dir = tempfile::tempdir().unwrap(); let cx = cx();
        let source = dir.path().join("source.db"); seed_database(&source);
        let conn = Connection::open(&*source.to_string_lossy()).await.unwrap();
        conn.execute("BEGIN").await.unwrap();
        conn.execute("UPDATE items SET generation=9").await.unwrap();
        let destination = dir.path().join("uncommitted.db");
        assert!(conn.capture_snapshot_transfer(&cx, &destination, config(), limits()).await.is_err());
        assert!(conn.in_transaction());
        let row = conn.query_row("SELECT min(generation) FROM items").await.unwrap();
        assert_eq!(row.get(0), Some(&SqliteValue::Integer(9)));
        conn.execute("ROLLBACK").await.unwrap(); conn.close().await.unwrap();
        assert_sql_image(&source, 3, 0);
    });
}

#[test]
fn capture_refuses_existing_namespaces_and_pre_cancelled_or_invalid_requests() {
    run(async {
        let dir = tempfile::tempdir().unwrap(); let cx = cx();
        let source = dir.path().join("source.db"); seed_database(&source);
        let conn = Connection::open(&*source.to_string_lossy()).await.unwrap();
        let destination = dir.path().join("existing.db");
        host_fs::write(&destination, b"owned sentinel").unwrap();
        assert!(conn.capture_snapshot_transfer(&cx, &destination, config(), limits()).await.is_err());
        assert_eq!(host_fs::read(&destination).unwrap(), b"owned sentinel");
        for suffix in ["-wal", "-shm", "-journal"] {
            let destination = dir.path().join(format!("sidecar{suffix}.db"));
            let saved = sidecar(&destination, suffix);
            host_fs::write(&saved, b"sidecar sentinel").unwrap();
            assert!(conn.capture_snapshot_transfer(&cx, &destination, config(), limits()).await.is_err());
            assert!(!destination.exists());
            assert_eq!(host_fs::read(&saved).unwrap(), b"sidecar sentinel");
        }
        let untouched = dir.path().join("never-created.db");
        let cancelled = Cx::new(); cancelled.cancel();
        assert!(matches!(conn.capture_snapshot_transfer(&cancelled, &untouched, config(), limits()).await, Err(FrankenError::Abort)));
        let invalid = SenderConfig { symbol_size: 0, max_isi_multiplier: 8 };
        assert!(conn.capture_snapshot_transfer(&cx, &untouched, invalid, limits()).await.is_err());
        assert!(!untouched.exists());
        conn.close().await.unwrap();
    });
}
