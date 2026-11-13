#![recursion_limit = "512"]

//! Native SQL -> authenticated packet journal -> reopened SQLite image.
//!
//! Local/RCH oracle keeper, not an Actions or power-loss certification claim.
//! The source is closed in DELETE journal mode before capture. Capturing an
//! arbitrary live main-file without its WAL is intentionally not exercised.
#![cfg(all(feature = "native", unix, not(target_arch = "wasm32")))]

use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_core::replication_sender::{PageEntry, ReplicationPacket, SenderConfig};
use fsqlite_core::snapshot_shipping::manifest::{
    ManifestSnapshotReceiver, SnapshotCheckpoint, SnapshotImageState, SnapshotImageWriter,
    SnapshotManifest, SnapshotSpool, SnapshotSpoolState,
};
use fsqlite_core::snapshot_shipping::SnapshotSender;
use fsqlite_types::{SqliteValue, cx::Cx, flags::VfsOpenFlags};
use fsqlite_vfs::{MemoryVfs, UnixVfs, traits::{Vfs, VfsFile}};

const KEY: [u8; 32] = [0x71; 32];
const PAGE_SIZE: u32 = 4096;
const CAP: u64 = 4 * 1024 * 1024;

fn run<F: std::future::Future<Output = ()>>(future: F) {
    asupersync::runtime::RuntimeBuilder::current_thread()
        .blocking_threads(1, 2).build().unwrap().block_on(future);
}

fn cx() -> Cx {
    let cx = Cx::new();
    cx.set_native_cx(asupersync::Cx::current().expect("native test context"));
    cx
}

fn open<V: Vfs>(vfs: &V, cx: &Cx, path: &Path) -> V::File {
    vfs.open(cx, Some(path), VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE).unwrap().0
}

fn source_image(path: &Path) -> Vec<u8> {
    let stock = rusqlite::Connection::open(path).unwrap();
    stock.execute_batch(
        "PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA user_version=37;
         CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, payload BLOB);
         CREATE INDEX payload_size ON items(length(payload));",
    ).unwrap();
    for id in 1..=64 {
        stock.execute(
            "INSERT INTO items VALUES (?1,?2,zeroblob(96))",
            rusqlite::params![id, format!("item-{id:03}")],
        ).unwrap();
    }
    drop(stock);
    let image = std::fs::read(path).unwrap();
    assert_eq!(image.len() % PAGE_SIZE as usize, 0);
    image
}

fn transfer(image: &[u8], repairs: bool) -> (SnapshotManifest, Vec<ReplicationPacket>) {
    let mut pages: Vec<_> = image.as_chunks::<{ PAGE_SIZE as usize }>().0.iter().enumerate()
        .map(|(index, data)| PageEntry::new(index as u32 + 1, data.to_vec())).collect();
    let mut sender = SnapshotSender::prepare(PAGE_SIZE, &mut pages, SenderConfig {
        symbol_size: 4096, max_isi_multiplier: if repairs { 8 } else { 1 },
    }).unwrap();
    let manifest = sender.manifest().unwrap();
    let manifest = SnapshotManifest::from_bytes(&manifest.to_bytes()).unwrap();
    let mut packets = Vec::new();
    while let Some(mut packet) = sender.next_packet(&Cx::new()).unwrap() {
        if repairs && packet.esi == 0 { continue; } // Permanently absent, never retransmitted.
        packet.attach_auth_tag(&KEY);
        packets.push(packet);
    }
    // Reordering does not alter the predetermined erasure or seed.
    for pair in packets.chunks_mut(2) {
        if pair.len() == 2 { pair.swap(0, 1); }
    }
    (manifest, packets)
}

fn receiver(manifest: &SnapshotManifest) -> ManifestSnapshotReceiver {
    ManifestSnapshotReceiver::new(manifest.clone(), manifest.id(), KEY, CAP as usize).unwrap()
}

async fn saved_journal<V: Vfs>(
    vfs: &V, cx: &Cx, path: &Path, manifest: &SnapshotManifest, packets: &[ReplicationPacket],
) -> SnapshotCheckpoint {
    let mut spool = SnapshotSpool::create(cx, open(vfs, cx, path), receiver(manifest), CAP).await.unwrap();
    for packet in packets {
        spool.append(cx, packet).await.unwrap();
        drop(spool.take_decoded_blocks());
        if spool.receiver().is_complete() { break; }
    }
    let checkpoint = spool.checkpoint(cx).unwrap();
    drop(spool.into_file());
    checkpoint
}

#[test]
fn native_restart_repairs_erased_source_and_reopens_through_both_sql_engines() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.db");
    let image = source_image(&source_path);
    let (manifest, packets) = transfer(&image, true);
    assert!(packets.iter().all(|packet| packet.esi != 0));
    assert!(packets[0].k_source > 2);
    run(async {
        let cx = cx(); let vfs = UnixVfs::new();
        let journal = directory.path().join("transfer.spool");
        let destination = directory.path().join("restored.db");
        // Save only two equations, close EVERY spool handle, then continue
        // after replay without retransmitting those saved equations.
        let partial = saved_journal(&vfs, &cx, &journal, &manifest, &packets[..2]).await;
        let mut spool = SnapshotSpool::open(&cx, open(&vfs, &cx, &journal), receiver(&manifest), CAP, Some(partial)).await.unwrap();
        while spool.replay_next(&cx).await.unwrap().is_some() {}
        assert!(!spool.receiver().is_complete());
        let mut repair_packets = 0;
        for packet in &packets[2..] {
            if !packet.is_source_symbol() { repair_packets += 1; }
            spool.append(&cx, packet).await.unwrap();
            // Receiving and applying are separate; deliberately discard all
            // decoded output so the next reopen has to reconstruct it again.
            drop(spool.take_decoded_blocks());
            if spool.receiver().is_complete() { break; }
        }
        assert!(repair_packets > 0 && spool.receiver().is_complete());
        let required = spool.checkpoint(&cx).unwrap();
        drop(spool.into_file());
        let journal_before = std::fs::read(&journal).unwrap();
        let mut spool = SnapshotSpool::open(&cx, open(&vfs, &cx, &journal), receiver(&manifest), CAP, Some(required)).await.unwrap();
        let mut writer = SnapshotImageWriter::create(&cx, open(&vfs, &cx, &destination), manifest.clone(), manifest.id(), image.len() as u64).unwrap();
        let receipt = spool.replay_into_image(&cx, &mut writer).await.unwrap();
        assert_eq!(receipt.image_blake3, *blake3::hash(&image).as_bytes());
        assert_eq!(receipt.byte_len, image.len() as u64);
        assert_eq!(writer.state(), SnapshotImageState::Verified);
        drop(writer.into_file());
        drop(spool.into_file());
        assert_eq!(std::fs::read(&journal).unwrap(), journal_before);
        assert_eq!(std::fs::read(&destination).unwrap(), image);
        assert_eq!(std::fs::read(&source_path).unwrap(), image);

        let stock = rusqlite::Connection::open_with_flags(&destination, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(stock.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0)).unwrap(), "ok");
        assert_eq!(stock.query_row("SELECT count(*),sum(id),sum(length(payload)) FROM items", [], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?))
        }).unwrap(), (64, 2080, 6144));
        drop(stock);

        let conn = Connection::open(destination.to_str().unwrap()).await.unwrap();
        let row = conn.query_row("SELECT count(*),sum(id) FROM items").await.unwrap();
        assert_eq!(row.get(0), Some(&SqliteValue::Integer(64)));
        assert_eq!(row.get(1), Some(&SqliteValue::Integer(2080)));
        let version = conn.query_row("PRAGMA user_version").await.unwrap();
        assert_eq!(version.get(0), Some(&SqliteValue::Integer(37)));
        conn.execute("INSERT INTO items VALUES (65,'item-065',zeroblob(96))").await.unwrap();
        conn.close().await.unwrap();
        let stock = rusqlite::Connection::open_with_flags(&destination, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(stock.query_row("SELECT count(*) FROM items", [], |row| row.get::<_, i64>(0)).unwrap(), 65);
        assert_eq!(stock.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0)).unwrap(), "ok");
    });
}

#[test]
fn complete_prefix_with_torn_unacknowledged_tail_restores_without_repairing_source() {
    let directory = tempfile::tempdir().unwrap();
    let image = source_image(&directory.path().join("source.db"));
    let (manifest, packets) = transfer(&image, false);
    run(async {
        let cx = cx(); let vfs = MemoryVfs::new(); let journal = Path::new("torn");
        let required = saved_journal(&vfs, &cx, journal, &manifest, &packets).await;
        let raw = open(&vfs, &cx, journal);
        raw.write(&cx, &[1, 2, 3], required.end_offset).await.unwrap();
        let length = raw.file_size(&cx).unwrap();
        let mut spool = SnapshotSpool::open(&cx, open(&vfs, &cx, journal), receiver(&manifest), CAP, Some(required)).await.unwrap();
        let mut writer = SnapshotImageWriter::create(&cx, open(&vfs, &cx, Path::new("output")), manifest.clone(), manifest.id(), CAP).unwrap();
        let receipt = spool.replay_into_image(&cx, &mut writer).await.unwrap();
        assert_eq!(receipt.image_blake3, *blake3::hash(&image).as_bytes());
        assert_eq!(spool.state(), SnapshotSpoolState::TornTail);
        assert_eq!(raw.file_size(&cx).unwrap(), length);
        let mut tail = [0; 3];
        raw.read(&cx, &mut tail, required.end_offset).await.unwrap();
        assert_eq!(tail, [1, 2, 3]);
    });
}

#[test]
fn wrong_required_checkpoint_cannot_be_bypassed_by_finalizing_the_image() {
    let directory = tempfile::tempdir().unwrap();
    let image = source_image(&directory.path().join("source.db"));
    let (manifest, packets) = transfer(&image, false);
    run(async {
        let cx = cx(); let vfs = MemoryVfs::new(); let journal = Path::new("wrong-receipt");
        let mut required = saved_journal(&vfs, &cx, journal, &manifest, &packets).await;
        required.chain_hash[0] ^= 1;
        let mut spool = SnapshotSpool::open(&cx, open(&vfs, &cx, journal), receiver(&manifest), CAP, Some(required)).await.unwrap();
        let target = Path::new("rejected-image");
        let mut writer = SnapshotImageWriter::create(&cx, open(&vfs, &cx, target), manifest.clone(), manifest.id(), CAP).unwrap();
        assert!(spool.replay_into_image(&cx, &mut writer).await.is_err());
        assert_eq!(writer.state(), SnapshotImageState::Poisoned);
        assert!(writer.finish(&cx).await.is_err());
        let mut magic = [0; 16];
        open(&vfs, &cx, target).read(&cx, &mut magic, 0).await.unwrap();
        assert_ne!(&magic, b"SQLite format 3\0");
    });
}

#[test]
fn incomplete_saved_snapshot_keeps_output_unpublished() {
    let directory = tempfile::tempdir().unwrap();
    let image = source_image(&directory.path().join("source.db"));
    let (manifest, packets) = transfer(&image, false);
    run(async {
        let cx = cx(); let vfs = MemoryVfs::new(); let journal = Path::new("incomplete");
        let required = saved_journal(&vfs, &cx, journal, &manifest, &packets[..1]).await;
        let mut spool = SnapshotSpool::open(&cx, open(&vfs, &cx, journal), receiver(&manifest), CAP, Some(required)).await.unwrap();
        let target = Path::new("partial-image");
        let mut writer = SnapshotImageWriter::create(&cx, open(&vfs, &cx, target), manifest.clone(), manifest.id(), CAP).unwrap();
        assert!(spool.replay_into_image(&cx, &mut writer).await.is_err());
        assert_eq!(writer.state(), SnapshotImageState::Poisoned);
        assert_eq!(open(&vfs, &cx, target).file_size(&cx).unwrap(), 0);
    });
}

#[test]
fn mismatched_spool_and_target_are_refused_before_replay_or_writes() {
    let directory = tempfile::tempdir().unwrap();
    let image = source_image(&directory.path().join("source.db"));
    let (manifest, packets) = transfer(&image, false);
    let mut other_image = image;
    *other_image.last_mut().unwrap() ^= 1;
    let (other, _) = transfer(&other_image, false);
    run(async {
        let cx = cx(); let vfs = MemoryVfs::new(); let journal = Path::new("mismatch");
        let required = saved_journal(&vfs, &cx, journal, &manifest, &packets).await;
        let mut spool = SnapshotSpool::open(&cx, open(&vfs, &cx, journal), receiver(&manifest), CAP, Some(required)).await.unwrap();
        let target = Path::new("other-image");
        let mut writer = SnapshotImageWriter::create(&cx, open(&vfs, &cx, target), other.clone(), other.id(), CAP).unwrap();
        assert!(spool.replay_into_image(&cx, &mut writer).await.is_err());
        assert_eq!(spool.record_count(), 0);
        assert_eq!(writer.blocks_applied(), 0);
        assert_eq!(writer.state(), SnapshotImageState::Writing);
        assert_eq!(open(&vfs, &cx, target).file_size(&cx).unwrap(), 0);
    });
}

#[test]
fn already_consumed_journal_cannot_skip_missing_image_blocks() {
    let directory = tempfile::tempdir().unwrap();
    let image = source_image(&directory.path().join("source.db"));
    let (manifest, packets) = transfer(&image, false);
    run(async {
        let cx = cx(); let vfs = MemoryVfs::new(); let journal = Path::new("consumed");
        let required = saved_journal(&vfs, &cx, journal, &manifest, &packets).await;
        let mut spool = SnapshotSpool::open(&cx, open(&vfs, &cx, journal), receiver(&manifest), CAP, Some(required)).await.unwrap();
        while spool.replay_next(&cx).await.unwrap().is_some() {
            drop(spool.take_decoded_blocks());
        }
        assert!(spool.receiver().is_complete());
        let target = Path::new("not-restored");
        let mut writer = SnapshotImageWriter::create(&cx, open(&vfs, &cx, target), manifest.clone(), manifest.id(), CAP).unwrap();
        assert!(spool.replay_into_image(&cx, &mut writer).await.is_err());
        assert_eq!(writer.blocks_applied(), 0);
        assert_eq!(writer.state(), SnapshotImageState::Writing);
        assert_eq!(open(&vfs, &cx, target).file_size(&cx).unwrap(), 0);
    });
}
