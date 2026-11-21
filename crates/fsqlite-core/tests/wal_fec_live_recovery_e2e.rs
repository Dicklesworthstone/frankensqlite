#![recursion_limit = "512"]

//! End-to-end verification of live automatic WAL-FEC self-healing recovery on
//! ordinary public `Connection::open` (§3.4.1, bd-1hi.11).
#![cfg(all(feature = "native", not(target_arch = "wasm32")))]

use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use fsqlite_wal::{
    WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WalHeader, raptorq_repair_metrics_snapshot,
    scan_wal_fec, wal_fec_path_for_wal,
};

fn run_with_repair_pool<F: std::future::Future<Output = ()>>(test: F) {
    let runtime = asupersync::runtime::RuntimeBuilder::current_thread()
        .blocking_threads(1, 2)
        .build()
        .expect("build runtime with blocking pool");
    runtime.block_on(test);
}

fn wal_path(db: &Path) -> PathBuf {
    let mut os = db.as_os_str().to_os_string();
    os.push("-wal");
    PathBuf::from(os)
}

fn shm_path(db: &Path) -> PathBuf {
    let mut os = db.as_os_str().to_os_string();
    os.push("-shm");
    PathBuf::from(os)
}

async fn open_wal_db(db: &Path) -> Connection {
    let conn = Connection::open(db.to_str().unwrap()).await.unwrap();
    conn.execute("PRAGMA journal_mode = WAL;").await.unwrap();
    conn.execute("PRAGMA synchronous = FULL;").await.unwrap();
    conn.execute("PRAGMA wal_autocheckpoint = 0;").await.unwrap();
    assert!(
        conn.is_concurrent_mode_default(),
        "concurrent_mode_default must remain true"
    );
    conn
}

/// Flip a byte inside the payload of one-based WAL frame `frame_no`.
fn corrupt_wal_frame_payload(wal: &Path, frame_no: u32, byte_offset_in_page: usize) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(wal)
        .expect("open wal for corruption");
    let mut hdr_bytes = [0_u8; WAL_HEADER_SIZE];
    file.read_exact(&mut hdr_bytes).expect("read wal header");
    let header = WalHeader::from_bytes(&hdr_bytes).expect("parse wal header");
    let page_size = usize::try_from(header.page_size).unwrap();
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    let frame_idx = usize::try_from(frame_no - 1).unwrap();
    let offset = WAL_HEADER_SIZE
        + frame_idx * frame_size
        + WAL_FRAME_HEADER_SIZE
        + byte_offset_in_page.min(page_size - 1);
    let mut byte = [0_u8; 1];
    file.seek(SeekFrom::Start(u64::try_from(offset).unwrap()))
        .unwrap();
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0x5A;
    file.seek(SeekFrom::Start(u64::try_from(offset).unwrap()))
        .unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

/// Flip a byte inside the salt field (bytes 8..16) of one-based WAL frame `frame_no`.
fn corrupt_wal_frame_salt(wal: &Path, frame_no: u32) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(wal)
        .expect("open wal for salt corruption");
    let mut hdr_bytes = [0_u8; WAL_HEADER_SIZE];
    file.read_exact(&mut hdr_bytes).expect("read wal header");
    let header = WalHeader::from_bytes(&hdr_bytes).expect("parse wal header");
    let page_size = usize::try_from(header.page_size).unwrap();
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    let frame_idx = usize::try_from(frame_no - 1).unwrap();
    let offset = WAL_HEADER_SIZE + frame_idx * frame_size + 8;
    let mut byte = [0_u8; 1];
    file.seek(SeekFrom::Start(u64::try_from(offset).unwrap()))
        .unwrap();
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xA5;
    file.seek(SeekFrom::Start(u64::try_from(offset).unwrap()))
        .unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn test_e2e_bd_1hi_11_compliance() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("live_heal.db");
        let wal = wal_path(&db);
        let sidecar = wal_fec_path_for_wal(&wal);

        let conn = open_wal_db(&db).await;
        // Use R=3 repair symbols so 2-frame corruption still leaves K+1 symbols
        // for deterministic RFC 6330 recovery.
        conn.execute("PRAGMA raptorq_repair_symbols = 3;")
            .await
            .unwrap();
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, body BLOB);")
            .await
            .unwrap();
        conn.execute("BEGIN;").await.unwrap();
        for id in 1..=12 {
            conn.execute(&format!(
                "INSERT INTO items VALUES ({id}, zeroblob(3000));"
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();
        // Drain background WAL-FEC worker without checkpointing the WAL.
        conn.close_without_checkpoint().await.unwrap();

        let scan = scan_wal_fec(&sidecar).expect("scan sidecar");
        let group = scan
            .groups
            .iter()
            .find(|g| g.meta.k_source >= 4)
            .expect("multi-frame FEC group must exist");
        let target_frame_a = group.meta.start_frame_no;
        let target_frame_b = group.meta.start_frame_no + 1;

        // Corrupt 2 distinct frames within R=2 repair budget (leaving commit
        // frame terminal checksum intact as anchor).
        corrupt_wal_frame_payload(&wal, target_frame_a, 128);
        corrupt_wal_frame_payload(&wal, target_frame_b, 256);

        let metrics_before = raptorq_repair_metrics_snapshot();

        // Ordinary public Connection::open MUST automatically heal the WAL via
        // `.wal-fec` and recover every committed row.
        let reopened = Connection::open(db.to_str().unwrap()).await.unwrap();
        assert_eq!(
            reopened.query("SELECT COUNT(*) FROM items;").await.unwrap()[0].values(),
            &[SqliteValue::Integer(12)]
        );
        assert_eq!(
            reopened.query("PRAGMA integrity_check;").await.unwrap()[0].values(),
            &[SqliteValue::Text("ok".into())]
        );

        let metrics_after = raptorq_repair_metrics_snapshot();
        assert!(
            metrics_after.repairs_total > metrics_before.repairs_total,
            "live WAL-FEC recovery must record telemetry"
        );
        assert!(
            metrics_after.symbols_reclaimed > metrics_before.symbols_reclaimed,
            "live WAL-FEC recovery must record reclaimed symbols"
        );

        // Checkpoint and prove stock SQLite reads the healed database cleanly.
        reopened
            .query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .unwrap();
        reopened.close().await.unwrap();

        let stock = rusqlite::Connection::open(&db).unwrap();
        assert_eq!(
            stock
                .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        assert_eq!(
            stock
                .query_row("SELECT COUNT(*) FROM items", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            12
        );
    });
}

#[test]
fn test_e2e_bitflip_recovery_multi_group_and_salt_corruption() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("multi_group.db");
        let wal = wal_path(&db);
        let sidecar = wal_fec_path_for_wal(&wal);

        let conn = open_wal_db(&db).await;
        conn.execute("CREATE TABLE log(id INTEGER PRIMARY KEY, pad BLOB);")
            .await
            .unwrap();

        // Group 1: rows 1..=6.
        conn.execute("BEGIN;").await.unwrap();
        for id in 1..=6 {
            conn.execute(&format!("INSERT INTO log VALUES ({id}, zeroblob(3000));"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();

        // Group 2: rows 7..=12.
        conn.execute("BEGIN;").await.unwrap();
        for id in 7..=12 {
            conn.execute(&format!("INSERT INTO log VALUES ({id}, zeroblob(3000));"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();

        // Group 3: rows 13..=16 (left intact to verify replay continues across boundaries).
        conn.execute("BEGIN;").await.unwrap();
        for id in 13..=16 {
            conn.execute(&format!("INSERT INTO log VALUES ({id}, zeroblob(3000));"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();

        conn.close_without_checkpoint().await.unwrap();

        let scan = scan_wal_fec(&sidecar).expect("scan sidecar");
        let big_groups: Vec<_> = scan
            .groups
            .iter()
            .filter(|g| g.meta.k_source >= 3)
            .collect();
        assert!(
            big_groups.len() >= 2,
            "expected at least 2 multi-frame FEC groups, found {}",
            big_groups.len()
        );

        // Corrupt a frame salt in group 1 (tests SaltMismatch -> sidecar probe -> heal!)
        // and a frame payload in group 2, while leaving any existing -shm on disk.
        corrupt_wal_frame_salt(&wal, big_groups[0].meta.start_frame_no);
        corrupt_wal_frame_payload(&wal, big_groups[1].meta.start_frame_no, 512);

        let reopened = Connection::open(db.to_str().unwrap()).await.unwrap();
        assert_eq!(
            reopened.query("SELECT COUNT(*) FROM log;").await.unwrap()[0].values(),
            &[SqliteValue::Integer(16)]
        );
        assert_eq!(
            reopened.query("SELECT SUM(id) FROM log;").await.unwrap()[0].values(),
            &[SqliteValue::Integer(16 * 17 / 2)]
        );
        assert_eq!(
            reopened.query("PRAGMA integrity_check;").await.unwrap()[0].values(),
            &[SqliteValue::Text("ok".into())]
        );
        reopened.close().await.unwrap();
    });
}

#[test]
fn test_e2e_wal_fec_insufficient_symbols_falls_back_cleanly() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("insufficient.db");
        let wal = wal_path(&db);
        let sidecar = wal_fec_path_for_wal(&wal);

        let conn = open_wal_db(&db).await;
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, body BLOB);")
            .await
            .unwrap();
        // Group 1 (committed and intact): rows 1..=3.
        conn.execute("BEGIN;").await.unwrap();
        for id in 1..=3 {
            conn.execute(&format!(
                "INSERT INTO items VALUES ({id}, zeroblob(3000));"
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();

        // Group 2 (large enough to corrupt > R=2 frames): rows 4..=10.
        conn.execute("BEGIN;").await.unwrap();
        for id in 4..=10 {
            conn.execute(&format!(
                "INSERT INTO items VALUES ({id}, zeroblob(3000));"
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();
        conn.close_without_checkpoint().await.unwrap();

        let scan = scan_wal_fec(&sidecar).expect("scan sidecar");
        let last_group = scan.groups.last().expect("last group");
        assert!(
            last_group.meta.k_source >= 4,
            "last group needs >= 4 frames to exceed R=2"
        );

        // Corrupt 3 distinct frames in group 2 (> default R=2 repair budget).
        corrupt_wal_frame_payload(&wal, last_group.meta.start_frame_no, 64);
        corrupt_wal_frame_payload(&wal, last_group.meta.start_frame_no + 1, 64);
        corrupt_wal_frame_payload(&wal, last_group.meta.start_frame_no + 2, 64);
        // Remove stale -shm so recovery scans from WAL cleanly.
        let _ = fs::remove_file(shm_path(&db));
        let wal_before = fs::read(&wal).unwrap();

        // Negative control: reopen MUST fall back cleanly to Group 1 (3 rows),
        // MUST NOT corrupt the database, and MUST NOT alter the unrecoverable WAL.
        let reopened = Connection::open(db.to_str().unwrap()).await.unwrap();
        assert_eq!(
            reopened.query("SELECT COUNT(*) FROM items;").await.unwrap()[0].values(),
            &[SqliteValue::Integer(3)]
        );
        assert_eq!(
            reopened.query("PRAGMA integrity_check;").await.unwrap()[0].values(),
            &[SqliteValue::Text("ok".into())]
        );
        assert_eq!(
            fs::read(&wal).unwrap(),
            wal_before,
            "unrecoverable WAL fallback must not rewrite WAL bytes"
        );
        reopened.close_without_checkpoint().await.unwrap();
    });
}

#[test]
fn test_e2e_wal_fec_corrupt_sidecar_and_readonly_inert_negative_controls() {
    run_with_repair_pool(async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("neg_controls.db");
        let wal = wal_path(&db);
        let sidecar = wal_fec_path_for_wal(&wal);

        let conn = open_wal_db(&db).await;
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, body BLOB);")
            .await
            .unwrap();
        conn.execute("INSERT INTO items VALUES (1, zeroblob(3000));")
            .await
            .unwrap();
        conn.execute("BEGIN;").await.unwrap();
        for id in 2..=8 {
            conn.execute(&format!(
                "INSERT INTO items VALUES ({id}, zeroblob(3000));"
            ))
            .await
            .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();
        conn.close_without_checkpoint().await.unwrap();

        let scan = scan_wal_fec(&sidecar).expect("scan sidecar");
        let last_group = scan.groups.last().expect("last group");
        corrupt_wal_frame_payload(&wal, last_group.meta.start_frame_no, 128);
        let _ = fs::remove_file(shm_path(&db));

        // Negative control A: read-only open (`open_schema_only`) MUST remain
        // filesystem-inert on the WAL even when a valid `.wal-fec` sidecar
        // exists and could repair the WAL.
        let wal_before_ro = fs::read(&wal).unwrap();
        let ro = Connection::open_schema_only(db.to_str().unwrap())
            .await
            .unwrap();
        let ro_count = ro.query("SELECT COUNT(*) FROM items;").await.unwrap();
        assert_eq!(ro_count[0].values(), &[SqliteValue::Integer(1)]);
        ro.close_without_checkpoint().await.unwrap();
        assert_eq!(
            fs::read(&wal).unwrap(),
            wal_before_ro,
            "read-only open must not mutate damaged WAL bytes"
        );

        // Negative control B: corrupt the `.wal-fec` sidecar -> writable open
        // MUST fall back safely to SQLite pre-group prefix (1 row) without error.
        let saved_sidecar = fs::read(&sidecar).unwrap();
        fs::write(&sidecar, b"CORRUPTED_SIDECAR_PAYLOAD_BYTES").unwrap();
        let rw_corrupt_sidecar = Connection::open(db.to_str().unwrap()).await.unwrap();
        assert_eq!(
            rw_corrupt_sidecar
                .query("SELECT COUNT(*) FROM items;")
                .await
                .unwrap()[0]
                .values(),
            &[SqliteValue::Integer(1)]
        );
        assert_eq!(
            rw_corrupt_sidecar
                .query("PRAGMA integrity_check;")
                .await
                .unwrap()[0]
                .values(),
            &[SqliteValue::Text("ok".into())]
        );
        rw_corrupt_sidecar.close_without_checkpoint().await.unwrap();

        // Restore valid sidecar -> writable open now heals all 8 rows!
        fs::write(&sidecar, &saved_sidecar).unwrap();
        let rw_healed = Connection::open(db.to_str().unwrap()).await.unwrap();
        assert_eq!(
            rw_healed
                .query("SELECT COUNT(*) FROM items;")
                .await
                .unwrap()[0]
                .values(),
            &[SqliteValue::Integer(8)]
        );
        assert_eq!(
            rw_healed.query("PRAGMA integrity_check;").await.unwrap()[0].values(),
            &[SqliteValue::Text("ok".into())]
        );
        rw_healed.close().await.unwrap();
    });
}
