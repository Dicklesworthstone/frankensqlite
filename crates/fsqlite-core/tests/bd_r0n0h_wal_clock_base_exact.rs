#![recursion_limit = "256"]

//! bd-r0n0h (GH#429 follow-up): exact WAL checkpoint header stamping and
//! clock-base derivation across frame-copying checkpoints, fresh opens,
//! long-lived open connections, and stock SQLite interoperability.

use std::path::Path;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn open_wal(path: &str) -> Connection {
    let conn = Connection::open(path).await.expect("open fsqlite connection");
    conn.query("PRAGMA journal_mode = WAL;")
        .await
        .expect("enable WAL");
    conn.execute("PRAGMA wal_autocheckpoint = 0;")
        .await
        .expect("disable autocheckpoint");
    conn
}

async fn count_rows(conn: &Connection) -> i64 {
    let rows = conn
        .query("SELECT COUNT(*) FROM t;")
        .await
        .expect("SELECT COUNT(*) FROM t");
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer count, got {other:?}"),
    }
}

async fn query_all_rows(conn: &Connection) -> Vec<(i64, i64)> {
    let rows = conn
        .query("SELECT id, v FROM t ORDER BY id;")
        .await
        .expect("SELECT id, v FROM t");
    rows.into_iter()
        .map(|row| match (row.values()[0].clone(), row.values()[1].clone()) {
            (SqliteValue::Integer(id), SqliteValue::Integer(v)) => (id, v),
            other => panic!("unexpected row values: {other:?}"),
        })
        .collect()
}

async fn probe_snapshot_seq(conn: &Connection) -> u64 {
    conn.execute("BEGIN;").await.expect("BEGIN");
    let seq = conn
        .current_concurrent_snapshot_seq()
        .expect("BEGIN must expose concurrent snapshot seq");
    conn.execute("ROLLBACK;").await.expect("ROLLBACK");
    seq
}

fn header_counters(path: &Path) -> (u32, u32) {
    let bytes = std::fs::read(path).expect("read database file");
    assert!(bytes.len() >= 100, "database file too short for header");
    let change_counter = u32::from_be_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]);
    let version_valid_for = u32::from_be_bytes([bytes[92], bytes[93], bytes[94], bytes[95]]);
    (change_counter, version_valid_for)
}

fn assert_rusqlite_parity(path: &Path, expected_rows: &[(i64, i64)]) {
    let stock = rusqlite::Connection::open(path).expect("open with bundled rusqlite");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .expect("rusqlite integrity_check");
    assert_eq!(integrity, "ok", "rusqlite PRAGMA integrity_check must pass");

    let mut stmt = stock
        .prepare("SELECT id, v FROM t ORDER BY id;")
        .expect("prepare rusqlite select");
    let stock_rows: Vec<(i64, i64)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query rusqlite rows")
        .map(|r| r.expect("decode rusqlite row"))
        .collect();
    assert_eq!(
        stock_rows, expected_rows,
        "stock SQLite rows must match FrankenSQLite rows"
    );
}

/// Defect #2 live repro on real `Connection::open`:
///
/// 1. `conn_a` commits 6 transactions in WAL generation G1 and runs
///    `PRAGMA wal_checkpoint(PASSIVE)` (stamping main DB header `change_counter`
///    while G1 remains active).
/// 2. Fresh `conn_b` opens and binds to G1 via a read.
/// 3. `conn_a` commits 2 more transactions and runs `PRAGMA wal_checkpoint(TRUNCATE)`.
/// 4. `conn_b` must advance cleanly to `conn_a`'s exact snapshot sequence and
///    update a row written by `conn_a` without hitting `BusySnapshot`.
#[test]
fn passive_checkpoint_fresh_open_then_peer_truncate_no_busy_snapshot() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("passive_open_truncate.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn_a = open_wal(&db_str).await;
        conn_a
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);")
            .await
            .expect("create table");
        for id in 1..=6_i64 {
            conn_a
                .execute(&format!("INSERT INTO t VALUES ({id}, {});", id * 10))
                .await
                .expect("seed insert");
        }

        let seq_a_before_passive = probe_snapshot_seq(&conn_a).await;
        let ckpt_passive = conn_a
            .query("PRAGMA wal_checkpoint(PASSIVE);")
            .await
            .expect("PASSIVE checkpoint");
        assert_eq!(ckpt_passive[0].values()[0], SqliteValue::Integer(0));

        // Fresh connection opens mid-generation after the PASSIVE checkpoint
        // stamped the main DB header with the already-committed WAL horizon.
        let conn_b = open_wal(&db_str).await;
        assert_eq!(count_rows(&conn_b).await, 6);
        let seq_b_after_passive = probe_snapshot_seq(&conn_b).await;
        assert_eq!(
            seq_b_after_passive, seq_a_before_passive,
            "fresh connection opening after PASSIVE checkpoint must bind to the exact same commit_seq (no double-count)"
        );

        // Peer commits 2 more transactions and resets the WAL via TRUNCATE.
        conn_a
            .execute("INSERT INTO t VALUES (7, 70);")
            .await
            .expect("insert 7");
        conn_a
            .execute("INSERT INTO t VALUES (8, 80);")
            .await
            .expect("insert 8");
        let seq_a_after_more = probe_snapshot_seq(&conn_a).await;
        assert_eq!(seq_a_after_more, seq_a_before_passive + 2);

        let ckpt_truncate = conn_a
            .query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("TRUNCATE checkpoint");
        assert_eq!(ckpt_truncate[0].values()[0], SqliteValue::Integer(0));

        let seq_a_after_truncate = probe_snapshot_seq(&conn_a).await;
        let seq_b_after_truncate = probe_snapshot_seq(&conn_b).await;
        assert_eq!(seq_a_after_truncate, seq_a_after_more);
        assert_eq!(
            seq_b_after_truncate, seq_a_after_truncate,
            "conn_b must not lag behind conn_a after PASSIVE + peer commits + TRUNCATE"
        );

        // Load-bearing user contract: conn_b must be able to update the page
        // committed by conn_a right before TRUNCATE without BusySnapshot.
        conn_b
            .execute("UPDATE t SET v = 999 WHERE id = 7;")
            .await
            .expect("conn_b UPDATE after PASSIVE + peer TRUNCATE must not BusySnapshot");
        conn_b
            .execute("INSERT INTO t VALUES (100, 1000);")
            .await
            .expect("conn_b INSERT after PASSIVE + peer TRUNCATE must succeed");

        let rows_a = query_all_rows(&conn_a).await;
        let rows_b = query_all_rows(&conn_b).await;
        assert_eq!(rows_a, rows_b);
        assert_eq!(rows_b.len(), 9);
        assert!(rows_b.contains(&(7, 999)));
        assert!(rows_b.contains(&(100, 1000)));

        let (cc, vvf) = header_counters(&db_path);
        assert_eq!(cc, vvf, "header change_counter must match version_valid_for");

        conn_b.close().await.expect("close conn_b");
        conn_a.close().await.expect("close conn_a");
        assert_rusqlite_parity(&db_path, &rows_b);
    });
}

/// Repeated `(commit + PASSIVE checkpoint + fresh open)` cycles on real
/// `Connection::open` must advance the commit clock and on-disk header counter
/// strictly by 1 per commit (zero double-count drift and zero open-connection
/// lag), and remain fully writable across subsequent TRUNCATE.
#[test]
fn repeated_passive_checkpoints_and_reopens_keep_exact_clock_and_header() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("repeated_passive_drift.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let long_lived = open_wal(&db_str).await;
        long_lived
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);")
            .await
            .expect("create table");
        for id in 1..=5_i64 {
            long_lived
                .execute(&format!("INSERT INTO t VALUES ({id}, {id});"))
                .await
                .expect("seed insert");
        }
        let mut expected_seq = probe_snapshot_seq(&long_lived).await;

        let mut expected_rows: Vec<(i64, i64)> = (1..=5).map(|i| (i, i)).collect();

        for cycle in 1..=10_i64 {
            let ckpt = long_lived
                .query("PRAGMA wal_checkpoint(PASSIVE);")
                .await
                .expect("PASSIVE checkpoint");
            assert_eq!(ckpt[0].values()[0], SqliteValue::Integer(0));

            let (cc, vvf) = header_counters(&db_path);
            assert_eq!(cc, vvf, "cycle {cycle}: version_valid_for mismatch");
            assert_eq!(
                u64::from(cc),
                expected_seq,
                "cycle {cycle}: PASSIVE checkpoint must stamp exact horizon without drift"
            );

            let fresh = open_wal(&db_str).await;
            assert_eq!(count_rows(&fresh).await, expected_rows.len() as i64);

            let fresh_seq = probe_snapshot_seq(&fresh).await;
            let open_seq = probe_snapshot_seq(&long_lived).await;
            assert_eq!(
                fresh_seq, expected_seq,
                "cycle {cycle}: fresh connection drifted upward after PASSIVE checkpoint"
            );
            assert_eq!(
                open_seq, fresh_seq,
                "cycle {cycle}: long-lived open connection lagged behind fresh connection"
            );

            let next_id = 5 + cycle;
            fresh
                .execute(&format!("INSERT INTO t VALUES ({next_id}, {});", next_id * 2))
                .await
                .expect("fresh connection commit");
            expected_rows.push((next_id, next_id * 2));
            expected_seq += 1;

            let open_after_commit = probe_snapshot_seq(&long_lived).await;
            assert_eq!(
                open_after_commit, expected_seq,
                "cycle {cycle}: long-lived connection must track peer commit exactly"
            );
            fresh
                .close_without_checkpoint()
                .await
                .expect("close fresh without checkpoint");
        }

        let trunc = long_lived
            .query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("final TRUNCATE");
        assert_eq!(trunc[0].values()[0], SqliteValue::Integer(0));
        let (final_cc, final_vvf) = header_counters(&db_path);
        assert_eq!(final_cc, final_vvf);
        assert_eq!(u64::from(final_cc), expected_seq);

        long_lived
            .execute("UPDATE t SET v = -1 WHERE id = 15;")
            .await
            .expect("update after TRUNCATE");
        expected_rows
            .iter_mut()
            .find(|(id, _)| *id == 15)
            .expect("row 15")
            .1 = -1;

        assert_eq!(query_all_rows(&long_lived).await, expected_rows);
        long_lived.close().await.expect("close long_lived");
        assert_rusqlite_parity(&db_path, &expected_rows);
    });
}

/// Defect #1 live repro on real on-disk `-wal` + `-shm` WITHOUT a parallel-WAL
/// certificate sidecar (`SimplePager<UnixVfs>` + `WalBackendAdapter` + shared
/// `-shm` index):
///
/// Repeated `(PASSIVE checkpoint + reopen + commit)` cycles must NOT drift the
/// clock base upward by `physical_wal_visible_commit_count` per cycle, and a
/// long-lived open pager must stay in exact lockstep with freshly opened pagers.
#[cfg(all(feature = "native", unix))]
#[test]
fn no_cert_passive_checkpoint_cycles_do_not_drift_or_lag_open_pager() {
    use fsqlite_core::wal_adapter::WalBackendAdapter;
    use fsqlite_pager::{
        CheckpointMode, JournalMode, MvccPager, SimplePager, TransactionHandle, TransactionMode,
    };
    use fsqlite_types::cx::Cx;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_types::{PageNumber, PageSize};
    use fsqlite_vfs::{UnixVfs, Vfs};
    use fsqlite_wal::{WalFile, WalSalts};

    async fn open_no_cert_wal_pager(
        cx: &Cx,
        vfs: &UnixVfs,
        db_path: &Path,
        wal_path: &Path,
        create_wal: bool,
    ) -> SimplePager<UnixVfs> {
        let pager = SimplePager::open_with_cx(cx, UnixVfs::new(), db_path, PageSize::DEFAULT)
            .await
            .expect("open SimplePager");
        let wal_flags = if create_wal {
            VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL
        } else {
            VfsOpenFlags::READWRITE | VfsOpenFlags::WAL
        };
        let (wal_file, _) = vfs.open(cx, Some(wal_path), wal_flags).expect("open wal");
        let wal = if create_wal {
            WalFile::create(
                cx,
                wal_file,
                PageSize::DEFAULT.get(),
                0,
                WalSalts {
                    salt1: 0x1357_2468,
                    salt2: 0x9ABC_DEF0,
                },
            )
            .await
            .expect("create WalFile")
        } else {
            WalFile::open(cx, wal_file).await.expect("open WalFile")
        };
        let mut adapter = WalBackendAdapter::new(wal);
        let shm_source = pager
            .wal_index_shm_source()
            .expect("pager wal_index_shm_source");
        adapter
            .attach_wal_index_shm_source(shm_source)
            .expect("attach shm source");
        pager
            .set_wal_backend(Box::new(adapter))
            .expect("install WalBackendAdapter");
        pager
            .set_journal_mode(cx, JournalMode::Wal)
            .await
            .expect("enable WAL mode");
        pager
    }

    asupersync::test_utils::run_test(|| async {
        let cx = Cx::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("no_cert_passive.db");
        let wal_path = dir.path().join("no_cert_passive.db-wal");
        let vfs = UnixVfs::new();
        let page_size = PageSize::DEFAULT.as_usize();

        let open_pager = open_no_cert_wal_pager(&cx, &vfs, &db_path, &wal_path, true).await;
        // Allocate page 2 first so later commits overwrite page 2 cleanly.
        {
            let mut txn = open_pager
                .begin(&cx, TransactionMode::Immediate)
                .await
                .expect("begin initial alloc");
            let p2 = txn.allocate_page(&cx).await.expect("allocate page 2");
            assert_eq!(p2, PageNumber::new(2).unwrap());
            txn.write_page(&cx, p2, &vec![0x11; page_size])
                .await
                .expect("write page 2");
            txn.commit(&cx).await.expect("commit page 2");
        }
        // Commit 4 more times in the same WAL generation (5 commits total).
        for step in 1..=4_u8 {
            let mut txn = open_pager
                .begin(&cx, TransactionMode::Immediate)
                .await
                .expect("begin seed write");
            txn.write_page(&cx, PageNumber::new(2).unwrap(), &vec![0x20 + step; page_size])
                .await
                .expect("write seed page 2");
            txn.commit(&cx).await.expect("commit seed write");
        }

        let mut expected_seq = open_pager.published_snapshot().visible_commit_seq.get();

        for cycle in 1..=8_u8 {
            let res = open_pager
                .checkpoint(&cx, CheckpointMode::Passive)
                .await
                .expect("PASSIVE checkpoint");
            assert!(
                !res.wal_was_reset,
                "cycle {cycle}: PASSIVE checkpoint must not reset WAL"
            );

            let (cc, vvf) = header_counters(&db_path);
            assert_eq!(cc, vvf, "cycle {cycle}: header change_counter != version_valid_for");
            assert_eq!(
                u64::from(cc),
                expected_seq,
                "cycle {cycle}: PASSIVE checkpoint must stamp exact horizon"
            );

            // Open a fresh pager on the exact same DB + WAL + SHM (no cert!).
            let fresh = open_no_cert_wal_pager(&cx, &vfs, &db_path, &wal_path, false).await;
            {
                let reader = fresh
                    .begin(&cx, TransactionMode::ReadOnly)
                    .await
                    .expect("fresh read begin");
                let _ = reader
                    .get_page(&cx, PageNumber::new(2).unwrap())
                    .await
                    .expect("fresh read page 2");
                drop(reader);
            }
            {
                let reader = open_pager
                    .begin(&cx, TransactionMode::ReadOnly)
                    .await
                    .expect("open_pager read begin");
                let _ = reader
                    .get_page(&cx, PageNumber::new(2).unwrap())
                    .await
                    .expect("open_pager read page 2");
                drop(reader);
            }

            let fresh_seq = fresh.published_snapshot().visible_commit_seq.get();
            let open_seq = open_pager.published_snapshot().visible_commit_seq.get();
            assert_eq!(
                fresh_seq, expected_seq,
                "cycle {cycle}: no-cert fresh pager must not double-count backfilled WAL commits \
                 (got {fresh_seq}, expected {expected_seq})"
            );
            assert_eq!(
                open_seq, fresh_seq,
                "cycle {cycle}: open pager ({open_seq}) must not lag behind fresh pager ({fresh_seq})"
            );

            // Commit 1 more transaction from the fresh pager.
            {
                let mut txn = fresh
                    .begin(&cx, TransactionMode::Immediate)
                    .await
                    .expect("fresh write begin");
                txn.write_page(&cx, PageNumber::new(2).unwrap(), &vec![0x50 + cycle; page_size])
                    .await
                    .expect("fresh write page 2");
                txn.commit(&cx).await.expect("fresh commit");
            }
            expected_seq += 1;
            assert_eq!(
                fresh.published_snapshot().visible_commit_seq.get(),
                expected_seq,
                "cycle {cycle}: fresh commit must advance by exactly 1"
            );
            drop(fresh);

            // Long-lived open pager refreshes and must match exact expected_seq.
            {
                let reader = open_pager
                    .begin(&cx, TransactionMode::ReadOnly)
                    .await
                    .expect("open_pager post-commit begin");
                let page2 = reader
                    .get_page(&cx, PageNumber::new(2).unwrap())
                    .await
                    .expect("open_pager read updated page 2");
                assert_eq!(page2.as_ref()[0], 0x50 + cycle);
                drop(reader);
            }
            assert_eq!(
                open_pager.published_snapshot().visible_commit_seq.get(),
                expected_seq,
                "cycle {cycle}: open_pager must track fresh commit without drift or lag"
            );
        }

        // Finally TRUNCATE and verify both open and fresh pagers converge on expected_seq.
        let trunc = open_pager
            .checkpoint(&cx, CheckpointMode::Truncate)
            .await
            .expect("TRUNCATE checkpoint");
        assert!(trunc.wal_was_reset, "TRUNCATE must reset WAL");
        let (final_cc, final_vvf) = header_counters(&db_path);
        assert_eq!(final_cc, final_vvf);
        assert_eq!(u64::from(final_cc), expected_seq);

        let after_trunc = open_no_cert_wal_pager(&cx, &vfs, &db_path, &wal_path, false).await;
        {
            let reader = open_pager
                .begin(&cx, TransactionMode::ReadOnly)
                .await
                .expect("open_pager after truncate");
            drop(reader);
            let reader2 = after_trunc
                .begin(&cx, TransactionMode::ReadOnly)
                .await
                .expect("after_trunc read");
            drop(reader2);
        }
        assert_eq!(
            open_pager.published_snapshot().visible_commit_seq.get(),
            expected_seq
        );
        assert_eq!(
            after_trunc.published_snapshot().visible_commit_seq.get(),
            expected_seq
        );
    });
}

/// Stock SQLite (`rusqlite`) interoperability: when stock SQLite commits in WAL
/// mode and runs `PRAGMA wal_checkpoint(PASSIVE)` (updating `-shm` `nBackfill`
/// without bumping the main DB header `change_counter`), FrankenSQLite must
/// open cleanly, keep a stable commit sequence across consecutive no-op reads
/// (no second-read jump), commit cleanly, and preserve stock integrity.
#[test]
fn stock_sqlite_passive_checkpoint_interop_no_second_read_jump() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("stock_passive_interop.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // Stock SQLite creates WAL DB, commits 5 rows, and runs PASSIVE checkpoint
        // without resetting the WAL (keep another read/write lifecycle clean).
        {
            let stock = rusqlite::Connection::open(&db_path).expect("open rusqlite");
            stock
                .pragma_update(None, "journal_mode", "WAL")
                .expect("stock WAL");
            stock
                .pragma_update(None, "wal_autocheckpoint", 0)
                .expect("stock autocheckpoint=0");
            stock
                .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL);", [])
                .expect("stock create table");
            for id in 1..=5_i64 {
                stock
                    .execute("INSERT INTO t VALUES (?1, ?2);", rusqlite::params![id, id * 10])
                    .expect("stock insert");
            }
            let (busy, log, ckpt): (i64, i64, i64) = stock
                .query_row("PRAGMA wal_checkpoint(PASSIVE);", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .expect("stock PASSIVE checkpoint");
            assert_eq!(busy, 0);
            assert!(log > 0 && ckpt == log, "stock PASSIVE should backfill all frames: log={log} ckpt={ckpt}");
            // Note: closing the last rusqlite connection normally deletes/checkpoints
            // WAL unless SQLITE_FCNTL_PERSIST_WAL is set, so keep `stock` open across
            // FrankenSQLite's reads and writes!
            let fconn = open_wal(&db_str).await;
            assert_eq!(count_rows(&fconn).await, 5);
            let seq_read_1 = probe_snapshot_seq(&fconn).await;
            let seq_read_2 = probe_snapshot_seq(&fconn).await;
            assert_eq!(
                seq_read_1, seq_read_2,
                "consecutive no-op reads over stock-PASSIVE-checkpointed WAL must not jump commit_seq"
            );

            fconn
                .execute("INSERT INTO t VALUES (6, 60);")
                .await
                .expect("fsqlite insert over stock-checkpointed WAL");
            let seq_after_insert = probe_snapshot_seq(&fconn).await;
            assert_eq!(seq_after_insert, seq_read_2 + 1);
            fconn
                .close_without_checkpoint()
                .await
                .expect("close fsqlite connection");
        }

        let expected: Vec<(i64, i64)> = (1..=6).map(|i| (i, i * 10)).collect();
        assert_rusqlite_parity(&db_path, &expected);
    });
}
