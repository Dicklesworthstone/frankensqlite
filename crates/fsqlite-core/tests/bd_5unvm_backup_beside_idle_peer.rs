#![recursion_limit = "512"]
#![cfg(unix)]

//! bd-5unvm: `Connection::backup_exact_to` must succeed while another process
//! has the WAL-mode source open and idle, as stock SQLite's backup does.
//!
//! Every 0.4.x connection keeps a main-file SHARED claim for its whole WAL
//! attachment. The byte-exact copy only reads the source, but it ran under
//! the whole-image EXCLUSIVE maintenance fence, which that idle claim
//! refuses, so the backup failed "database is busy" after busy_timeout
//! (GH#442 fixed the same fence for VACUUM INTO's source receipt).
//!
//! The test re-executes itself as the peer process: in peer mode it opens the
//! database, reads it, reports readiness on stdout and stays open until its
//! stdin closes.

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const PEER_ENV: &str = "FSQLITE_BD_5UNVM_PEER_DB";
const TEST: &str = "backup_exact_succeeds_beside_idle_peer_process";
const READY: &str = "BD_5UNVM_PEER_READY";

fn count_rows(rows: &[fsqlite_core::connection::Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected integer count, got {other:?}"),
    }
}

#[test]
fn backup_exact_succeeds_beside_idle_peer_process() {
    if let Some(db) = std::env::var_os(PEER_ENV) {
        let db = db.into_string().expect("utf-8 peer path");
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&db).await.expect("peer open");
            let rows = conn.query("SELECT count(*) FROM t;").await.expect("peer read");
            println!("{READY} {}", count_rows(&rows));
            std::io::stdout().flush().expect("flush readiness");
            let mut sink = Vec::new();
            let _ = std::io::stdin().read_to_end(&mut sink);
            conn.close().await.expect("peer close");
        });
        return;
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("live.db");
    let copy = dir.path().join("copy.db");
    let db_str = db.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db_str).await.expect("open source");
        conn.query("PRAGMA journal_mode = WAL;").await.expect("WAL");
        conn.execute("CREATE TABLE t(x);").await.expect("create");
        conn.execute(
            "INSERT INTO t WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 500) SELECT i FROM n;",
        )
        .await
        .expect("seed");

        let mut peer = Command::new(std::env::current_exe().expect("test binary"))
            .args([TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env(PEER_ENV, &db_str)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn peer process");
        let peer_stdin = peer.stdin.take().expect("peer stdin");
        let mut peer_stdout = BufReader::new(peer.stdout.take().expect("peer stdout"));
        let started = Instant::now();
        loop {
            let mut line = String::new();
            let read = peer_stdout.read_line(&mut line).expect("read peer stdout");
            assert!(read > 0, "peer exited before reporting readiness");
            if line.starts_with(READY) {
                assert_eq!(line.trim(), format!("{READY} 500"));
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(60), "peer never became ready");
        }

        conn.execute("PRAGMA busy_timeout = 3000;").await.expect("busy timeout");
        let backup_started = Instant::now();
        let report = conn.backup_exact_to(&copy).await;
        let elapsed = backup_started.elapsed();
        drop(peer_stdin);
        let _ = peer.wait();
        let report = report.unwrap_or_else(|error| {
            panic!("bd-5unvm: backup_exact_to beside an idle peer failed after {elapsed:?}: {error}")
        });
        assert!(report.page_count > 0);

        let stock = rusqlite::Connection::open(&copy).expect("stock open copy");
        let integrity: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("integrity");
        assert_eq!(integrity, "ok");
        let count: i64 = stock
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .expect("count");
        assert_eq!(count, 500);

        conn.execute("INSERT INTO t VALUES (0);").await.expect("write after backup");
        conn.close().await.expect("close source");
    });
}
