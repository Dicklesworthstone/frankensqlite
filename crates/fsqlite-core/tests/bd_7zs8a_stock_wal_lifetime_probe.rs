//! bd-7zs8a — does a stock SQLite connection delete our WAL out from under us?
//!
//! Nine `retained_autocommit_*_flushes_prior_batch` unit tests fail on main with
//! `CannotOpen { path: "....db-wal" }`, and all nine share one shape: our
//! Connection writes, then a STOCK `rusqlite` connection opens and closes the
//! same file, then our next statement fails. Bisected to 304280b37
//! ("feat(wal): native WAL-index SHM, append ownership, and reader leases");
//! v0.3.18 is clean and v0.4.4 is not, so the shipped release carries it.
//!
//! The hypothesis this probe tests: stock SQLite checkpoints and unlinks the
//! `-wal` and `-shm` when it believes it is the last connection, and it decides
//! that from the SHM dead-man-switch lock. If our connection no longer holds
//! that lock in a way stock recognises, stock deletes a WAL our open connection
//! still needs, and the next open of it — with `create = false` — raises
//! `CannotOpen` (crates/fsqlite-vfs/src/unix.rs, the generic open's NotFound
//! arm).
//!
//! This prints file existence at each step rather than asserting, so it reports
//! what actually happens instead of encoding today's behaviour as correct.
//!
//!   cargo test -p fsqlite-core --test bd_7zs8a_stock_wal_lifetime_probe -- --ignored --nocapture

use fsqlite_core::connection::Connection;
use std::path::Path;

fn report(label: &str, db: &Path) {
    // Build the companions the way the engine does — by appending to the full
    // file name. `with_extension` would mangle a name that already has one.
    let companion = |suffix: &str| {
        db.with_file_name(format!(
            "{}{suffix}",
            db.file_name().unwrap_or_default().to_string_lossy()
        ))
    };
    let wal = companion("-wal");
    let shm = companion("-shm");
    println!(
        "{label:<38} db={} wal={} shm={}",
        db.exists(),
        wal.exists(),
        shm.exists()
    );
}

#[test]
#[ignore = "bd-7zs8a diagnosis probe; prints WAL companion lifetime, run explicitly"]
fn stock_open_close_while_we_hold_the_database() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("stock_wal_lifetime.db");
    let db_display = db_path.clone();
    let path = db_path.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&path).await.expect("open ours");
        conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
            .await
            .expect("ddl");
        conn.execute("INSERT INTO t VALUES (1,'alpha')")
            .await
            .expect("insert");
        report("after our write, ours still open", &db_display);

        // A stock connection reads the same file and closes. Our connection is
        // still open the whole time, so stock must NOT treat itself as the last
        // connection.
        let stock = rusqlite::Connection::open(&db_display).expect("stock open");
        let count: i64 = stock
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .expect("stock read");
        assert_eq!(count, 1, "stock must see our committed row");
        report("stock open, before its close", &db_display);
        drop(stock);
        report("after stock close (ours still open)", &db_display);

        // Whatever the files look like now, our next statement is the one the
        // failing tests trip on.
        let outcome = conn.execute("SAVEPOINT probe_sp").await;
        println!("our next statement after stock close: {outcome:?}");
        if outcome.is_ok() {
            let _ = conn.execute("ROLLBACK").await;
        }
        report("after our next statement", &db_display);
        let _ = conn.close().await;
        report("after our close", &db_display);
    });
}
