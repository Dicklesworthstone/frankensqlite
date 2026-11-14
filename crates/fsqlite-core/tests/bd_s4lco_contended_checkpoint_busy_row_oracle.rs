#![recursion_limit = "256"]

//! bd-s4lco MUST-2: a checkpoint that a peer's open read transaction keeps
//! from finishing answers with stock's row `(busy, log, checkpointed)`, never
//! an error, in every mode; once the reader commits, the same checkpoint
//! completes. Each engine runs the identical shape on its own file: setup in
//! WAL mode, connection `A` opens a read transaction, connection `B`
//! (busy_timeout 0) commits two rows and checkpoints, `A` commits, `B`
//! checkpoints again. The rows are compared with rusqlite's.
//!
//! Fails a checkpoint that errors under contention (the bd-s4lco
//! `BusyRecovery` bug) and one that reports completion (`busy = 0`) while the
//! reader still pins frames it would have to overwrite.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &str = "PRAGMA journal_mode=WAL; CREATE TABLE t(x); INSERT INTO t VALUES(1); \
                     PRAGMA wal_checkpoint(TRUNCATE);";

fn tag(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Integer(n) => n.to_string(),
        other => format!("{other:?}"),
    }
}

async fn fsqlite_rows(mode: &str, path: &str) -> Vec<String> {
    let a = Connection::open(path).await.expect("open A");
    for statement in SETUP.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        a.execute(statement).await.expect("setup");
    }
    let b = Connection::open(path).await.expect("open B");
    b.execute("PRAGMA busy_timeout=0").await.expect("busy_timeout");
    a.execute("BEGIN").await.expect("begin A");
    let seen = a.query("SELECT count(*) FROM t").await.expect("A reads");
    let mut out = vec![format!("A saw {}", tag(&seen[0].values()[0]))];
    b.execute("INSERT INTO t VALUES(2)").await.expect("B insert 2");
    b.execute("INSERT INTO t VALUES(3)").await.expect("B insert 3");
    let pragma = format!("PRAGMA wal_checkpoint({mode})");
    match b.query(&pragma).await {
        Ok(rows) => out.push(format!(
            "contended {}",
            rows[0].values().iter().map(tag).collect::<Vec<_>>().join("|")
        )),
        Err(error) => out.push(format!("contended ERROR {error}")),
    }
    a.execute("COMMIT").await.expect("commit A");
    match b.query(&pragma).await {
        Ok(rows) => out.push(format!(
            "uncontended {}",
            rows[0].values().iter().map(tag).collect::<Vec<_>>().join("|")
        )),
        Err(error) => out.push(format!("uncontended ERROR {error}")),
    }
    let count = b.query("SELECT count(*) FROM t").await.expect("B count");
    out.push(format!("count {}", tag(&count[0].values()[0])));
    b.close().await.expect("close B");
    a.close().await.expect("close A");
    out
}

fn stock_rows(mode: &str, path: &str) -> Vec<String> {
    let a = rusqlite::Connection::open(path).expect("open A");
    a.execute_batch(SETUP).expect("setup");
    let b = rusqlite::Connection::open(path).expect("open B");
    b.busy_timeout(std::time::Duration::ZERO).expect("busy_timeout");
    a.execute_batch("BEGIN").expect("begin A");
    let seen: i64 = a
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .expect("A reads");
    let mut out = vec![format!("A saw {seen}")];
    b.execute_batch("INSERT INTO t VALUES(2); INSERT INTO t VALUES(3);")
        .expect("B inserts");
    let pragma = format!("PRAGMA wal_checkpoint({mode})");
    let row = |label: &str| -> String {
        match b.query_row(&pragma, [], |r| {
            Ok(format!(
                "{}|{}|{}",
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?
            ))
        }) {
            Ok(text) => format!("{label} {text}"),
            Err(error) => format!("{label} ERROR {error}"),
        }
    };
    out.push(row("contended"));
    a.execute_batch("COMMIT").expect("commit A");
    out.push(row("uncontended"));
    let count: i64 = b
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .expect("B count");
    out.push(format!("count {count}"));
    out
}

#[test]
fn contended_checkpoint_reports_stock_busy_row_in_every_mode() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut diffs = Vec::new();
        for mode in ["PASSIVE", "FULL", "RESTART", "TRUNCATE"] {
            let ours_path = dir.path().join(format!("ours_{mode}.db"));
            let stock_path = dir.path().join(format!("stock_{mode}.db"));
            let ours = fsqlite_rows(mode, ours_path.to_str().expect("utf-8 path")).await;
            let stock = stock_rows(mode, stock_path.to_str().expect("utf-8 path"));
            assert!(
                stock.iter().any(|line| line.starts_with("contended 1|")) || mode == "PASSIVE",
                "the shape must contend for stock in {mode}: {stock:?}"
            );
            if ours != stock {
                diffs.push(format!("{mode}\n  frank={ours:?}\n  stock={stock:?}"));
            }
        }
        assert!(diffs.is_empty(), "checkpoint rows differ from stock:\n{}", diffs.join("\n"));
    });
}
