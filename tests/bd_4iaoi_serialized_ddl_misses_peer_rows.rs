//! bd-4iaoi: a schema change in a non-CONCURRENT transaction must not commit
//! derived structures built from a snapshot that misses a peer's rows.
//!
//! Connection A opens `BEGIN IMMEDIATE` (or a deferred `BEGIN` upgraded by its
//! first write). Connection B, in the default concurrent mode, then commits an
//! INSERT. A's CREATE INDEX reads its own snapshot, which lacks B's row. Stock
//! SQLite makes B wait for A's write lock. Either way the index must end up
//! covering every row: a busy refusal on either side is fine, and a lost
//! row is not.
#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;

async fn integrity(path: &str) -> String {
    let conn = Connection::open(path).await.expect("reopen");
    let rows = conn
        .query("PRAGMA integrity_check")
        .await
        .expect("integrity_check");
    let verdict = format!("{:?}", rows[0].values()[0]);
    conn.close().await.expect("close");
    verdict
}

async fn run(begin: &str) -> (String, String, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bd4iaoi.db");
    let path = path.to_str().expect("utf-8 path").to_owned();
    let seed = Connection::open(&path).await.expect("open seed");
    seed.execute_batch(
        "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
         INSERT INTO t(v) VALUES ('a'), ('b');",
    )
    .await
    .expect("seed");
    seed.close().await.expect("close seed");

    let a = Connection::open(&path).await.expect("open a");
    let b = Connection::open(&path).await.expect("open b");
    a.execute(begin).await.expect("begin a");
    // Pin A's snapshot before B commits.
    a.query("SELECT count(*) FROM t").await.expect("a reads");
    let peer = b.execute("INSERT INTO t(v) VALUES ('peer')").await;
    let ddl = match a.execute("CREATE INDEX t_v ON t(v)").await {
        Ok(_) => a.execute("COMMIT").await.map(|_| ()),
        Err(error) => Err(error),
    };
    if ddl.is_err() {
        let _ = a.execute("ROLLBACK").await;
    }
    a.close().await.expect("close a");
    b.close().await.expect("close b");
    (
        format!("{:?}", peer.map(|_| ())),
        format!("{ddl:?}"),
        integrity(&path).await,
    )
}

/// The root cause, deterministically: a peer compiled its INSERT while index
/// `t_v` lived at one root page. Another connection then drops it and creates
/// an index (the same name or another) at a different root page, so the table
/// still has one index. The peer's schema reload must invalidate its compiled
/// INSERT, which addresses the old root page.
#[test]
fn replacing_an_index_invalidates_a_peers_compiled_insert() {
    for replacement in [
        "DROP INDEX t_v; CREATE TABLE filler(x); CREATE INDEX t_v ON t(v);",
        "DROP INDEX t_v; CREATE TABLE filler(x); CREATE INDEX t_w ON t(id, v);",
    ] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("bd4iaoi_replace.db");
            let path = path.to_str().expect("utf-8 path").to_owned();
            let a = Connection::open(&path).await.expect("open a");
            a.execute_batch(
                "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
                 CREATE INDEX t_v ON t(v); INSERT INTO t(v) VALUES ('a');",
            )
            .await
            .expect("seed");
            let b = Connection::open(&path).await.expect("open b");
            for _ in 0..2 {
                b.execute("INSERT INTO t(v) VALUES ('before')")
                    .await
                    .expect("b compiles its insert");
            }
            a.execute_batch(replacement).await.expect("replace index");
            b.execute("INSERT INTO t(v) VALUES ('after')")
                .await
                .expect("b inserts after the replacement");
            a.close().await.expect("close a");
            b.close().await.expect("close b");
            let verdict = integrity(&path).await;
            println!("PROBE replace {replacement}: integrity={verdict}");
            assert_eq!(
                verdict, "Text(\"ok\")",
                "{replacement}: the peer's insert missed the new index"
            );
        });
    }
}

/// The field failure: a peer that already ran statements under the old schema
/// inserts after A's serialized CREATE INDEX has committed. It must notice the
/// schema change and maintain the new index.
#[test]
fn peer_sees_a_committed_serialized_create_index() {
    for (begin, commit, checkpoint, cycle) in [
        ("BEGIN IMMEDIATE", "COMMIT", false, false),
        ("BEGIN", "COMMIT", false, false),
        ("BEGIN EXCLUSIVE", "COMMIT", false, false),
        ("SELECT 1", "SELECT 1", false, false),
        ("BEGIN IMMEDIATE", "COMMIT", true, false),
        ("BEGIN IMMEDIATE", "COMMIT", false, true),
        ("BEGIN IMMEDIATE", "COMMIT", true, true),
        ("SELECT 1", "SELECT 1", true, true),
    ] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("bd4iaoi_after.db");
            let path = path.to_str().expect("utf-8 path").to_owned();
            let a = Connection::open(&path).await.expect("open a");
            a.execute_batch(
                "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
                 INSERT INTO t(v) VALUES ('a');",
            )
            .await
            .expect("seed");
            let b = Connection::open(&path).await.expect("open b");
            b.execute("PRAGMA busy_timeout=4000").await.expect("busy_timeout");
            b.execute("INSERT INTO t(v) VALUES ('before')")
                .await
                .expect("b warms its schema");
            let steps: &[&str] = if cycle {
                &["CREATE INDEX t_v ON t(v)", "DROP INDEX t_v", "CREATE INDEX t_v ON t(v)"]
            } else {
                &["CREATE INDEX t_v ON t(v)"]
            };
            for step in steps {
                a.execute(begin).await.expect("begin a");
                a.execute(step).await.expect("ddl");
                a.execute(commit).await.expect("commit a");
            }
            if checkpoint {
                b.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    .await
                    .expect("checkpoint");
            }
            let after = b.execute("INSERT INTO t(v) VALUES ('after')").await;
            a.close().await.expect("close a");
            b.close().await.expect("close b");
            let verdict = integrity(&path).await;
            println!(
                "PROBE after {begin} checkpoint={checkpoint} cycle={cycle}: peer={:?} integrity={verdict}",
                after.map(|_| ())
            );
            assert_eq!(verdict, "Text(\"ok\")", "{begin}: the peer's row skipped the index");
        });
    }
}

/// The peer waits in its busy handler for A's write lock, then commits an
/// INSERT that started before A's CREATE INDEX committed.
#[test]
fn peer_waiting_out_a_serialized_ddl_maintains_the_new_index() {
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bd4iaoi_wait.db");
    let path = path.to_str().expect("utf-8 path").to_owned();
    asupersync::test_utils::run_test(|| async {
        let seed = Connection::open(&path).await.expect("open seed");
        seed.execute_batch(
            "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t(v) VALUES ('a'), ('b');",
        )
        .await
        .expect("seed");
        seed.close().await.expect("close seed");
    });

    let a_began = Arc::new(Barrier::new(2));
    let peer = {
        let path = path.clone();
        let a_began = Arc::clone(&a_began);
        std::thread::spawn(move || {
            let result = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
            let out = std::sync::Arc::clone(&result);
            asupersync::test_utils::run_test(move || async move {
                let b = Connection::open(&path).await.expect("open b");
                b.execute("PRAGMA busy_timeout=4000").await.expect("busy_timeout");
                b.execute("INSERT INTO t(v) VALUES ('warm')")
                    .await
                    .expect("b warms its schema");
                a_began.wait();
                let mut results = Vec::new();
                for _ in 0..30 {
                    let _ = b.execute("PRAGMA wal_checkpoint(TRUNCATE)").await;
                    let inserted = b.execute("INSERT INTO t(v) VALUES ('peer')").await;
                    results.push(format!("{:?}", inserted.map(|_| ())));
                    std::thread::sleep(Duration::from_millis(20));
                }
                results.dedup();
                *out.lock().expect("out") = results.join(",");
                b.close().await.expect("close b");
            });
            let text = result.lock().expect("result").clone();
            text
        })
    };
    let ddl = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    {
        let ddl = std::sync::Arc::clone(&ddl);
        let path = path.clone();
        asupersync::test_utils::run_test(move || async move {
            let a = Connection::open(&path).await.expect("open a");
            a.execute("PRAGMA busy_timeout=4000").await.expect("busy_timeout");
            a_began.wait();
            let mut results = Vec::new();
            for step in [
                "CREATE INDEX t_v ON t(v)",
                "DROP INDEX t_v",
                "CREATE INDEX t_v ON t(v)",
            ] {
                let result = match a.execute("BEGIN IMMEDIATE").await {
                    Ok(_) => match a.execute(step).await {
                        Ok(_) => a.execute("COMMIT").await.map(|_| ()),
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                };
                if result.is_err() {
                    let _ = a.execute("ROLLBACK").await;
                }
                results.push(format!("{result:?}"));
                std::thread::sleep(Duration::from_millis(50));
            }
            *ddl.lock().expect("ddl") = results.join(",");
            a.close().await.expect("close a");
        });
    }
    let peer = peer.join().expect("peer thread");
    let ddl = ddl.lock().expect("ddl").clone();
    asupersync::test_utils::run_test(|| async {
        let verdict = integrity(&path).await;
        println!("PROBE waiting peer: peer={peer} ddl={ddl} integrity={verdict}");
        assert_eq!(
            verdict, "Text(\"ok\")",
            "peer={peer} ddl={ddl}: the index lost the peer's row"
        );
    });
}

#[test]
fn serialized_ddl_never_commits_an_index_missing_peer_rows() {
    for begin in ["BEGIN IMMEDIATE", "BEGIN"] {
        asupersync::test_utils::run_test(|| async move {
            let (peer, ddl, verdict) = run(begin).await;
            println!("PROBE {begin}: peer={peer} ddl={ddl} integrity={verdict}");
            assert_eq!(
                verdict, "Text(\"ok\")",
                "{begin}: peer={peer} ddl={ddl}: the index lost the peer's row"
            );
        });
    }
}
