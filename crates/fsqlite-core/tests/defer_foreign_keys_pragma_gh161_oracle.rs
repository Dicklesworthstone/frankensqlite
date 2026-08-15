#![recursion_limit = "512"]

//! GH #161 (bd-gh-fk-deferred-enforcement): `PRAGMA defer_foreign_keys=ON`
//! defers every foreign-key NO ACTION check to COMMIT for the duration of the
//! transaction, regardless of the constraint's declared deferrability, and
//! auto-switches off at the conclusion of each transaction. rusqlite (real
//! SQLite) is the oracle for both the readback value and the deferred-vs-error
//! behavior.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

async fn fq(fconn: &Connection, sql: &str) -> Vec<Vec<String>> {
    fconn.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}")).iter().map(|r| r.values().iter().map(tag_f).collect()).collect()
}
fn rq(rconn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    st.query_map([], |row| Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())).unwrap().collect::<Result<Vec<_>, _>>().unwrap()
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let mut fr = fq(fconn, sql).await;
    fr.sort();
    let mut rr = rq(rconn, sql);
    rr.sort();
    assert_eq!(fr, rr, "mismatch on `{sql}`");
}

/// Run a statement on both engines; assert they agree on ok/err.
async fn agree_exec(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let f = fconn.execute(sql).await;
    let r = rconn.execute_batch(sql);
    match (&f, &r) {
        (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
        (Ok(_), Err(e)) => panic!("DIVERGE `{sql}`: frank OK, csql ERR({e})"),
        (Err(e), Ok(())) => panic!("DIVERGE `{sql}`: frank ERR({e:?}), csql OK"),
    }
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    for s in [
        "PRAGMA foreign_keys=ON",
        "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
        "INSERT INTO parent VALUES (1)",
        "INSERT INTO child VALUES (10, 1)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn defer_foreign_keys_readback_and_autoreset_gh161() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Default readback is 0.
        assert_agree(&f, &r, "PRAGMA defer_foreign_keys").await;
        f.execute("PRAGMA defer_foreign_keys=ON").await.unwrap();
        r.execute_batch("PRAGMA defer_foreign_keys=ON").unwrap();
        assert_agree(&f, &r, "PRAGMA defer_foreign_keys").await;
        // Auto-switches off at the conclusion of a transaction.
        for s in ["BEGIN", "COMMIT"] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        assert_agree(&f, &r, "PRAGMA defer_foreign_keys").await;
    });
}

#[test]
fn defer_foreign_keys_delete_reinsert_commits_gh161() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // Delete the referenced parent inside a deferred txn, then re-insert it
        // before COMMIT — no error on either engine.
        for s in [
            "PRAGMA defer_foreign_keys=ON",
            "BEGIN",
            "DELETE FROM parent WHERE id=1",
            "INSERT INTO parent VALUES (1)",
            "COMMIT",
        ] {
            agree_exec(&f, &r, s).await;
        }
        assert_agree(&f, &r, "SELECT id, pid FROM child ORDER BY id").await;
        assert_agree(&f, &r, "SELECT id FROM parent ORDER BY id").await;
    });
}

#[test]
fn defer_foreign_keys_unresolved_orphan_errors_at_commit_gh161() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // Delete the parent and DO NOT restore it: the DELETE is allowed, but the
        // dangling child must make COMMIT fail on both engines.
        for s in ["PRAGMA defer_foreign_keys=ON", "BEGIN", "DELETE FROM parent WHERE id=1"] {
            agree_exec(&f, &r, s).await;
        }
        agree_exec(&f, &r, "COMMIT").await;
    });
}

#[test]
fn defer_foreign_keys_parent_pk_update_gh161() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // Updating the parent PK inside a deferred txn must NOT immediately error
        // (the GH #161 bug). Resolve the deferred violation by restoring the
        // referenced id before COMMIT — the recheck then passes on both engines.
        for s in [
            "PRAGMA defer_foreign_keys=ON",
            "BEGIN",
            "UPDATE parent SET id=2 WHERE id=1",
            "INSERT INTO parent VALUES (1)",
            "COMMIT",
        ] {
            agree_exec(&f, &r, s).await;
        }
        assert_agree(&f, &r, "SELECT id, pid FROM child ORDER BY id").await;
        assert_agree(&f, &r, "SELECT id FROM parent ORDER BY id").await;
    });
}

#[test]
#[ignore = "shares the bd-gh-deferred-fk-parent-dml (GH #149) stale-snapshot limitation: \
the parent UPDATE queues the child's pid=1 snapshot, and re-pointing the child to a new \
parent does not clear that stale commit-time obligation. Needs recheck_deferred_fk_at_commit \
to re-query the current child row by identity instead of rechecking the queued snapshot."]
fn defer_foreign_keys_parent_pk_update_reparent_gh161_needs_gh149() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // Resolve by re-pointing the child (snapshot-incompatible resolution).
        for s in [
            "PRAGMA defer_foreign_keys=ON",
            "BEGIN",
            "UPDATE parent SET id=2 WHERE id=1",
            "UPDATE child SET pid=2 WHERE id=10",
            "COMMIT",
        ] {
            agree_exec(&f, &r, s).await;
        }
        assert_agree(&f, &r, "SELECT id, pid FROM child ORDER BY id").await;
        assert_agree(&f, &r, "SELECT id FROM parent ORDER BY id").await;
    });
}

#[test]
fn defer_foreign_keys_child_insert_deferred_gh161() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // Insert a child referencing a not-yet-existing parent under defer, then
        // create the parent before COMMIT — deferred, so no error.
        for s in [
            "PRAGMA defer_foreign_keys=ON",
            "BEGIN",
            "INSERT INTO child VALUES (11, 5)",
            "INSERT INTO parent VALUES (5)",
            "COMMIT",
        ] {
            agree_exec(&f, &r, s).await;
        }
        assert_agree(&f, &r, "SELECT id, pid FROM child ORDER BY id").await;
    });
}

#[test]
fn without_defer_immediate_violation_still_errors_gh161() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // Control: WITHOUT defer_foreign_keys, deleting a referenced parent is an
        // immediate error on both engines (behavior must be unchanged).
        for s in ["BEGIN", "DELETE FROM parent WHERE id=1"] {
            agree_exec(&f, &r, s).await;
        }
        // Roll back the (frank) open txn so the connection closes cleanly.
        let _ = f.execute("ROLLBACK").await;
        let _ = r.execute_batch("ROLLBACK");
    });
}
