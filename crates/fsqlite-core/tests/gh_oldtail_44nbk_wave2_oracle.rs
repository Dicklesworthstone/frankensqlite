#![recursion_limit = "512"]

//! bd-44nbk old-tail GH triage — wave 2: savepoint/schema/UPSERT/attached-DML
//! conformance issues, each differential vs rusqlite (SQLite 3.46.1).
//!  #143 SAVEPOINT rollback + AFTER trigger + JOIN view visibility
//!  #144 schema-qualified main.t must not write a shadowing TEMP table
//!  #145 UPSERT conflict target matches a partial UNIQUE index
//!  #147 concurrent AUTOINCREMENT allocator restored on ROLLBACK TO
//!  #149 DEFERRABLE INITIALLY DEFERRED NO ACTION FK defers to COMMIT
//!  #151 DO UPDATE SET id=excluded.id updates the IPK on a secondary UNIQUE conflict
//!  #244 UPDATE of an attached schema inside an explicit transaction

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
async fn fq(f: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    match f.query(sql).await {
        Ok(rows) => Ok(rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect()),
        Err(e) => Err(format!("{e:?}")),
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut st = r.prepare(sql).map_err(|e| e.to_string())?;
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}
async fn fx(f: &Connection, sql: &str) -> Result<(), String> {
    f.execute(sql)
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// Run `setup` on both, then compare `query` results (as strings, on error a marker).
async fn agree(setup: &[&str], query: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = fx(&f, s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, query)
        .await
        .unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
    let rr = rq(&r, query).unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
    assert_eq!(fr, rr, "{msg}");
}

#[test]
fn gh143_savepoint_rollback_trigger_view() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)",
                "CREATE TABLE events(id INTEGER PRIMARY KEY, uid INTEGER)",
                "CREATE VIEW v AS SELECT u.id AS uid, u.name FROM users u JOIN events e ON e.uid=u.id",
                "CREATE TRIGGER trg AFTER INSERT ON users BEGIN INSERT INTO events(uid) VALUES (NEW.id); END",
                "INSERT INTO users(name) VALUES ('base')",
                "SAVEPOINT sp",
                "INSERT INTO users(name) VALUES ('second')",
                "ROLLBACK TO sp",
            ],
            "SELECT uid, name FROM v ORDER BY uid",
            "GH#143: rolled-back user/event must not be visible in the view",
        )
        .await;
    });
}

#[test]
fn gh144_schema_qualified_main_not_temp_shadow() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(x)",
            "INSERT INTO t VALUES (1)",
            "CREATE TEMP TABLE t(x)",
            "INSERT INTO temp.t VALUES (9)",
            "UPDATE t SET x=8",              // unqualified → TEMP shadow
            "INSERT INTO main.t VALUES (2)", // explicit main
        ] {
            let _ = fx(&f, s).await;
            let _ = r.execute_batch(s);
        }
        for (q, label) in [
            ("SELECT x FROM main.t ORDER BY x", "main.t"),
            ("SELECT x FROM temp.t ORDER BY x", "temp.t"),
        ] {
            let fr = fq(&f, q)
                .await
                .unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
            let rr = rq(&r, q).unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
            assert_eq!(
                fr, rr,
                "GH#144: {label} content diverges (main-qualified write hit TEMP)"
            );
        }
    });
}

#[test]
fn gh145_upsert_partial_unique_target() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, email TEXT, active INTEGER)",
                "CREATE UNIQUE INDEX uq ON t(email) WHERE active=1",
                "INSERT INTO t VALUES (1,'a@x',1),(2,'a@x',0)",
                "INSERT INTO t VALUES (3,'a@x',1) ON CONFLICT(email) WHERE active=1 DO UPDATE SET id=excluded.id",
            ],
            "SELECT id, email, active FROM t ORDER BY email, active, id",
            "GH#145: UPSERT must match the partial UNIQUE index and update the active row",
        )
        .await;
    });
}

// GH#147 fixed (bd-gh-147): a CAS-safe savepoint rewind of the shared concurrent
// rowid allocator now restores the AUTOINCREMENT allocator on ROLLBACK TO in
// concurrent mode (id 2 for row c, matching stock and the non-concurrent control).
#[test]
fn gh147_concurrent_autoincrement_savepoint_rollback() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
                "BEGIN",
                "INSERT INTO t(v) VALUES ('a')",
                "SAVEPOINT sp",
                "INSERT INTO t(v) VALUES ('b')",
                "ROLLBACK TO sp",
                "INSERT INTO t(v) VALUES ('c')",
                "COMMIT",
            ],
            "SELECT id, v FROM t ORDER BY id",
            "GH#147: ROLLBACK TO must restore the AUTOINCREMENT allocator (expect 1,a / 2,c)",
        )
        .await;
    });
}

#[test]
fn gh149_deferred_no_action_fk() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let setup = [
            "PRAGMA foreign_keys=ON",
            "CREATE TABLE p(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED)",
            "INSERT INTO p VALUES (1)",
            "INSERT INTO c VALUES (10,1)",
        ];
        for s in setup {
            fx(&f, s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        let txn = [
            "BEGIN",
            "DELETE FROM p WHERE id=1",
            "INSERT INTO p VALUES (1)",
            "COMMIT",
        ];
        let frank_ok = {
            let mut ok = true;
            for s in txn {
                if fx(&f, s).await.is_err() {
                    ok = false;
                }
            }
            ok
        };
        let stock_ok = txn.iter().all(|s| r.execute_batch(s).is_ok());
        assert_eq!(
            frank_ok, stock_ok,
            "GH#149: deferred NO ACTION FK must allow the temporary orphan (stock_ok={stock_ok})"
        );
    });
}

#[test]
fn gh151_upsert_updates_ipk_on_secondary_unique() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, email TEXT UNIQUE, n INTEGER)",
                "INSERT INTO t VALUES (1,'a@x',100)",
                "INSERT INTO t VALUES (99,'a@x',200) ON CONFLICT(email) DO UPDATE SET id=excluded.id, n=excluded.n",
            ],
            "SELECT id, email, n FROM t",
            "GH#151: DO UPDATE SET id=excluded.id must update the IPK to 99",
        )
        .await;
    });
}

#[test]
fn gh244_update_attached_schema_in_txn() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let aux = dir.path().join("aux.db");
        let main = dir.path().join("main.db");
        let f = Connection::open(main.to_str().unwrap()).await.unwrap();
        let attach = format!("ATTACH '{}' AS aux", aux.display());
        for s in [
            attach.as_str(),
            "CREATE TABLE aux.t(x INTEGER)",
            "INSERT INTO aux.t VALUES (1)",
            "BEGIN",
            "UPDATE aux.t SET x=2",
            "COMMIT",
        ] {
            fx(&f, s)
                .await
                .unwrap_or_else(|e| panic!("GH#244: `{s}` must succeed: {e}"));
        }
        let rows = fq(&f, "SELECT x FROM aux.t").await.unwrap();
        assert_eq!(
            rows,
            vec![vec!["2".to_owned()]],
            "GH#244: attached UPDATE in a txn must commit x=2"
        );
    });
}

/// bd-bsc69: open a main DB with `aux` attached and `aux.t(x)` seeded to 1.
async fn open_with_attached_aux(dir: &std::path::Path) -> Connection {
    let aux = dir.join("aux.db");
    let main = dir.join("main.db");
    let f = Connection::open(main.to_str().unwrap()).await.unwrap();
    let attach = format!("ATTACH '{}' AS aux", aux.display());
    for s in [
        attach.as_str(),
        "CREATE TABLE aux.t(x INTEGER)",
        "INSERT INTO aux.t VALUES (1)",
    ] {
        fx(&f, s)
            .await
            .unwrap_or_else(|e| panic!("bd-bsc69 setup `{s}`: {e}"));
    }
    f
}

#[test]
fn gh244_rollback_undoes_attached_write() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let f = open_with_attached_aux(dir.path()).await;
        for s in ["BEGIN", "UPDATE aux.t SET x=2", "ROLLBACK"] {
            fx(&f, s).await.unwrap_or_else(|e| panic!("`{s}`: {e}"));
        }
        let rows = fq(&f, "SELECT x FROM aux.t").await.unwrap();
        assert_eq!(
            rows,
            vec![vec!["1".to_owned()]],
            "bd-bsc69: ROLLBACK must undo the attached write (x stays 1)"
        );
    });
}

#[test]
fn gh244_multi_statement_single_txn_is_atomic() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let f = open_with_attached_aux(dir.path()).await;
        for s in [
            "CREATE TABLE m(y INTEGER)",
            "BEGIN",
            "UPDATE aux.t SET x=x+10", // 1 -> 11
            "INSERT INTO aux.t VALUES (5)",
            "INSERT INTO m VALUES (7)",
            "COMMIT",
        ] {
            fx(&f, s).await.unwrap_or_else(|e| panic!("`{s}`: {e}"));
        }
        assert_eq!(
            fq(&f, "SELECT x FROM aux.t ORDER BY x").await.unwrap(),
            vec![vec!["5".to_owned()], vec!["11".to_owned()]],
            "bd-bsc69: both attached writes must commit atomically"
        );
        assert_eq!(
            fq(&f, "SELECT y FROM m").await.unwrap(),
            vec![vec!["7".to_owned()]],
            "bd-bsc69: the main write must commit in the same txn"
        );
    });
}

#[test]
fn gh244_two_attached_dbs_commit_together() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let a = dir.path().join("a.db");
        let b = dir.path().join("b.db");
        let main = dir.path().join("main.db");
        let f = Connection::open(main.to_str().unwrap()).await.unwrap();
        let attach_a = format!("ATTACH '{}' AS a", a.display());
        let attach_b = format!("ATTACH '{}' AS b", b.display());
        for s in [
            attach_a.as_str(),
            attach_b.as_str(),
            "CREATE TABLE a.t(x INTEGER)",
            "CREATE TABLE b.t(x INTEGER)",
            "INSERT INTO a.t VALUES (1)",
            "INSERT INTO b.t VALUES (2)",
            "BEGIN",
            "UPDATE a.t SET x=10",
            "UPDATE b.t SET x=20",
            "COMMIT",
        ] {
            fx(&f, s).await.unwrap_or_else(|e| panic!("`{s}`: {e}"));
        }
        assert_eq!(
            fq(&f, "SELECT x FROM a.t").await.unwrap(),
            vec![vec!["10".to_owned()]],
            "bd-bsc69: first attached DB must commit"
        );
        assert_eq!(
            fq(&f, "SELECT x FROM b.t").await.unwrap(),
            vec![vec!["20".to_owned()]],
            "bd-bsc69: second attached DB must commit"
        );
    });
}

#[test]
fn gh244_rollback_then_rebegin_works() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let f = open_with_attached_aux(dir.path()).await;
        for s in [
            "BEGIN",
            "UPDATE aux.t SET x=9",
            "ROLLBACK",
            "BEGIN",
            "UPDATE aux.t SET x=3",
            "COMMIT",
        ] {
            fx(&f, s).await.unwrap_or_else(|e| panic!("`{s}`: {e}"));
        }
        assert_eq!(
            fq(&f, "SELECT x FROM aux.t").await.unwrap(),
            vec![vec!["3".to_owned()]],
            "bd-bsc69: a fresh txn after ROLLBACK must commit (x=3)"
        );
    });
}

#[test]
fn gh244_autocommit_attached_write_unchanged() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let f = open_with_attached_aux(dir.path()).await;
        // Regression guard: an attached write in AUTOCOMMIT still commits
        // immediately (the no-outer-txn path is unchanged).
        fx(&f, "UPDATE aux.t SET x=8")
            .await
            .unwrap_or_else(|e| panic!("autocommit attached UPDATE: {e}"));
        assert_eq!(
            fq(&f, "SELECT x FROM aux.t").await.unwrap(),
            vec![vec!["8".to_owned()]],
            "bd-bsc69: autocommit attached write must persist immediately"
        );
    });
}

#[test]
fn gh244_savepoint_scoped_attached_write_still_errors() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let f = open_with_attached_aux(dir.path()).await;
        fx(&f, "BEGIN").await.unwrap();
        fx(&f, "SAVEPOINT sp").await.unwrap();
        // Savepoint fan-out is a follow-up; the attached write must still cleanly
        // error (NotImplemented) rather than silently diverge cross-DB state.
        let result = fx(&f, "UPDATE aux.t SET x=9").await;
        assert!(
            result.is_err(),
            "bd-bsc69: a savepoint-scoped attached write must still error until \
             savepoint participation lands"
        );
    });
}
