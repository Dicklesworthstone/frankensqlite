#![recursion_limit = "512"]

//! bd-44nbk old-tail GH triage — wave 1: the DML/constraint/query conformance
//! issues that verify cleanly against rusqlite (SQLite 3.46.1). Each test either
//! confirms frank now matches stock (→ close the issue with the fix SHA) or, if
//! it fails, pins the residual for a follow-up fix.

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
    let rows = st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(rows)
}

async fn fexec(f: &Connection, sql: &str) -> Result<(), String> {
    f.execute(sql)
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// Run identical setup on both engines; then assert the final `query` agrees.
async fn setup_and_agree(setup: &[&str], query: &str) -> (Vec<Vec<String>>, Vec<Vec<String>>) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        let _ = fexec(&f, sql).await; // errors surfaced by the final query comparison
        let _ = r.execute_batch(sql);
    }
    let fr = fq(&f, query)
        .await
        .unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
    let rr = rq(&r, query).unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
    (fr, rr)
}

/// #142: INSERT OR REPLACE on a parent fires ON DELETE CASCADE.
#[test]
fn gh142_replace_parent_fires_cascade() {
    asupersync::test_utils::run_test(|| async {
        let (fr, rr) = setup_and_agree(
            &[
                "PRAGMA foreign_keys=ON",
                "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
                "CREATE TABLE child(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
                "INSERT INTO parent(id) VALUES (1)",
                "INSERT INTO child(cid, pid) VALUES (10, 1)",
                "INSERT OR REPLACE INTO parent(id) VALUES (1)",
            ],
            "SELECT count(*) FROM child",
        )
        .await;
        assert_eq!(
            fr, rr,
            "GH#142: REPLACE parent must cascade-delete the child"
        );
    });
}

/// #146: nested correlated NOT EXISTS resolves the outer correlation (relational division).
#[test]
fn gh146_nested_correlated_not_exists() {
    asupersync::test_utils::run_test(|| async {
        let (fr, rr) = setup_and_agree(
            &[
                "CREATE TABLE students(sid INTEGER PRIMARY KEY)",
                "CREATE TABLE courses(cid INTEGER PRIMARY KEY)",
                "CREATE TABLE took(sid INTEGER, cid INTEGER)",
                "INSERT INTO students VALUES (1),(2),(3)",
                "INSERT INTO courses VALUES (1),(2)",
                // students 1 and 3 took every course; student 2 took only course 1.
                "INSERT INTO took VALUES (1,1),(1,2),(2,1),(3,1),(3,2)",
            ],
            "SELECT sid FROM students s WHERE NOT EXISTS (SELECT 1 FROM courses c \
             WHERE NOT EXISTS (SELECT 1 FROM took t WHERE t.sid = s.sid AND t.cid = c.cid)) ORDER BY sid",
        )
        .await;
        assert_eq!(fr, rr, "GH#146: nested NOT EXISTS must return students 1,3");
    });
}

/// #152: recursive CTE honors LIMIT.
#[test]
fn gh152_recursive_cte_limit() {
    asupersync::test_utils::run_test(|| async {
        let (fr, rr) = setup_and_agree(
            &[],
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c LIMIT 3) SELECT n FROM c",
        )
        .await;
        assert_eq!(fr, rr, "GH#152: recursive CTE LIMIT 3 must yield 1,2,3");
    });
}

/// #159: UPDATE OR IGNORE that is ignored emits no RETURNING row.
#[test]
fn gh159_update_or_ignore_returning() {
    asupersync::test_utils::run_test(|| async {
        let (fr, rr) = setup_and_agree(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, email TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES (1,'a@x',10),(2,'b@x',20)",
            ],
            "UPDATE OR IGNORE t SET email='a@x' WHERE id=2 RETURNING id,email,v",
        )
        .await;
        assert_eq!(fr, rr, "GH#159: ignored UPDATE must emit no RETURNING row");
    });
}

/// #161: PRAGMA defer_foreign_keys defers the check to COMMIT.
#[test]
fn gh161_defer_foreign_keys() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let setup = [
            "PRAGMA foreign_keys=ON",
            "CREATE TABLE p(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
            "INSERT INTO p VALUES (1)",
            "INSERT INTO c VALUES (10,1)",
        ];
        for s in setup {
            fexec(&f, s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // Defer, then temporarily violate and repair within the txn.
        let txn = [
            "PRAGMA defer_foreign_keys=ON",
            "BEGIN",
            "DELETE FROM p WHERE id=1",
            "INSERT INTO p VALUES (1)",
            "COMMIT",
        ];
        let mut frank_ok = true;
        for s in txn {
            if fexec(&f, s).await.is_err() {
                frank_ok = false;
            }
        }
        let stock_ok = txn.iter().all(|s| r.execute_batch(s).is_ok());
        assert_eq!(
            frank_ok, stock_ok,
            "GH#161: deferred FK must let the txn repair the violation before COMMIT (stock_ok={stock_ok})"
        );
    });
}

/// #164: STRICT INTEGER column accepts an exact-integer REAL value.
#[test]
fn gh164_strict_integer_accepts_exact_real() {
    asupersync::test_utils::run_test(|| async {
        let (fr, rr) = setup_and_agree(
            &[
                "CREATE TABLE t(x INTEGER) STRICT",
                "INSERT INTO t VALUES (2.0)",
            ],
            "SELECT x, typeof(x) FROM t",
        )
        .await;
        assert_eq!(
            fr, rr,
            "GH#164: STRICT INTEGER must accept exact-integer REAL 2.0 as integer 2"
        );
    });
}

/// #165: UPDATE of a STORED generated column is rejected.
#[test]
fn gh165_update_stored_generated_rejected() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(a INTEGER, b INTEGER GENERATED ALWAYS AS (a*2) STORED)",
            "INSERT INTO t(a) VALUES (1)",
        ] {
            fexec(&f, s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        let frank_err = fexec(&f, "UPDATE t SET b=99").await.is_err();
        let stock_err = r.execute_batch("UPDATE t SET b=99").is_err();
        assert_eq!(
            frank_err, stock_err,
            "GH#165: UPDATE of a STORED generated column must be rejected (stock_err={stock_err})"
        );
    });
}

/// #166: UPDATE of a VIRTUAL generated column is rejected.
#[test]
fn gh166_update_virtual_generated_rejected() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(a INTEGER, b INTEGER GENERATED ALWAYS AS (a*2) VIRTUAL)",
            "INSERT INTO t(a) VALUES (1)",
        ] {
            fexec(&f, s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        let frank_err = fexec(&f, "UPDATE t SET b=99").await.is_err();
        let stock_err = r.execute_batch("UPDATE t SET b=99").is_err();
        assert_eq!(
            frank_err, stock_err,
            "GH#166: UPDATE of a VIRTUAL generated column must be rejected (stock_err={stock_err})"
        );
    });
}

/// #169: CHECK is evaluated after column affinity.
#[test]
fn gh169_check_after_affinity() {
    asupersync::test_utils::run_test(|| async {
        let (fr, rr) = setup_and_agree(
            &[
                "CREATE TABLE t(x INTEGER CHECK(x < 10))",
                "INSERT INTO t VALUES ('5')",
            ],
            "SELECT x, typeof(x) FROM t",
        )
        .await;
        assert_eq!(
            fr, rr,
            "GH#169: '5' gets INTEGER affinity (→5) before CHECK(5<10)"
        );
    });
}

/// #172: correlated scalar subquery uses ORDER BY (not insertion order) for LIMIT 1.
#[test]
fn gh172_correlated_scalar_orderby_limit1() {
    asupersync::test_utils::run_test(|| async {
        let (fr, rr) = setup_and_agree(
            &[
                "CREATE TABLE o(id INTEGER PRIMARY KEY)",
                "CREATE TABLE i(oid INTEGER, v INTEGER)",
                "INSERT INTO o VALUES (1),(2)",
                // non-sorted insertion order per group
                "INSERT INTO i VALUES (1,30),(1,10),(1,20),(2,5),(2,9),(2,1)",
            ],
            "SELECT id, (SELECT v FROM i WHERE i.oid=o.id ORDER BY v ASC LIMIT 1) FROM o ORDER BY id",
        )
        .await;
        assert_eq!(
            fr, rr,
            "GH#172: correlated subquery must honor ORDER BY v ASC (min per group)"
        );
    });
}
