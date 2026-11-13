#![recursion_limit = "512"]

//! Broad differential conformance probe vs rusqlite (bundled SQLite) across
//! constraint / ON CONFLICT / affinity / multi-row areas. Each case checks the
//! externally-observable contract (rows or success-vs-error), not error strings.
//! Landed as a keeper regardless of outcome: it guards these behaviors from
//! regressing. Any RED found here is filed as its own bead and pinned #[ignore].

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

/// Compare a query's rows across both engines.
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
    assert_eq!(fr, rr, "{msg}\n  frank={fr:?}\n  stock={rr:?}");
}

/// Compare success-vs-error of a mutating statement, then the resulting rows.
async fn agree_stmt(setup: &[&str], stmt: &str, check: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = fx(&f, s).await;
        let _ = r.execute_batch(s);
    }
    let f_ok = fx(&f, stmt).await.is_ok();
    let r_ok = r.execute_batch(stmt).is_ok();
    assert_eq!(
        f_ok, r_ok,
        "{msg}: success-vs-error diverges (frank_ok={f_ok}, stock_ok={r_ok})"
    );
    let fr = fq(&f, check)
        .await
        .unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
    let rr = rq(&r, check).unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
    assert_eq!(
        fr, rr,
        "{msg}: post-state diverges\n  frank={fr:?}\n  stock={rr:?}"
    );
}

#[test]
fn probe_upsert_do_nothing_multi_row() {
    asupersync::test_utils::run_test(|| async {
        agree_stmt(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
                "INSERT INTO t VALUES(1,'a'),(2,'b')",
            ],
            "INSERT INTO t VALUES(3,'a'),(4,'c') ON CONFLICT(v) DO NOTHING",
            "SELECT id,v FROM t ORDER BY id",
            "UPSERT DO NOTHING skips only the conflicting row",
        )
        .await;
    });
}

#[test]
fn probe_check_constraint_and_affinity_update() {
    asupersync::test_utils::run_test(|| async {
        agree_stmt(
            &[
                "CREATE TABLE t(a NUMERIC CHECK(a >= 0))",
                "INSERT INTO t VALUES('5')",
            ],
            "UPDATE t SET a='10'",
            "SELECT typeof(a), a FROM t",
            "NUMERIC affinity applies '10'->10 before CHECK on UPDATE",
        )
        .await;
    });
}

#[test]
fn probe_insert_default_and_notnull() {
    asupersync::test_utils::run_test(|| async {
        agree_stmt(
            &["CREATE TABLE t(a INTEGER NOT NULL DEFAULT 7, b TEXT)"],
            "INSERT INTO t(b) VALUES('x')",
            "SELECT a,b FROM t",
            "NOT NULL column fills from DEFAULT when omitted",
        )
        .await;
    });
}

#[test]
fn probe_insert_notnull_explicit_null_errors() {
    asupersync::test_utils::run_test(|| async {
        agree_stmt(
            &["CREATE TABLE t(a INTEGER NOT NULL DEFAULT 7)"],
            "INSERT INTO t(a) VALUES(NULL)",
            "SELECT count(*) FROM t",
            "explicit NULL into NOT NULL errors (DEFAULT does not rescue)",
        )
        .await;
    });
}

#[test]
fn probe_unique_multicolumn_conflict() {
    asupersync::test_utils::run_test(|| async {
        agree_stmt(
            &[
                "CREATE TABLE t(a,b,UNIQUE(a,b))",
                "INSERT INTO t VALUES(1,2),(1,3)",
            ],
            "INSERT INTO t VALUES(1,2)",
            "SELECT count(*) FROM t",
            "multi-column UNIQUE rejects duplicate pair",
        )
        .await;
    });
}

#[test]
fn probe_integer_affinity_comparison() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(a INTEGER)",
                "INSERT INTO t VALUES(1),(2),(10)",
            ],
            "SELECT a FROM t WHERE a > '1' ORDER BY a",
            "INTEGER column vs text literal: numeric compare",
        )
        .await;
    });
}

#[test]
fn probe_text_affinity_comparison() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(a TEXT)",
                "INSERT INTO t VALUES('1'),('2'),('10')",
            ],
            "SELECT a FROM t WHERE a > 1 ORDER BY a",
            "TEXT column vs int literal: text-affinity compare",
        )
        .await;
    });
}

#[test]
fn probe_group_by_having_count() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(g INT, v INT)",
                "INSERT INTO t VALUES(1,10),(1,20),(2,5),(3,7),(3,8),(3,9)",
            ],
            "SELECT g, count(*), sum(v) FROM t GROUP BY g HAVING count(*) >= 2 ORDER BY g",
            "GROUP BY / HAVING count filter",
        )
        .await;
    });
}

#[test]
fn probe_distinct_and_order_nulls() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(a INT)",
                "INSERT INTO t VALUES(3),(1),(NULL),(1),(2),(NULL)",
            ],
            "SELECT DISTINCT a FROM t ORDER BY a",
            "DISTINCT + ORDER BY places NULLs first",
        )
        .await;
    });
}

#[test]
fn probe_delete_returning_rows() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)",
                "INSERT INTO t VALUES(1,10),(2,20),(3,30)",
            ],
            "DELETE FROM t WHERE v >= 20 RETURNING id, v",
            "DELETE ... RETURNING projects deleted rows",
        )
        .await;
    });
}

#[test]
fn probe_cte_recursive_series() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 5) \
             SELECT x FROM c ORDER BY x",
            "recursive CTE counts 1..5",
        )
        .await;
    });
}

#[test]
fn probe_window_running_sum() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INT, v INT)",
                "INSERT INTO t VALUES(1,10),(2,20),(3,30)",
            ],
            "SELECT id, sum(v) OVER (ORDER BY id) AS running FROM t ORDER BY id",
            "window running SUM over ORDER BY",
        )
        .await;
    });
}
