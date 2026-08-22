//! Differential oracle: DML write paths, RETURNING, and LIMIT/OFFSET vs
//! rusqlite (bundled SQLite 3.53). A probe sweep found this surface
//! stock-correct across 15 cases; this keeper locks it in.
//!
//! Covers INSERT ... SELECT, INSERT DEFAULT VALUES, multi-row INSERT with
//! expressions, UPDATE with an expression / a correlated subquery / a FROM
//! clause (SQLite 3.33+), DELETE with a WHERE / a NOT IN subquery, RETURNING on
//! INSERT/UPDATE/DELETE (rows and final state), and LIMIT/OFFSET edges
//! (LIMIT -1 = no limit, OFFSET past end, `LIMIT off, lim` comma syntax).

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

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Run setup + a final read on both engines and assert the read agrees. The DML
/// under test is part of `setup` (its effect shows in the read).
async fn agree(setup: &[&str], read: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, read).await;
    let rr = rq(&r, read);
    assert_eq!(
        fr, rr,
        "{msg}\n  read  ={read}\n  frank ={fr:?}\n  sqlite={rr:?}"
    );
}

/// Execute a RETURNING statement on both engines; assert the RETURNING rows
/// (order-normalized) AND the final table state agree.
async fn agree_returning(setup: &[&str], write: &str, read: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let mut fret = fq(&f, write).await;
    let mut rret = rq(&r, write);
    fret.sort();
    rret.sort();
    assert_eq!(fret, rret, "{msg} (RETURNING rows)\n  sql={write}");
    let fstate = fq(&f, read).await;
    let rstate = rq(&r, read);
    assert_eq!(fstate, rstate, "{msg} (final state)\n  read={read}");
}

#[test]
fn insert_forms() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE a(x INT)",
                "CREATE TABLE b(x INT)",
                "INSERT INTO a VALUES (1),(2),(3)",
                "INSERT INTO b SELECT x*10 FROM a WHERE x >= 2",
            ],
            "SELECT x FROM b ORDER BY x",
            "INSERT ... SELECT",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, n INT DEFAULT 7, s TEXT DEFAULT 'd')",
                "INSERT INTO t DEFAULT VALUES",
                "INSERT INTO t DEFAULT VALUES",
            ],
            "SELECT id, n, s FROM t ORDER BY id",
            "INSERT DEFAULT VALUES",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(a INT, b INT)",
                "INSERT INTO t VALUES (1+1, 2*3),(abs(-4), 5%2)",
            ],
            "SELECT a, b FROM t ORDER BY a",
            "multi-row INSERT with expressions",
        )
        .await;
    });
}

#[test]
fn update_forms() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INT, v INT)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
                "UPDATE t SET v = v + 100 WHERE id >= 2",
            ],
            "SELECT id, v FROM t ORDER BY id",
            "UPDATE with expression + WHERE",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(id INT, v INT)",
                "CREATE TABLE m(id INT, mul INT)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "INSERT INTO m VALUES (1,2),(2,3)",
                "UPDATE t SET v = v * (SELECT mul FROM m WHERE m.id = t.id)",
            ],
            "SELECT id, v FROM t ORDER BY id",
            "UPDATE with correlated subquery",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(id INT, v INT)",
                "CREATE TABLE m(id INT, add INT)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
                "INSERT INTO m VALUES (1,5),(3,7)",
                "UPDATE t SET v = v + m.add FROM m WHERE m.id = t.id",
            ],
            "SELECT id, v FROM t ORDER BY id",
            "UPDATE ... FROM",
        )
        .await;
    });
}

#[test]
fn delete_forms() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INT)",
                "INSERT INTO t VALUES (1),(2),(3),(4)",
                "DELETE FROM t WHERE id % 2 = 0",
            ],
            "SELECT id FROM t ORDER BY id",
            "DELETE with WHERE",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(id INT)",
                "CREATE TABLE keep(id INT)",
                "INSERT INTO t VALUES (1),(2),(3)",
                "INSERT INTO keep VALUES (2)",
                "DELETE FROM t WHERE id NOT IN (SELECT id FROM keep)",
            ],
            "SELECT id FROM t ORDER BY id",
            "DELETE with NOT IN subquery",
        )
        .await;
    });
}

#[test]
fn returning_clause() {
    asupersync::test_utils::run_test(|| async {
        agree_returning(
            &["CREATE TABLE t(id INTEGER PRIMARY KEY, v INT)"],
            "INSERT INTO t(v) VALUES (11),(22) RETURNING id, v",
            "SELECT id, v FROM t ORDER BY id",
            "RETURNING on INSERT",
        )
        .await;
        agree_returning(
            &[
                "CREATE TABLE t(id INT, v INT)",
                "INSERT INTO t VALUES (1,10),(2,20)",
            ],
            "UPDATE t SET v = v + 1 WHERE id = 2 RETURNING id, v",
            "SELECT id, v FROM t ORDER BY id",
            "RETURNING on UPDATE",
        )
        .await;
        agree_returning(
            &[
                "CREATE TABLE t(id INT, v INT)",
                "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
            ],
            "DELETE FROM t WHERE v >= 20 RETURNING id",
            "SELECT id FROM t ORDER BY id",
            "RETURNING on DELETE",
        )
        .await;
    });
}

#[test]
fn limit_offset_edges() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(x INT)",
                "INSERT INTO t VALUES (1),(2),(3),(4),(5)",
            ],
            "SELECT x FROM t ORDER BY x LIMIT 2 OFFSET 1",
            "LIMIT/OFFSET",
        )
        .await;
        agree(
            &["CREATE TABLE t(x INT)", "INSERT INTO t VALUES (1),(2),(3)"],
            "SELECT x FROM t ORDER BY x LIMIT -1",
            "LIMIT -1 means no limit",
        )
        .await;
        agree(
            &["CREATE TABLE t(x INT)", "INSERT INTO t VALUES (1),(2)"],
            "SELECT x FROM t ORDER BY x LIMIT 5 OFFSET 10",
            "OFFSET past end is empty",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(x INT)",
                "INSERT INTO t VALUES (1),(2),(3),(4)",
            ],
            "SELECT x FROM t ORDER BY x LIMIT 1, 2",
            "LIMIT offset, count comma syntax",
        )
        .await;
    });
}
