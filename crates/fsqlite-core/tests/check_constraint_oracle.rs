//! Differential oracle: CHECK constraint enforcement vs rusqlite (bundled
//! SQLite 3.53). A probe sweep found this surface stock-correct across 12 cases;
//! this keeper locks it in.
//!
//! Error-agnostic strategy: run a batch of INSERT/UPDATE where some rows violate
//! the CHECK (both engines reject them; the test driver ignores the per-statement
//! error) then SELECT the survivors. Identical surviving state on both engines
//! means identical enforcement — no dependence on the exact error string. Key
//! semantics asserted: a CHECK passes when it evaluates to anything other than
//! FALSE, so a NULL operand does NOT reject the row.

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

/// Run setup (violating writes are silently rejected by both engines), then
/// assert the surviving state matches.
async fn agree(setup: &[&str], sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(
        fr, rr,
        "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}"
    );
}

#[test]
fn check_greater_than() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(x INT CHECK (x > 0))",
                "INSERT INTO t VALUES (5)",
                "INSERT INTO t VALUES (-1)",
                "INSERT INTO t VALUES (0)",
            ],
            "SELECT x FROM t ORDER BY x",
            "column CHECK (x > 0): only 5 survives",
        )
        .await;
    });
}

#[test]
fn check_null_operand_passes() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(x INT CHECK (x > 0))",
                "INSERT INTO t VALUES (NULL)",
                "INSERT INTO t VALUES (3)",
                "INSERT INTO t VALUES (-2)",
            ],
            "SELECT x FROM t ORDER BY x",
            "NULL passes a CHECK (constraint not FALSE)",
        )
        .await;
    });
}

#[test]
fn check_table_level_multicolumn() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(a INT, b INT, CHECK (a < b))",
                "INSERT INTO t VALUES (1, 2)",
                "INSERT INTO t VALUES (3, 3)",
                "INSERT INTO t VALUES (5, 4)",
            ],
            "SELECT a, b FROM t ORDER BY a",
            "table-level CHECK (a < b)",
        )
        .await;
    });
}

#[test]
fn check_length_expression() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(s TEXT CHECK (length(s) <= 3))",
                "INSERT INTO t VALUES ('ok')",
                "INSERT INTO t VALUES ('toolong')",
                "INSERT INTO t VALUES ('abc')",
            ],
            "SELECT s FROM t ORDER BY s",
            "CHECK with length() expression",
        )
        .await;
    });
}

#[test]
fn check_in_set() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(c TEXT CHECK (c IN ('r','g','b')))",
                "INSERT INTO t VALUES ('r')",
                "INSERT INTO t VALUES ('x')",
                "INSERT INTO t VALUES ('b')",
            ],
            "SELECT c FROM t ORDER BY c",
            "CHECK with IN set",
        )
        .await;
    });
}

#[test]
fn check_or() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(n INT CHECK (n < 0 OR n > 100))",
                "INSERT INTO t VALUES (-5)",
                "INSERT INTO t VALUES (50)",
                "INSERT INTO t VALUES (200)",
            ],
            "SELECT n FROM t ORDER BY n",
            "CHECK with OR",
        )
        .await;
    });
}

#[test]
fn check_enforced_on_update() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(x INT CHECK (x > 0))",
                "INSERT INTO t VALUES (5)",
                "UPDATE t SET x = -1",
                "UPDATE t SET x = 9",
            ],
            "SELECT x FROM t",
            "CHECK on UPDATE: violating update rejected, then 9 lands",
        )
        .await;
    });
}

#[test]
fn check_named_constraint() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(x INT CONSTRAINT positive CHECK (x > 0))",
                "INSERT INTO t VALUES (7)",
                "INSERT INTO t VALUES (-3)",
            ],
            "SELECT x FROM t ORDER BY x",
            "named CHECK constraint",
        )
        .await;
    });
}

#[test]
fn check_with_default_column() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INT, x INT DEFAULT 10 CHECK (x >= 5))",
                "INSERT INTO t(id) VALUES (1)",
                "INSERT INTO t(id, x) VALUES (2, 3)",
                "INSERT INTO t(id, x) VALUES (3, 8)",
            ],
            "SELECT id, x FROM t ORDER BY id",
            "CHECK evaluated against the DEFAULT for an omitted column",
        )
        .await;
    });
}

#[test]
fn check_arithmetic() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(n INT CHECK (n % 2 = 0))",
                "INSERT INTO t VALUES (4)",
                "INSERT INTO t VALUES (7)",
                "INSERT INTO t VALUES (0)",
            ],
            "SELECT n FROM t ORDER BY n",
            "CHECK with modulo arithmetic",
        )
        .await;
    });
}

#[test]
fn check_two_constraints_on_column() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(x INT CHECK (x > 0) CHECK (x < 100))",
                "INSERT INTO t VALUES (50)",
                "INSERT INTO t VALUES (-1)",
                "INSERT INTO t VALUES (150)",
            ],
            "SELECT x FROM t ORDER BY x",
            "two CHECKs on one column both enforced",
        )
        .await;
    });
}

#[test]
fn check_upper_expression() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(s TEXT CHECK (upper(s) = s))",
                "INSERT INTO t VALUES ('ABC')",
                "INSERT INTO t VALUES ('abc')",
                "INSERT INTO t VALUES ('XyZ')",
            ],
            "SELECT s FROM t ORDER BY s",
            "CHECK with upper() equality",
        )
        .await;
    });
}
