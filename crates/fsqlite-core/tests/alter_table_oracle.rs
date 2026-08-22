//! Differential oracle: ALTER TABLE vs rusqlite (bundled SQLite 3.53). A probe
//! sweep found this surface stock-correct across 13 cases; this keeper locks it
//! in.
//!
//! Covers ADD COLUMN with a constant default (existing rows adopt it), without
//! a default (existing rows NULL), NOT NULL DEFAULT, and TEXT default; ADD then
//! INSERT specifying the new column; multiple ADD COLUMNs; RENAME COLUMN
//! (including use of the new name in WHERE); RENAME TABLE (data + index
//! survive); DROP COLUMN; pragma_table_info reflecting an added column; and a
//! view over the table being unaffected by an ADD COLUMN.

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
fn add_column() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INT)",
                "INSERT INTO t VALUES (1),(2)",
                "ALTER TABLE t ADD COLUMN n INT DEFAULT 9",
            ],
            "SELECT id, n FROM t ORDER BY id",
            "ADD COLUMN with constant default",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(id INT)",
                "INSERT INTO t VALUES (1)",
                "ALTER TABLE t ADD COLUMN s TEXT",
            ],
            "SELECT id, s FROM t ORDER BY id",
            "ADD COLUMN without default -> NULL",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(id INT)",
                "INSERT INTO t VALUES (1),(2)",
                "ALTER TABLE t ADD COLUMN flag INT NOT NULL DEFAULT 0",
            ],
            "SELECT id, flag FROM t ORDER BY id",
            "ADD COLUMN NOT NULL DEFAULT",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(id INT)",
                "INSERT INTO t VALUES (1),(2)",
                "ALTER TABLE t ADD COLUMN label TEXT DEFAULT 'none'",
            ],
            "SELECT id, label FROM t ORDER BY id",
            "ADD COLUMN with TEXT default",
        )
        .await;
    });
}

#[test]
fn add_then_use() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(id INT)",
                "INSERT INTO t VALUES (1)",
                "ALTER TABLE t ADD COLUMN v INT DEFAULT 5",
                "INSERT INTO t(id,v) VALUES (2,20)",
            ],
            "SELECT id, v FROM t ORDER BY id",
            "ADD COLUMN then INSERT specifying it",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(id INT)",
                "INSERT INTO t VALUES (1)",
                "ALTER TABLE t ADD COLUMN x INT DEFAULT 1",
                "ALTER TABLE t ADD COLUMN y TEXT DEFAULT 'z'",
            ],
            "SELECT id, x, y FROM t",
            "multiple ADD COLUMNs",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(id INT)",
                "ALTER TABLE t ADD COLUMN extra INT DEFAULT 0",
            ],
            "SELECT name, type FROM pragma_table_info('t') ORDER BY cid",
            "pragma_table_info reflects the added column",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(id INT, v INT)",
                "INSERT INTO t VALUES (1,10)",
                "CREATE VIEW vw AS SELECT id, v FROM t",
                "ALTER TABLE t ADD COLUMN w INT DEFAULT 0",
            ],
            "SELECT id, v FROM vw ORDER BY id",
            "a view is unaffected by ADD COLUMN",
        )
        .await;
    });
}

#[test]
fn rename_column() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(a INT, b INT)",
                "INSERT INTO t VALUES (1,2)",
                "ALTER TABLE t RENAME COLUMN b TO c",
            ],
            "SELECT a, c FROM t",
            "RENAME COLUMN",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(a INT, old INT)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "ALTER TABLE t RENAME COLUMN old TO new",
            ],
            "SELECT a FROM t WHERE new > 15 ORDER BY a",
            "use the renamed column in WHERE",
        )
        .await;
    });
}

#[test]
fn rename_table_and_drop_column() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(x INT)",
                "INSERT INTO t VALUES (7)",
                "ALTER TABLE t RENAME TO t2",
            ],
            "SELECT x FROM t2",
            "RENAME TABLE",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(x INT)",
                "CREATE INDEX ix ON t(x)",
                "INSERT INTO t VALUES (3),(1),(2)",
                "ALTER TABLE t RENAME TO t2",
            ],
            "SELECT x FROM t2 WHERE x > 1 ORDER BY x",
            "index survives RENAME TABLE",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(a INT, b INT, c INT)",
                "INSERT INTO t VALUES (1,2,3)",
                "ALTER TABLE t DROP COLUMN b",
            ],
            "SELECT * FROM t",
            "DROP COLUMN",
        )
        .await;
    });
}
