//! bd-1m280: a WHERE-less `DELETE FROM p INDEXED BY <idx>` on a table that is an
//! FK *parent* must still take stock's truncate optimization when
//! `PRAGMA foreign_keys` is OFF (the default) — stock's `sqlite3FkRequired` is
//! FALSE then, so it truncates and ignores the forced index. frank previously
//! used a static `table_is_foreign_key_parent` check that disabled the truncate
//! regardless of enforcement, erroring "no query solution" on an uncovered
//! forced *partial* index where stock silently empties the table.
//!
//! Differential vs rusqlite (bundled SQLite). The decisive assertion is the row
//! count after the DELETE: fixed frank truncates (0), buggy frank left the rows
//! (or errored, leaving the count unchanged).

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
    match f.query(sql).await {
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
/// Run `setup` (errors ignored, matching each engine's own tolerance), then the
/// `delete` statement, then assert both engines agree on the `probe` query.
async fn agree_after_delete(setup: &[&str], delete: &str, probe: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let _ = f.execute(delete).await;
    let _ = r.execute_batch(delete);
    let fr = fq(&f, probe).await;
    let rr = rq(&r, probe);
    assert_eq!(fr, rr, "{msg}\n  frank={fr:?}\n  stock={rr:?}");
}

// FK parent + PARTIAL forced index, fk OFF (default): stock truncates the whole
// table (rows with v NULL and v NOT NULL both go), ignoring the partial index.
#[test]
fn delete_truncate_fk_parent_partial_index_fk_off() {
    asupersync::test_utils::run_test(|| async {
        agree_after_delete(
            &[
                "PRAGMA foreign_keys=OFF",
                "CREATE TABLE p(id INTEGER PRIMARY KEY, v)",
                "CREATE INDEX pi ON p(v) WHERE v IS NOT NULL",
                "CREATE TABLE c(x, pid REFERENCES p(id))",
                "INSERT INTO p VALUES (1,'a'),(2,NULL),(3,'b')",
                "INSERT INTO c VALUES (10, 1)",
            ],
            "DELETE FROM p INDEXED BY pi",
            "SELECT count(*) FROM p",
            "(bd-1m280) fk OFF: WHERE-less DELETE on FK parent must truncate via forced partial index",
        )
        .await;
    });
}

// FK parent + FULL forced index, fk OFF: truncate too.
#[test]
fn delete_truncate_fk_parent_full_index_fk_off() {
    asupersync::test_utils::run_test(|| async {
        agree_after_delete(
            &[
                "PRAGMA foreign_keys=OFF",
                "CREATE TABLE p(id INTEGER PRIMARY KEY, v)",
                "CREATE INDEX pi ON p(v)",
                "CREATE TABLE c(x, pid REFERENCES p(id))",
                "INSERT INTO p VALUES (1,'a'),(2,'b')",
                "INSERT INTO c VALUES (10, 1)",
            ],
            "DELETE FROM p INDEXED BY pi",
            "SELECT count(*) FROM p",
            "(bd-1m280) fk OFF: WHERE-less DELETE on FK parent must truncate via forced full index",
        )
        .await;
    });
}

// Control: a non-FK-parent table with a partial forced index, fk OFF — always
// truncated (this already matched stock; guards against a regression the fix
// direction could touch).
#[test]
fn delete_truncate_non_fk_parent_partial_index() {
    asupersync::test_utils::run_test(|| async {
        agree_after_delete(
            &[
                "PRAGMA foreign_keys=OFF",
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v)",
                "CREATE INDEX ti ON t(v) WHERE v IS NOT NULL",
                "INSERT INTO t VALUES (1,'a'),(2,NULL)",
            ],
            "DELETE FROM t INDEXED BY ti",
            "SELECT count(*) FROM t",
            "(bd-1m280) control: non-parent partial forced index truncates",
        )
        .await;
    });
}
