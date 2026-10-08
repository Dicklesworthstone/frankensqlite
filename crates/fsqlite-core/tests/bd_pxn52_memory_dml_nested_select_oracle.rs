#![recursion_limit = "512"]

//! bd-pxn52: on a `:memory:` database, a DML statement whose WHERE needs a
//! nested SELECT over the target table (a correlated EXISTS or IN subquery)
//! must see the rows the earlier statements wrote, even when no SELECT has run
//! on the connection since. `DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2
//! ...)` on a WITHOUT ROWID table deleted nothing right after the INSERTs: the
//! lane froze its rows with a nested SELECT whose join fallback trusted the
//! MemDatabase mirror, and a WITHOUT ROWID insert (an IdxInsert on the table's
//! b-tree) never marked that mirror stale, so it still held no rows. Each case
//! runs its setup, then the DML as the first statement after the writes
//! (autocommit, and inside BEGIN ... COMMIT), and compares the changed-row
//! count and the table with stock SQLite (rusqlite, bundled), on `:memory:` and
//! on a file. (A correlated `k IN (SELECT ...)` DELETE on a WITHOUT ROWID table
//! fails in every mode for a different reason, bd-1jnfu.)

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const WITHOUT_ROWID: &[&str] = &[
    "CREATE TABLE t(k PRIMARY KEY, v) WITHOUT ROWID",
    "INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'c'),(5,'c')",
];
const WITHOUT_ROWID_TEXT_KEY: &[&str] = &[
    "CREATE TABLE t(v, k TEXT PRIMARY KEY) WITHOUT ROWID",
    "INSERT INTO t VALUES ('a','k1'),('a','k2'),('b','k3'),('c','k4'),('c','k5')",
];
const ROWID: &[&str] = &[
    "CREATE TABLE t(k, v)",
    "INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'c'),(5,'c')",
];
const TWO_TABLES: &[&str] = &[
    "CREATE TABLE t(k PRIMARY KEY, v) WITHOUT ROWID",
    "CREATE TABLE u(k PRIMARY KEY) WITHOUT ROWID",
    "INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'c'),(5,'c')",
    "INSERT INTO u VALUES (2),(4)",
];

const STATE: &str = "SELECT * FROM t ORDER BY 1, 2";

const CASES: &[(&[&str], &str)] = &[
    (
        WITHOUT_ROWID,
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.v = t.v AND t2.k < t.k)",
    ),
    (
        WITHOUT_ROWID,
        "UPDATE t SET v = v || '!' WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.v = t.v AND t2.k <> t.k)",
    ),
    (
        WITHOUT_ROWID_TEXT_KEY,
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.v = t.v AND t2.k < t.k)",
    ),
    (
        ROWID,
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.v = t.v AND t2.k < t.k)",
    ),
    (
        ROWID,
        "UPDATE t SET v = v || '!' WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.v = t.v AND t2.k <> t.k)",
    ),
    (
        TWO_TABLES,
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.k)",
    ),
    (
        TWO_TABLES,
        "DELETE FROM t WHERE k IN (SELECT k FROM u) OR EXISTS (SELECT 1 FROM t AS t2 WHERE t2.v = t.v AND t2.k > t.k)",
    ),
];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = r.prepare(sql).expect("stock prepare");
    let n = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock query")
}

async fn frank_rows(f: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    f.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect()
        })
        .map_err(|e| e.to_string())
}

async fn run(file_backed: bool, explicit_txn: bool) -> Vec<String> {
    let mut failures = Vec::new();
    for (index, (setup, statement)) in CASES.iter().enumerate() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = if file_backed {
            dir.path().join("pxn52.db").to_str().expect("utf-8 path").to_owned()
        } else {
            ":memory:".to_owned()
        };
        let f = Connection::open(&path).await.expect("open");
        let r = rusqlite::Connection::open_in_memory().expect("stock open");
        if explicit_txn {
            f.execute("BEGIN").await.expect("frank begin");
            r.execute_batch("BEGIN").expect("stock begin");
        }
        for sql in *setup {
            f.execute(sql).await.expect("frank setup");
            r.execute(sql, []).expect("stock setup");
        }
        // The DML is the first statement after the writes: no SELECT has
        // loaded anything since.
        let frank_changes = f.execute(statement).await.map_err(|e| e.to_string());
        let stock_changes = r.execute(statement, []).map_err(|e| e.to_string());
        if explicit_txn {
            f.execute("COMMIT").await.expect("frank commit");
            r.execute_batch("COMMIT").expect("stock commit");
        }
        let frank_state = frank_rows(&f, STATE).await;
        let stock_state = stock_rows(&r, STATE);
        let label = format!(
            "[file_backed={file_backed} txn={explicit_txn} case={index}] `{statement}`"
        );
        if frank_changes.as_ref().ok() != stock_changes.as_ref().ok() {
            failures.push(format!(
                "{label}: changes frank {frank_changes:?} vs stock {stock_changes:?}"
            ));
        }
        if frank_state.as_ref().ok() != Some(&stock_state) {
            failures.push(format!(
                "{label}: table frank {frank_state:?} vs stock {stock_state:?}"
            ));
        }
        f.close().await.expect("close");
    }
    failures
}

#[test]
fn memory_dml_with_a_nested_select_sees_prior_writes() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            for explicit_txn in [false, true] {
                failures.extend(run(file_backed, explicit_txn).await);
            }
        }
        assert!(
            failures.is_empty(),
            "{} failures:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
