#![recursion_limit = "512"]

//! Review of c52c2f112 (bd-jdjee): writes that read the row mirror after the
//! deferred `:memory:` mirror rebuild must see the connection's own parked
//! autocommit writes.
//!
//! bd-jdjee stopped DDL and explicit COMMIT from rebuilding the `:memory:` row
//! mirror and armed a one-time rebuild for the next read statement instead.
//! A read that touches no table (`SELECT 1`, `SELECT changes()`) reached that
//! rebuild while the previous autocommit writes were still parked in a
//! retained batch. The rebuild reads the published pager image, which does not
//! hold the batch, and marked the mirror current, so the next write that reads
//! the mirror saw the batch's tables as they were before it:
//!
//! ```sql
//! CREATE TABLE t (id INTEGER PRIMARY KEY, v, w);  -- arms the rebuild
//! INSERT INTO t SELECT ...;                       -- parked in the batch
//! SELECT 1;                                        -- rebuilt without it
//! INSERT INTO t2 SELECT w, sum(v) FROM t GROUP BY w;  -- inserted 0 rows
//! ```
//!
//! The same lost rows showed up in UPDATE ... FROM over an aggregate subquery
//! or CTE, DELETE ... IN (aggregate), and UPSERT from an aggregate. Plain reads
//! were right, because a read that touches a table flushes the batch first.
//! Every shape here runs against stock SQLite on the same statements, in
//! memory and file-backed.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => "null".to_owned(),
                    SqliteValue::Integer(i) => format!("i:{i}"),
                    SqliteValue::Float(f) => format!("r:{f}"),
                    SqliteValue::Text(t) => format!("t:{t}"),
                    SqliteValue::Blob(b) => format!("b:{b:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn render_stock(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).expect("stock prepare");
    let columns = stmt.column_count();
    stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(columns);
        for i in 0..columns {
            cells.push(match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => "null".to_owned(),
                rusqlite::types::ValueRef::Integer(v) => format!("i:{v}"),
                rusqlite::types::ValueRef::Real(v) => format!("r:{v}"),
                rusqlite::types::ValueRef::Text(v) => {
                    format!("t:{}", String::from_utf8_lossy(v))
                }
                rusqlite::types::ValueRef::Blob(v) => format!("b:{v:?}"),
            });
        }
        Ok(cells.join("|"))
    })
    .expect("stock query")
    .collect::<Result<Vec<_>, _>>()
    .expect("stock rows")
}

/// The last statement is an autocommit write, so its rows are still parked in
/// the retained batch when the next statement starts; the DDL before it armed
/// the deferred rebuild.
const SETUP: &[&str] = &[
    "CREATE TABLE nums (value INTEGER PRIMARY KEY)",
    "WITH RECURSIVE g(v) AS (SELECT 1 UNION ALL SELECT v + 1 FROM g WHERE v < 120) \
     INSERT INTO nums SELECT v FROM g",
    "CREATE TABLE t (id INTEGER PRIMARY KEY, v, w)",
    "CREATE INDEX t_v ON t(v)",
    "CREATE TABLE t2 (a, b)",
    "INSERT INTO t SELECT value, value * 10, value % 3 FROM nums WHERE value <= 90",
];

/// Statements between the setup and the write under test. The storage-free
/// ones reach the deferred rebuild without flushing the batch.
const BETWEEN: &[&[&str]] = &[
    &["SELECT 1"],
    &["SELECT changes()"],
    &["SELECT 1", "SELECT 2"],
    // The first write runs before the storage-free read (the reported shape).
    &[
        "UPDATE t SET v = n.v + 1 FROM t AS n WHERE n.id = t.id + 1",
        "SELECT changes()",
    ],
    &["UPDATE t SET v = v + 11", "SELECT 1"],
    // Controls: no read, and a read that touches the batch's table.
    &[],
    &["SELECT count(*) FROM t"],
];

const WRITES: &[&str] = &[
    "INSERT INTO t2 SELECT w, sum(v) FROM t GROUP BY w",
    "INSERT INTO t2 SELECT t.w, agg.sv FROM t, (SELECT w, sum(v) AS sv FROM t GROUP BY w) AS agg \
     WHERE agg.w = t.w",
    "WITH agg AS (SELECT w, sum(v) AS sv FROM t GROUP BY w) INSERT INTO t2 SELECT w, sv FROM agg",
    "UPDATE t SET v = agg.sv FROM (SELECT w, sum(v) AS sv FROM t GROUP BY w) AS agg \
     WHERE agg.w = t.w AND t.id % 10 = 0",
    "WITH agg AS (SELECT w, sum(v) AS sv FROM t GROUP BY w) \
     UPDATE t SET v = agg.sv FROM agg WHERE agg.w = t.w AND t.id % 10 = 0",
    "DELETE FROM t WHERE w IN (SELECT w FROM (SELECT w, sum(v) FROM t GROUP BY w)) AND id % 10 = 0",
    "INSERT INTO t(id, v, w) SELECT w + 1, sum(v), 9 FROM t GROUP BY w \
     ON CONFLICT(id) DO UPDATE SET v = excluded.v",
    "UPDATE t SET v = (SELECT sum(v) FROM t AS s WHERE s.w = t.w) WHERE id % 10 = 0",
];

const CHECKS: &[&str] = &[
    "SELECT changes()",
    "SELECT id, v, w FROM t ORDER BY id",
    "SELECT a, b FROM t2 ORDER BY a, b",
];

async fn run_case(conn: &Connection, between: &[&str], write: &str) -> Vec<String> {
    let mut out = Vec::new();
    for sql in SETUP.iter().chain(between) {
        if sql.starts_with("SELECT") {
            let rows = conn
                .query(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
            out.extend(render_fsqlite(&rows));
        } else {
            conn.execute(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }
    conn.execute(write)
        .await
        .unwrap_or_else(|e| panic!("{write}: {e}"));
    for sql in CHECKS {
        let rows = conn
            .query(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        out.extend(render_fsqlite(&rows));
    }
    out
}

fn stock_case(between: &[&str], write: &str) -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().expect("stock");
    let mut out = Vec::new();
    for sql in SETUP.iter().chain(between) {
        if sql.starts_with("SELECT") {
            out.extend(render_stock(&conn, sql));
        } else {
            conn.execute_batch(sql).expect("stock statement");
        }
    }
    conn.execute_batch(write).expect("stock write");
    for sql in CHECKS {
        out.extend(render_stock(&conn, sql));
    }
    out
}

#[test]
fn mirror_reading_writes_see_parked_autocommit_rows_after_a_storage_free_read() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mismatches = Vec::new();
        let mut cases = 0;
        for (between_index, between) in BETWEEN.iter().enumerate() {
            for (write_index, write) in WRITES.iter().enumerate() {
                let expected = stock_case(between, write);
                for file_backed in [false, true] {
                    let path = if file_backed {
                        dir.path()
                            .join(format!("case_{between_index}_{write_index}.db"))
                            .to_string_lossy()
                            .into_owned()
                    } else {
                        ":memory:".to_owned()
                    };
                    let conn = Connection::open(path.as_str()).await.expect("open");
                    let ours = run_case(&conn, between, write).await;
                    conn.close().await.expect("close");
                    cases += 1;
                    if ours != expected {
                        mismatches.push(format!(
                            "{} | between {between:?} | {write}\n  fsqlite: {}\n  stock:   {}",
                            if file_backed { "file" } else { "memory" },
                            summarize(&ours),
                            summarize(&expected),
                        ));
                    }
                }
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {cases} cases diverged from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}

fn summarize(rows: &[String]) -> String {
    const SHOWN: usize = 12;
    let head = rows
        .iter()
        .take(SHOWN)
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    if rows.len() > SHOWN {
        format!("{head} ... ({} values)", rows.len())
    } else {
        head
    }
}
