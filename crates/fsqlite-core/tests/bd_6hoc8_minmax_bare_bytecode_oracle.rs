#![recursion_limit = "512"]

//! bd-6hoc8: a whole-table aggregate with a min()/max() reads its bare columns
//! from the row that produced the extremum. SQLite does this in bytecode: an
//! `OP_CollSeq` before each min()/max() `AggStep` names a register the step
//! sets when the row is not its new extremum, and the bare columns load only
//! from rows that leave it clear. fsqlite routed the single-min()/max() shapes
//! to the row-materializing interpreter instead (several times slower than
//! the bytecode scan), and the bytecode path read the FIRST row's bare columns
//! whenever it ran: beside another aggregate (`SELECT sum(a), max(a), b`
//! returned the first row's `b`) and for every prepared statement
//! (`prepare("SELECT max(a) FILTER (WHERE c < 3), b FROM t")`).
//!
//! The bytecode aggregate now emits SQLite's skip register, the plain
//! single-table shapes stay on it, and every shape is compared with rusqlite,
//! ad hoc and prepared, in memory and file-backed. With several min()/max()
//! calls SQLite's last call decides the row (each `OP_CollSeq` clears the
//! shared register), which the bytecode reproduces.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{b:?}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(a, b, c)",
    "INSERT INTO t VALUES (3,'b1',1),(9,'b6',2),(1,'b2',1),(9,'b7',2),(NULL,'bn',3),(5,'b5',3),(2,'b3',1)",
    // Group 1 is all NULL; group 2 has leading NULLs before its maximum.
    "CREATE TABLE z(a, b, g)",
    "INSERT INTO z VALUES (NULL,'z1',1),(NULL,'z2',1),(NULL,'z3',1),\
     (NULL,'y1',2),(4,'y2',2),(NULL,'y3',2),(4,'y4',2)",
    "CREATE TABLE n(k TEXT COLLATE NOCASE, tag)",
    "INSERT INTO n VALUES ('b','lower-b'),('A','upper-a'),('B','upper-b'),('a','lower-a')",
    // Every storage class, with values that compare equal across classes.
    "CREATE TABLE m(a, b)",
    "INSERT INTO m VALUES (1,'int1'),(1.0,'real1'),('1','text1'),(x'01','blob'),(2.5,'r25'),\
     ('abc','t-abc'),(NULL,'null'),('ABC','t-ABC'),(x'00','blob0')",
    "CREATE TABLE e(a, b)",
];

/// Whole-table aggregates whose bare columns come from the min()/max() row.
/// Each reaches the bytecode aggregate.
const BYTECODE_QUERIES: &[&str] = &[
    "SELECT max(a), b FROM t",
    "SELECT min(a), b FROM t",
    "SELECT b, max(a) FROM t",
    "SELECT max(a), b, c FROM t WHERE c < 3",
    "SELECT max(a), b FROM t WHERE a < 9",
    "SELECT max(a), b FROM t WHERE b > 'b5'",
    "SELECT max(a + 0), upper(b), b || c FROM t",
    "SELECT max(a), rowid, b FROM t",
    "SELECT max(rowid), b FROM t",
    "SELECT max(c), b FROM t",
    "SELECT min(c), b FROM t",
    "SELECT max(length(b)), b FROM t",
    "SELECT max(b), a FROM t",
    "SELECT min(b), a FROM t",
    // HAVING repeats the aggregate, names it by alias, reads a bare column, or
    // filters on count().
    "SELECT max(a), b FROM t HAVING max(a) > 0",
    "SELECT max(a), b FROM t HAVING max(a) > 100",
    "SELECT max(a), b FROM t HAVING b = 'b6'",
    "SELECT max(a), b FROM t HAVING b = 'b1'",
    "SELECT max(a) AS mx, b FROM t HAVING mx > 4",
    "SELECT max(a), b FROM t HAVING count(*) > 3",
    "SELECT max(a), b FROM t WHERE c <> 2 HAVING b IS NOT NULL",
    // count() beside the min()/max().
    "SELECT max(a), count(*), b FROM t",
    "SELECT count(a), min(a), b FROM t",
    // A NULL argument loads the bare columns until the first non-NULL value,
    // so an all-NULL input reports its last row.
    "SELECT max(a), b FROM z WHERE g = 1",
    "SELECT max(a), b FROM z WHERE g = 2",
    "SELECT min(a), b FROM z",
    "SELECT max(a), b FROM z",
    "SELECT min(a), b FROM t WHERE a IS NULL",
    // Ties keep the earliest row under the argument's collation.
    "SELECT max(k), tag FROM n",
    "SELECT min(k), tag FROM n",
    "SELECT max(k COLLATE BINARY), tag FROM n",
    "SELECT min(k COLLATE BINARY), tag FROM n",
    "SELECT max(tag COLLATE NOCASE), k FROM n",
    // Cross-class ordering (numbers < text < blobs; 1 = 1.0).
    "SELECT max(a), b FROM m",
    "SELECT min(a), b FROM m",
    // Empty input, LIMIT and OFFSET.
    "SELECT max(a), b FROM e",
    "SELECT max(a), typeof(b), b FROM e",
    "SELECT max(a), b FROM t LIMIT 0",
    "SELECT max(a), b FROM t LIMIT 1 OFFSET 1",
];

/// Shapes the bytecode path used to answer from the first row. Other
/// aggregates beside a min()/max() never decide the bare-column row.
const OTHER_AGGREGATE_QUERIES: &[&str] = &[
    "SELECT sum(a), max(a), b FROM t",
    "SELECT total(a), min(a), b, c FROM t",
    "SELECT group_concat(b), max(a), b FROM t",
    "SELECT sum(a) * 2, min(a), b FROM t",
    "SELECT max(a), count(*) FILTER (WHERE c = 3), b FROM t",
    // Several min()/max() calls: the last one's skip decides, as in SQLite.
    "SELECT max(a), min(a), b FROM t",
    "SELECT min(a), max(a), b FROM t",
    "SELECT max(a), b FROM t HAVING min(a) > 0",
    "SELECT max(a) - min(a), b FROM t",
    // FILTER: while every min()/max() has one, a row the FILTER rejects
    // supplies the bare columns only when it is the first row (SQLite's
    // "magnet" register); otherwise it leaves the last decision standing.
    "SELECT max(a) FILTER (WHERE c < 3), b FROM t",
    "SELECT max(a) FILTER (WHERE c = 3), b FROM t",
    "SELECT max(a) FILTER (WHERE c > 5), b FROM t",
    "SELECT min(a) FILTER (WHERE c <> 2), b, c FROM t",
    "SELECT max(a) FILTER (WHERE g = 2), b FROM z",
    "SELECT max(a) FILTER (WHERE a IS NULL), b FROM z",
    "SELECT min(a) FILTER (WHERE c <> 2), max(a), b FROM t",
    "SELECT max(a), min(a) FILTER (WHERE c <> 2), b FROM t",
];

/// Ad-hoc execution keeps the interpreter for these (an aggregate inside a
/// larger result expression, DISTINCT); prepared statements compile them to
/// bytecode, which must agree.
const INTERPRETER_QUERIES: &[&str] = &[
    "SELECT max(a), max(a) + 1, b FROM t",
    "SELECT DISTINCT max(a), b FROM t",
];

/// Checked ad hoc only, on the interpreter. The prepared bytecode evaluates
/// an aggregate's wrapper expression after the scan, where a bare column
/// inside it reads NULL, and keeps the first row for a DISTINCT min()/max()
/// (both separate, pre-existing prepared-statement gaps).
const AD_HOC_ONLY_QUERIES: &[&str] = &[
    "SELECT max(a) || ':' || b FROM t",
    "SELECT max(DISTINCT a), b FROM t",
    "SELECT min(DISTINCT c), b FROM t",
];

async fn rows_f(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

async fn rows_prepared(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("franken prepare `{sql}`: {e:?}"))
        .query()
        .await
        .unwrap_or_else(|e| panic!("franken prepared query `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

fn rows_r(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..ncol)
            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect())
    })
    .expect("rusqlite query")
    .map(|r| r.unwrap())
    .collect()
}

async fn open_pair(file_backed: bool, dir: &tempfile::TempDir) -> (Connection, rusqlite::Connection) {
    let target = if file_backed {
        dir.path()
            .join("bd_6hoc8.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&target).await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        f.execute(sql).await.unwrap();
        r.execute(sql, []).unwrap();
    }
    (f, r)
}

#[test]
fn minmax_bare_columns_come_from_the_extremum_row() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = open_pair(file_backed, &dir).await;
            for sql in BYTECODE_QUERIES
                .iter()
                .chain(OTHER_AGGREGATE_QUERIES)
                .chain(INTERPRETER_QUERIES)
            {
                let stock = rows_r(&r, sql);
                assert_eq!(rows_f(&f, sql).await, stock, "query `{sql}`");
                assert_eq!(rows_prepared(&f, sql).await, stock, "prepared `{sql}`");
            }
            for sql in AD_HOC_ONLY_QUERIES {
                assert_eq!(rows_f(&f, sql).await, rows_r(&r, sql), "query `{sql}`");
            }
        });
    }
}

/// The single-table shapes run on bytecode: with the in-memory interpreter
/// fallbacks refused, they still answer, and still match SQLite.
#[test]
fn whole_table_minmax_bare_columns_stay_on_bytecode() {
    asupersync::test_utils::run_test(|| async move {
        let dir = tempfile::tempdir().unwrap();
        let (f, r) = open_pair(true, &dir).await;
        f.set_reject_mem_fallback(true);
        f.set_strict_mem_fallback_rejection(true);
        for sql in BYTECODE_QUERIES.iter().chain(OTHER_AGGREGATE_QUERIES) {
            assert_eq!(rows_f(&f, sql).await, rows_r(&r, sql), "query `{sql}`");
        }
        f.set_strict_mem_fallback_rejection(false);
        f.set_reject_mem_fallback(false);
    });
}
