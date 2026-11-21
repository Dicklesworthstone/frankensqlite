//! bd-u01ku (item 2): an INSERT, UPDATE or DELETE calling a function that does
//! not exist, or with a wrong number of arguments, is refused while it is
//! prepared, with stock SQLite 3.54.1's text: `UPDATE w SET x = nofunc(1)` is
//! "no such function: nofunc". fsqlite raised it only when the VDBE reached
//! the call, as "internal error: no such function: NOFUNC/1".
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on `:memory:`
//! and on a file, executed directly and prepared. Each statement's rows, or
//! its error text, must equal stock's, and is pinned to stock's.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> String {
    let result = (|| -> rusqlite::Result<Vec<Vec<String>>> {
        let mut statement = conn.prepare(sql)?;
        let columns = statement.column_count();
        let mut rows = Vec::new();
        let mut cursor = statement.query([])?;
        while let Some(row) = cursor.next()? {
            rows.push(
                (0..columns)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect(),
            );
        }
        Ok(rows)
    })();
    match result {
        Ok(rows) => format!("rows={rows:?}"),
        Err(
            rusqlite::Error::SqliteFailure(_, Some(message))
            | rusqlite::Error::SqlInputError { msg: message, .. },
        ) => format!("error: {message}"),
        Err(error) => format!("error: {error}"),
    }
}

/// `sql` on fsqlite, run directly or prepared first: a prepared SELECT is
/// queried, a prepared INSERT, UPDATE or DELETE executed (one with RETURNING
/// queried, for its rows).
async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let result = if prepared {
        match conn.prepare(sql).await {
            Ok(statement) if sql.starts_with("SELECT") || sql.contains("RETURNING") => {
                statement.query().await
            }
            Ok(statement) => statement.execute().await.map(|_| Vec::new()),
            Err(error) => Err(error),
        }
    } else {
        conn.query(sql).await
    };
    match result {
        Ok(rows) => {
            let rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.values().iter().map(render_fsqlite).collect())
                .collect();
            format!("rows={rows:?}")
        }
        Err(error) => format!("error: {error}"),
    }
}

const NO_SUCH: &str = "error: no such function: nofunc";
const ABS_ARITY: &str = "error: wrong number of arguments to function abs()";
const READ: &str = "SELECT x, y FROM w ORDER BY x";
const UNCHANGED: &str = "rows=[[\"1\", \"2\"]]";

const SETUP: &[&str] = &[
    "CREATE TABLE w(x, y)",
    "INSERT INTO w VALUES (1, 2)",
    "CREATE TABLE u(k UNIQUE, v)",
    "INSERT INTO u VALUES (1, 2)",
];

const STATEMENTS: &[(&str, &str)] = &[
    // The bead's shape, a name as written, each clause.
    ("UPDATE w SET x = nofunc(1)", NO_SUCH),
    (
        "UPDATE w SET x = NoFunc(1)",
        "error: no such function: NoFunc",
    ),
    ("UPDATE w SET x = 1 WHERE nofunc(x)", NO_SUCH),
    ("UPDATE w SET x = 1 RETURNING nofunc(x)", NO_SUCH),
    ("UPDATE w SET x = (SELECT nofunc(1))", NO_SUCH),
    ("UPDATE w SET x = nofunc(1) WHERE nope", NO_SUCH),
    ("DELETE FROM w WHERE nofunc(x)", NO_SUCH),
    ("DELETE FROM w RETURNING nofunc(x)", NO_SUCH),
    ("INSERT INTO w VALUES (nofunc(1), 2)", NO_SUCH),
    ("INSERT INTO w VALUES (1, 2) RETURNING nofunc(x)", NO_SUCH),
    ("INSERT INTO w SELECT nofunc(1), 2", NO_SUCH),
    // An upsert's DO UPDATE, its WHERE, and one whose row does not conflict.
    (
        "INSERT INTO u VALUES (1, 2) ON CONFLICT(k) DO UPDATE SET v = nofunc(1)",
        NO_SUCH,
    ),
    (
        "INSERT INTO u VALUES (1, 2) ON CONFLICT(k) DO UPDATE SET v = 1 WHERE nofunc(v)",
        NO_SUCH,
    ),
    (
        "INSERT INTO u VALUES (5, 2) ON CONFLICT(k) DO UPDATE SET v = nofunc(1)",
        NO_SUCH,
    ),
    ("SELECT k, v FROM u", "rows=[[\"1\", \"2\"]]"),
    // Wrong argument counts.
    ("UPDATE w SET x = abs(1, 2)", ABS_ARITY),
    ("UPDATE w SET x = abs()", ABS_ARITY),
    ("DELETE FROM w WHERE abs(x, 1)", ABS_ARITY),
    // Nothing ran.
    (READ, UNCHANGED),
    // Negatives: known functions run.
    ("UPDATE w SET x = abs(-3) WHERE length(y) = 1", "rows=[]"),
    (
        "INSERT INTO w VALUES (upper('a'), 4) RETURNING lower(x)",
        "rows=[[\"'a'\"]]",
    ),
    (
        "DELETE FROM w WHERE typeof(x) = 'text' RETURNING abs(y)",
        "rows=[[\"4\"]]",
    ),
    (READ, "rows=[[\"3\", \"2\"]]"),
];

#[test]
fn dml_naming_an_unknown_function_is_refused_with_stocks_text() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    let expected: Vec<String> = STATEMENTS
        .iter()
        .map(|(sql, _)| stock_outcome(&stock, sql))
        .collect();
    // Not vacuous: stock gives each pinned outcome.
    for ((sql, pinned), stock_gave) in STATEMENTS.iter().zip(&expected) {
        assert_eq!(stock_gave, pinned, "stock on `{sql}`");
    }
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_u01ku.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for ((sql, _), expected) in STATEMENTS.iter().zip(&expected) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected,
                        "file_backed={file_backed} prepared={prepared}: `{sql}`"
                    );
                }
            }
        }
    });
}
