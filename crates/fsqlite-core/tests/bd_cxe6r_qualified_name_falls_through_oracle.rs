//! bd-cxe6r: a qualified column `c.x` whose nearest item named `c` has no `x`
//! binds the next item of that name outward, as stock SQLite's lookupName
//! does. fsqlite took the nearest item and failed: `SELECT (SELECT c.x FROM d
//! AS c) FROM c` was "no such column: c.x" where stock 3.54.1 reads the outer
//! c's x, and `WITH c(y) AS (SELECT 5) UPDATE main.c SET x = c.y FROM c` was
//! "no such column: c.y" where stock assigns the CTE's 5.
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
/// queried, any other prepared statement executed.
async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let query = sql.starts_with("SELECT") || (sql.starts_with("WITH") && !sql.contains(" UPDATE "));
    let result = if prepared {
        match conn.prepare(sql).await {
            Ok(statement) if query => statement.query().await,
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

const HUNDRED: &str = "rows=[[\"100\"]]";

const SETUP: &[&str] = &[
    "CREATE TABLE c(x)",
    "INSERT INTO c VALUES (100)",
    "CREATE TABLE d(y)",
    "INSERT INTO d VALUES (5)",
];

const STATEMENTS: &[(&str, &str)] = &[
    // The bead's shapes; one column each way in one expression; beside the
    // outer item's own column; an aggregate; an EXISTS in WHERE.
    ("SELECT (SELECT c.x FROM d AS c) FROM c", HUNDRED),
    (
        "WITH c(y) AS (SELECT 5) SELECT (SELECT c.x FROM c) FROM main.c",
        HUNDRED,
    ),
    (
        "SELECT (SELECT c.x + c.y FROM d AS c) FROM c",
        "rows=[[\"105\"]]",
    ),
    (
        "SELECT c.x, (SELECT c.y FROM d AS c) FROM c",
        "rows=[[\"100\", \"5\"]]",
    ),
    ("SELECT (SELECT max(c.x) FROM d AS c) FROM c", HUNDRED),
    (
        "SELECT x FROM c WHERE EXISTS (SELECT 1 FROM d AS c WHERE c.x = 100)",
        HUNDRED,
    ),
    // Two items of the name in one query, only one with the column.
    ("SELECT c.x FROM c, d AS c", HUNDRED),
    // Negatives: the inner item has the column; no item has it; the outer
    // item lacks it too.
    ("SELECT (SELECT c.y FROM d AS c) FROM c", "rows=[[\"5\"]]"),
    ("SELECT (SELECT c.x FROM c) FROM c", HUNDRED),
    (
        "SELECT (SELECT c.nope FROM d AS c) FROM c",
        "error: no such column: c.nope",
    ),
    (
        "SELECT (SELECT c.x FROM d AS c) FROM d",
        "error: no such column: c.x",
    ),
    // UPDATE ... FROM: the target and a CTE share the name c.
    (
        "WITH c(y) AS (SELECT 5) UPDATE main.c SET x = c.y FROM c",
        "rows=[]",
    ),
    ("SELECT x FROM main.c", "rows=[[\"5\"]]"),
    (
        "WITH c(y) AS (SELECT 7) UPDATE main.c SET x = c.x + c.y FROM c",
        "rows=[]",
    ),
    ("SELECT x FROM main.c", "rows=[[\"12\"]]"),
];

#[test]
fn a_qualified_name_falls_through_to_the_item_with_the_column_like_stock() {
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
                    dir.path().join("bd_cxe6r.db").to_str().unwrap().to_owned()
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
