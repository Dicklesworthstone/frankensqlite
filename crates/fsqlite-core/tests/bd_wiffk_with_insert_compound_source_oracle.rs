//! bd-wiffk: an INSERT with a leading WITH whose source SELECT is a compound
//! kept only its first arm, so rows were lost:
//!
//! ```sql
//! CREATE TABLE t(x);
//! WITH c(x) AS (SELECT 5) INSERT INTO t SELECT x FROM c UNION ALL SELECT 7;
//! ```
//!
//! leaves 5 and 7 in stock SQLite 3.54.1; fsqlite wrote 5. The source runs
//! through the memdb-fallback evaluators, which answer one SELECT core; an
//! aggregate first arm (`SELECT count(*) FROM c UNION ALL SELECT 2`) wrote no
//! row at all, and the compound's ORDER BY and LIMIT were ignored. The same
//! evaluators answered a WITH's consuming SELECT that reads an attached
//! table, so `WITH c AS (...) SELECT x FROM c UNION ALL SELECT y FROM aux.w`
//! lost its second arm too.
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

/// `sql` on fsqlite, run directly or prepared first: a prepared query is
/// queried (including INSERT RETURNING), a prepared INSERT executed.
async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let query = sql.starts_with("SELECT") || (sql.starts_with("WITH") && !sql.contains(" INSERT "));
    let dml = sql.contains(" INSERT ") || sql.starts_with("INSERT");
    let result = if prepared && (query || dml) {
        match conn.prepare(sql).await {
            Ok(statement) if query || sql.contains("RETURNING") => statement.query().await,
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

struct Script {
    name: &'static str,
    /// Its final SELECT can be repeated by stock SQLite after closing the file.
    persistent_result: bool,
    setup: &'static [&'static str],
    statements: &'static [(&'static str, &'static str)],
}

const NO_ROWS: &str = "rows=[]";

const INSERTS: Script = Script {
    name: "inserts",
    persistent_result: true,
    setup: &[
        "CREATE TABLE t(x)",
        "CREATE TABLE u(y)",
        "INSERT INTO u VALUES (100), (50)",
        "CREATE TABLE t2(a, b)",
    ],
    statements: &[
        // The bead's shape, and its change count.
        (
            "WITH c(x) AS (SELECT 5) INSERT INTO t SELECT x FROM c UNION ALL SELECT 7",
            NO_ROWS,
        ),
        ("SELECT changes()", "rows=[[\"2\"]]"),
        ("SELECT x FROM t ORDER BY x", "rows=[[\"5\"], [\"7\"]]"),
        // The compound's ORDER BY and LIMIT, UNION and EXCEPT over a table,
        // a VALUES arm, an aggregate first arm, INTERSECT.
        (
            "WITH c(x) AS (SELECT 5) INSERT INTO t \
             SELECT x FROM c UNION SELECT 10 ORDER BY 1 DESC LIMIT 1",
            NO_ROWS,
        ),
        (
            "SELECT x FROM t ORDER BY x",
            "rows=[[\"5\"], [\"7\"], [\"10\"]]",
        ),
        (
            "WITH c(x) AS (SELECT 5) INSERT INTO t \
             SELECT x FROM c UNION SELECT y FROM u EXCEPT SELECT 50",
            NO_ROWS,
        ),
        (
            "SELECT x FROM t ORDER BY x",
            "rows=[[\"5\"], [\"5\"], [\"7\"], [\"10\"], [\"100\"]]",
        ),
        (
            "WITH c(x) AS (SELECT 5) INSERT INTO t SELECT x FROM c UNION ALL VALUES (9)",
            NO_ROWS,
        ),
        (
            "SELECT x FROM t ORDER BY x",
            "rows=[[\"5\"], [\"5\"], [\"5\"], [\"7\"], [\"9\"], [\"10\"], [\"100\"]]",
        ),
        (
            "WITH c(x) AS (SELECT 5) INSERT INTO t SELECT count(*) FROM c UNION ALL SELECT 2",
            NO_ROWS,
        ),
        (
            "SELECT x FROM t ORDER BY x",
            "rows=[[\"1\"], [\"2\"], [\"5\"], [\"5\"], [\"5\"], [\"7\"], [\"9\"], [\"10\"], \
             [\"100\"]]",
        ),
        (
            "WITH c(x) AS (SELECT 100 UNION ALL SELECT 3) INSERT INTO t \
             SELECT x FROM c INTERSECT SELECT y FROM u",
            NO_ROWS,
        ),
        // An arm reading the target sees it as it was before the INSERT.
        (
            "WITH c(x) AS (SELECT 1) INSERT INTO t \
             SELECT x FROM c UNION ALL SELECT max(x) + 1000 FROM t",
            NO_ROWS,
        ),
        (
            "SELECT x FROM t ORDER BY x",
            "rows=[[\"1\"], [\"1\"], [\"2\"], [\"5\"], [\"5\"], [\"5\"], [\"7\"], [\"9\"], \
             [\"10\"], [\"100\"], [\"100\"], [\"1100\"]]",
        ),
        // A column list and RETURNING.
        (
            "WITH c(x, y) AS (SELECT 1, 'p') INSERT INTO t2(b, a) \
             SELECT y, x FROM c UNION ALL SELECT 'q', 2 RETURNING a, b",
            "rows=[[\"1\", \"'p'\"], [\"2\", \"'q'\"]]",
        ),
        (
            "SELECT a, b FROM t2 ORDER BY a",
            "rows=[[\"1\", \"'p'\"], [\"2\", \"'q'\"]]",
        ),
        // Controls: a one-core source, and a WITH inside the source SELECT.
        (
            "WITH c(x) AS (SELECT 5) INSERT INTO t SELECT x + 1 FROM c",
            NO_ROWS,
        ),
        (
            "INSERT INTO t WITH c(x) AS (SELECT 8) SELECT x FROM c UNION ALL SELECT 9",
            NO_ROWS,
        ),
        (
            "SELECT count(*), sum(x) FROM t",
            "rows=[[\"15\", \"1368\"]]",
        ),
    ],
};

const ATTACHED: Script = Script {
    name: "attached",
    persistent_result: false,
    setup: &[
        "ATTACH ':memory:' AS aux",
        "CREATE TABLE aux.w(y)",
        "INSERT INTO aux.w VALUES (7)",
    ],
    statements: &[
        (
            "WITH c(x) AS (SELECT 1) SELECT x FROM c UNION ALL SELECT y FROM aux.w",
            "rows=[[\"1\"], [\"7\"]]",
        ),
        (
            "WITH c(x) AS (SELECT 1) SELECT y FROM aux.w UNION SELECT x FROM c ORDER BY 1 DESC",
            "rows=[[\"7\"], [\"1\"]]",
        ),
    ],
};

const RECURSIVE: Script = Script {
    name: "recursive",
    persistent_result: true,
    setup: &[
        "CREATE TABLE dst(x INTEGER UNIQUE)",
        "CREATE TABLE src(x)",
        "INSERT INTO src VALUES (8), (10)",
    ],
    statements: &[
        (
            "WITH RECURSIVE seq(x) AS (VALUES (1) UNION ALL SELECT x+1 FROM seq WHERE x<3) \
             INSERT INTO dst SELECT x FROM seq UNION ALL SELECT x FROM src ORDER BY 1 DESC LIMIT 3 OFFSET 1",
            NO_ROWS,
        ),
        (
            "SELECT x FROM dst ORDER BY rowid",
            "rows=[[\"8\"], [\"3\"], [\"2\"]]",
        ),
    ],
};

fn run_script(script: &Script) {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in script.setup {
        stock.execute_batch(sql).unwrap();
    }
    let expected: Vec<String> = script
        .statements
        .iter()
        .map(|(sql, _)| stock_outcome(&stock, sql))
        .collect();
    // Not vacuous: stock gives each pinned outcome.
    for ((sql, pinned), stock_gave) in script.statements.iter().zip(&expected) {
        assert_eq!(stock_gave, pinned, "{}: stock on `{sql}`", script.name);
    }
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("bd_wiffk.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in script.setup {
                    conn.execute(sql).await.unwrap();
                }
                for ((sql, _), expected) in script.statements.iter().zip(&expected) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    assert_eq!(
                        &got, expected,
                        "{}: file_backed={file_backed} prepared={prepared}: `{sql}`",
                        script.name
                    );
                }
                conn.close().await.unwrap();
                if file_backed && script.persistent_result {
                    let reopened = rusqlite::Connection::open(&path).unwrap();
                    let (sql, pinned) = script.statements.last().unwrap();
                    assert_eq!(stock_outcome(&reopened, sql), *pinned);
                    assert_eq!(
                        stock_outcome(&reopened, "PRAGMA integrity_check"),
                        "rows=[[\"'ok'\"]]"
                    );
                }
            }
        }
    });
}

#[test]
fn with_insert_compound_sources_write_every_arm_like_stock() {
    run_script(&INSERTS);
}

#[test]
fn with_select_compound_reading_an_attached_table_answers_every_arm_like_stock() {
    run_script(&ATTACHED);
}

#[test]
fn recursive_insert_applies_offset_to_the_whole_compound() {
    run_script(&RECURSIVE);
}

#[test]
fn bound_compound_insert_on_a_stock_file_preserves_the_statement_boundary() {
    const INSERT: &str = "WITH c(x) AS (SELECT ?) INSERT INTO dst \
        SELECT x FROM c UNION ALL SELECT ?2 UNION ALL SELECT ? ORDER BY 1 LIMIT ? OFFSET ?";
    const READ: &str = "SELECT x FROM dst ORDER BY rowid";
    asupersync::test_utils::run_test(|| async {
        for prepared in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("bound.db");
            let stock = rusqlite::Connection::open(&path).unwrap();
            stock
                .execute_batch("CREATE TABLE dst(x INTEGER UNIQUE)")
                .unwrap();
            stock.execute(INSERT, [11, 3, 9, 2, 1]).unwrap();
            let expected = stock_outcome(&stock, READ);
            assert_eq!(expected, "rows=[[\"9\"], [\"11\"]]");
            assert!(stock.execute(INSERT, [15, 1, 11, 2, 0]).is_err());
            assert_eq!(stock_outcome(&stock, READ), expected);
            stock.execute_batch("DELETE FROM dst").unwrap();
            drop(stock);

            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            for (values, success) in [([11, 3, 9, 2, 1], true), ([15, 1, 11, 2, 0], false)] {
                let params = values.map(SqliteValue::Integer);
                let result = if prepared {
                    conn.prepare(INSERT)
                        .await
                        .unwrap()
                        .execute_with_params(&params)
                        .await
                } else {
                    conn.execute_with_params(INSERT, &params).await
                };
                if success {
                    assert_eq!(result.unwrap(), 2);
                } else {
                    assert!(result.is_err(), "late unique violation: {result:?}");
                }
                assert_eq!(fsqlite_outcome(&conn, READ, prepared).await, expected);
            }
            conn.close().await.unwrap();
            let reopened = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(stock_outcome(&reopened, READ), expected);
            assert_eq!(
                stock_outcome(&reopened, "PRAGMA integrity_check"),
                "rows=[[\"'ok'\"]]"
            );
        }
    });
}
