//! bd-4l4y3: a row-value SET from a subquery, `SET (n, c) = (SELECT ...)`.
//! Stock SQLite 3.54.1 assigns the subquery's first row (NULLs when it has
//! none) after checking its width against the targets ("2 columns assigned 3
//! values"). fsqlite refused it in an upsert's DO UPDATE ("sub-select returns
//! 2 columns - expected 1"; "multi-column SET requires a row-value
//! expression" for `(SELECT excluded.n, excluded.c + 1)`), and in UPDATE for a
//! `*`, compound or VALUES source ("not implemented"), and it ran a source
//! naming a missing column (`(SELECT nope, 1 FROM s)`) without an error.
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

fn render_rows(rows: &[fsqlite_core::connection::Row]) -> String {
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect();
    format!("rows={rows:?}")
}

/// `sql` on fsqlite, run directly or prepared first: a prepared SELECT is
/// queried, a prepared INSERT, UPDATE or DELETE executed.
async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let result = if prepared {
        match conn.prepare(sql).await {
            Ok(statement) if sql.starts_with("SELECT") => statement.query().await,
            Ok(statement) => statement.execute().await.map(|_| Vec::new()),
            Err(error) => Err(error),
        }
    } else {
        conn.query(sql).await
    };
    match result {
        Ok(rows) => render_rows(&rows),
        Err(error) => format!("error: {error}"),
    }
}

struct Script {
    name: &'static str,
    setup: &'static [&'static str],
    statements: &'static [(&'static str, &'static str)],
}

const NO_ROWS: &str = "rows=[]";
const SETUP: &[&str] = &[
    "CREATE TABLE t(a UNIQUE, n, c)",
    "INSERT INTO t VALUES ('abc', 1, 2)",
    "CREATE TABLE s(p, q)",
    "INSERT INTO s VALUES (5, 6), (7, 8)",
    "CREATE TABLE e(p, q)",
];
const READ: &str = "SELECT a, n, c FROM t ORDER BY a";

const UPSERT: Script = Script {
    name: "upsert",
    setup: SETUP,
    statements: &[
        // The bead's two shapes.
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE SET (n, c) = (SELECT 7, 8)",
            NO_ROWS,
        ),
        (READ, "rows=[[\"'abc'\", \"7\", \"8\"]]"),
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE \
             SET (n, c) = (SELECT excluded.n, excluded.c + 1)",
            NO_ROWS,
        ),
        (READ, "rows=[[\"'abc'\", \"9\", \"10\"]]"),
        // Widths: three values, a compound of one column, one target.
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE SET (n, c) = (SELECT 1, 2, 3)",
            "error: 2 columns assigned 3 values",
        ),
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE \
             SET (n, c) = (SELECT 1 UNION ALL SELECT 2)",
            "error: 2 columns assigned 1 values",
        ),
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE SET (n) = (SELECT 1, 2)",
            "error: 1 columns assigned 2 values",
        ),
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE \
             SET (n, c) = (SELECT nope, 1 FROM s)",
            "error: no such column: nope",
        ),
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE \
             SET (n, c) = (SELECT p, q FROM s WHERE p > excluded.nope)",
            "error: no such column: excluded.nope",
        ),
        (READ, "rows=[[\"'abc'\", \"9\", \"10\"]]"),
        // `*`, an ordered source's first row, VALUES, no row (NULLs).
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE SET (n, c) = (SELECT * FROM s)",
            NO_ROWS,
        ),
        (READ, "rows=[[\"'abc'\", \"5\", \"6\"]]"),
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE \
             SET (n, c) = (SELECT p, q FROM s ORDER BY p DESC)",
            NO_ROWS,
        ),
        (READ, "rows=[[\"'abc'\", \"7\", \"8\"]]"),
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE \
             SET (n, c) = (VALUES (11, 12))",
            NO_ROWS,
        ),
        (READ, "rows=[[\"'abc'\", \"11\", \"12\"]]"),
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE \
             SET (n, c) = (SELECT p, q FROM e)",
            NO_ROWS,
        ),
        (READ, "rows=[[\"'abc'\", \"NULL\", \"NULL\"]]"),
        // Controls: a row value, and a row that does not conflict.
        (
            "INSERT INTO t VALUES ('abc', 9, 9) ON CONFLICT(a) DO UPDATE SET (n, c) = (3, 4)",
            NO_ROWS,
        ),
        (
            "INSERT INTO t VALUES ('xyz', 9, 9) ON CONFLICT(a) DO UPDATE SET (n, c) = (SELECT 1, 2)",
            NO_ROWS,
        ),
        (
            READ,
            "rows=[[\"'abc'\", \"3\", \"4\"], [\"'xyz'\", \"9\", \"9\"]]",
        ),
    ],
};

const UPDATE: Script = Script {
    name: "update",
    setup: SETUP,
    statements: &[
        // The bead's three sources.
        ("UPDATE t SET (n, c) = (SELECT * FROM s)", NO_ROWS),
        (READ, "rows=[[\"'abc'\", \"5\", \"6\"]]"),
        (
            "UPDATE t SET (n, c) = (SELECT 1, 2 UNION ALL SELECT 3, 4)",
            NO_ROWS,
        ),
        (READ, "rows=[[\"'abc'\", \"1\", \"2\"]]"),
        ("UPDATE t SET (n, c) = (VALUES (11, 12))", NO_ROWS),
        (READ, "rows=[[\"'abc'\", \"11\", \"12\"]]"),
        // Widths: 2 targets and 3 values, 3 and 2, a `*` over a join of
        // four, a one-column compound, VALUES; a missing column first.
        (
            "UPDATE t SET (n, c) = (SELECT 1, 2, 3)",
            "error: 2 columns assigned 3 values",
        ),
        (
            "UPDATE t SET (a, n, c) = (SELECT 1, 2)",
            "error: 3 columns assigned 2 values",
        ),
        (
            "UPDATE t SET (n, c) = (SELECT p FROM s)",
            "error: 2 columns assigned 1 values",
        ),
        (
            "UPDATE t SET (n, c) = (SELECT * FROM s, e)",
            "error: 2 columns assigned 4 values",
        ),
        (
            "UPDATE t SET (n, c) = (SELECT 1 UNION ALL SELECT 2)",
            "error: 2 columns assigned 1 values",
        ),
        (
            "UPDATE t SET (n, c) = (VALUES (1, 2, 3))",
            "error: 2 columns assigned 3 values",
        ),
        (
            "UPDATE t SET (n, c) = (SELECT nope, 1 FROM s)",
            "error: no such column: nope",
        ),
        (
            "UPDATE t SET (n, c) = (SELECT nope, 1, 2 FROM s)",
            "error: no such column: nope",
        ),
        (READ, "rows=[[\"'abc'\", \"11\", \"12\"]]"),
        // No row (NULLs), a NULL value, a correlated source, a FROM-less
        // source reading the row (a swap), aggregates.
        ("UPDATE t SET (n, c) = (SELECT p, q FROM e)", NO_ROWS),
        (READ, "rows=[[\"'abc'\", \"NULL\", \"NULL\"]]"),
        ("UPDATE t SET (n, c) = (SELECT NULL, 3)", NO_ROWS),
        (READ, "rows=[[\"'abc'\", \"NULL\", \"3\"]]"),
        ("UPDATE t SET (n, c) = (SELECT 20, 30)", NO_ROWS),
        (
            "UPDATE t SET (n, c) = (SELECT p + t.n, q FROM s ORDER BY p LIMIT 1)",
            NO_ROWS,
        ),
        (READ, "rows=[[\"'abc'\", \"25\", \"6\"]]"),
        ("UPDATE t SET (n, c) = (SELECT c, n)", NO_ROWS),
        (READ, "rows=[[\"'abc'\", \"6\", \"25\"]]"),
        (
            "UPDATE t SET (n, c) = (SELECT max(p), min(q) FROM s)",
            NO_ROWS,
        ),
        (READ, "rows=[[\"'abc'\", \"7\", \"6\"]]"),
        ("UPDATE t SET (n, c) = (SELECT count(*), 1)", NO_ROWS),
        (READ, "rows=[[\"'abc'\", \"1\", \"1\"]]"),
        // A correlated source's names resolve: in the source, against the
        // target, nowhere.
        (
            "UPDATE t SET (n, c) = (SELECT nope, q FROM s WHERE s.p > t.n)",
            "error: no such column: nope",
        ),
        (
            "UPDATE t SET (n, c) = (SELECT p, q FROM s WHERE s.p > t.nope)",
            "error: no such column: t.nope",
        ),
        (READ, "rows=[[\"'abc'\", \"1\", \"1\"]]"),
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
                    dir.path().join("bd_4l4y3.db").to_str().unwrap().to_owned()
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
            }
        }
    });
}

#[test]
fn upsert_row_value_set_from_a_subquery_matches_stock() {
    run_script(&UPSERT);
}

#[test]
fn update_row_value_set_from_a_star_compound_or_values_source_matches_stock() {
    run_script(&UPDATE);
}

/// A statement prepared before its source table changes assigns the
/// source's rows at execution, as stock's does: the subquery is not read
/// once at prepare.
#[test]
fn a_prepared_row_value_set_reads_its_source_when_it_runs() {
    const STATEMENTS: &[&str] = &[
        "UPDATE t SET (n, c) = (SELECT * FROM s ORDER BY p DESC)",
        "INSERT INTO t VALUES ('abc', 0, 0) ON CONFLICT(a) DO UPDATE SET (n, c) = (VALUES ((SELECT max(p) FROM s), 0))",
        "UPDATE t SET n = (SELECT max(p) FROM s)",
    ];
    for sql in STATEMENTS {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for setup in SETUP {
            stock.execute_batch(setup).unwrap();
        }
        let mut statement = stock.prepare(sql).unwrap();
        stock
            .execute_batch("INSERT INTO s VALUES (70, 80)")
            .unwrap();
        statement.execute([]).unwrap();
        drop(statement);
        let stock_read = stock_outcome(&stock, READ);
        assert_ne!(
            stock_read, "rows=[[\"'abc'\", \"1\", \"2\"]]",
            "stock on `{sql}`"
        );
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(":memory:").await.unwrap();
            for setup in SETUP {
                conn.execute(setup).await.unwrap();
            }
            let statement = conn.prepare(sql).await.unwrap();
            conn.execute("INSERT INTO s VALUES (70, 80)").await.unwrap();
            statement.execute().await.unwrap();
            let read = render_rows(&conn.query(READ).await.unwrap());
            assert_eq!(read, stock_read, "`{sql}` prepared, then s changed");
        });
    }
}
