//! bd-9gfen: the legacy TEMP catalog aliases `sqlite_temp_master` and
//! `sqlite_temp_schema` inside a nested scope. Stock SQLite 3.54.1 reads them
//! as `temp.sqlite_master` everywhere: a derived table, a CTE, an IN, EXISTS
//! or scalar subquery, a join. fsqlite answered them at the top level, but in
//! a nested scope it bound no columns for them, so every named column failed
//! ("no such column: name").
//!
//! Every statement runs on bundled SQLite 3.54.1 and on fsqlite, on
//! `:memory:` and on a file, directly and prepared, with a TEMP table, index,
//! view and trigger present. Each outcome (its rows, or its error text) must
//! equal stock's on the same kind of database, and is pinned to stock's.
//!
//! Negatives a naive fix fails: `main.sqlite_temp_master` is still no table;
//! a column the derived table does not project (`nope`, `x.tbl_name`) is
//! still no column; `temp.sqlite_temp_master` still reads the TEMP catalog;
//! `sqlite_master` in the same nested forms still lists only main's objects.

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

async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let result = if prepared {
        match conn.prepare(sql).await {
            Ok(statement) => statement.query().await,
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

const SETUP: &[&str] = &[
    "CREATE TABLE m(a)",
    "INSERT INTO m VALUES (1)",
    "CREATE TEMP TABLE tt(x)",
    "CREATE INDEX temp.tti ON tt(x)",
    "CREATE TEMP VIEW tv AS SELECT x FROM tt",
    "CREATE TEMP TRIGGER ttr AFTER INSERT ON tt BEGIN SELECT 1; END",
    "INSERT INTO tt VALUES (5)",
];

const ALL_TEMP: &str = r#"rows=[["'tt'"], ["'tti'"], ["'ttr'"], ["'tv'"]]"#;

/// Each statement and stock's outcome for it.
const STATEMENTS: &[(&str, &str)] = &[
    (
        "SELECT name FROM sqlite_temp_master ORDER BY name",
        ALL_TEMP,
    ),
    // The bead's nested forms.
    (
        "SELECT name FROM (SELECT name FROM sqlite_temp_master) ORDER BY name",
        ALL_TEMP,
    ),
    (
        "SELECT name FROM (SELECT name FROM sqlite_temp_schema) ORDER BY name",
        ALL_TEMP,
    ),
    (
        "SELECT x.name FROM (SELECT name FROM sqlite_temp_master) AS x ORDER BY 1",
        ALL_TEMP,
    ),
    (
        "SELECT name FROM (SELECT type, name FROM sqlite_temp_master) ORDER BY name",
        ALL_TEMP,
    ),
    (
        "WITH c AS (SELECT name FROM sqlite_temp_master) SELECT name FROM c ORDER BY name",
        ALL_TEMP,
    ),
    (
        "SELECT name FROM sqlite_temp_master WHERE name IN (SELECT name FROM sqlite_temp_master) ORDER BY name",
        ALL_TEMP,
    ),
    (
        "SELECT name FROM (SELECT name FROM temp.sqlite_master) ORDER BY name",
        ALL_TEMP,
    ),
    (
        "SELECT name FROM (SELECT name FROM temp.sqlite_schema) ORDER BY name",
        ALL_TEMP,
    ),
    (
        "SELECT count(*) FROM (SELECT * FROM sqlite_temp_master)",
        r#"rows=[["4"]]"#,
    ),
    (
        "SELECT name FROM (SELECT name FROM sqlite_master) ORDER BY name",
        r#"rows=[["'m'"]]"#,
    ),
    // More nested shapes.
    (
        "SELECT type, name, tbl_name FROM (SELECT * FROM sqlite_temp_master) ORDER BY name",
        r#"rows=[["'table'", "'tt'", "'tt'"], ["'index'", "'tti'", "'tt'"], ["'trigger'", "'ttr'", "'tt'"], ["'view'", "'tv'", "'tv'"]]"#,
    ),
    (
        "SELECT n FROM (SELECT name AS n, tbl_name FROM sqlite_temp_schema WHERE type = 'index')",
        r#"rows=[["'tti'"]]"#,
    ),
    (
        "SELECT a, name FROM m, (SELECT name FROM sqlite_temp_master WHERE type = 'view')",
        r#"rows=[["1", "'tv'"]]"#,
    ),
    (
        "SELECT (SELECT count(*) FROM sqlite_temp_master WHERE tbl_name = 'tt')",
        r#"rows=[["3"]]"#,
    ),
    (
        "SELECT EXISTS (SELECT 1 FROM sqlite_temp_schema WHERE name = 'tv')",
        r#"rows=[["1"]]"#,
    ),
    (
        "WITH c(k) AS (SELECT name FROM sqlite_temp_schema) SELECT k FROM c ORDER BY k",
        ALL_TEMP,
    ),
    (
        "SELECT name FROM sqlite_master WHERE name NOT IN (SELECT name FROM sqlite_temp_master)",
        r#"rows=[["'m'"]]"#,
    ),
    (
        "SELECT name FROM sqlite_temp_master WHERE type IN (SELECT type FROM sqlite_temp_schema WHERE name = 'ttr')",
        r#"rows=[["'ttr'"]]"#,
    ),
    (
        "SELECT name FROM (SELECT name FROM temp.sqlite_temp_master) ORDER BY name",
        ALL_TEMP,
    ),
    // Negatives.
    (
        "SELECT nope FROM (SELECT name FROM sqlite_temp_master)",
        "error: no such column: nope",
    ),
    (
        "SELECT x.tbl_name FROM (SELECT name FROM sqlite_temp_master) AS x",
        "error: no such column: x.tbl_name",
    ),
    (
        "SELECT name FROM (SELECT name FROM main.sqlite_temp_master)",
        "error: no such table: main.sqlite_temp_master",
    ),
    (
        "SELECT name FROM (SELECT nope FROM sqlite_temp_master)",
        "error: no such column: nope",
    ),
];

#[test]
fn temp_catalog_aliases_in_nested_scopes_match_stock() {
    let dir = tempfile::tempdir().unwrap();
    // Stock's outcomes on `:memory:` and on a file, each pinned.
    let mut stock_outcomes: Vec<Vec<String>> = Vec::new();
    for file_backed in [false, true] {
        let stock = if file_backed {
            rusqlite::Connection::open(dir.path().join("stock.db")).unwrap()
        } else {
            rusqlite::Connection::open_in_memory().unwrap()
        };
        for sql in SETUP {
            stock.execute_batch(sql).unwrap();
        }
        let expected: Vec<String> = STATEMENTS
            .iter()
            .map(|(sql, _)| stock_outcome(&stock, sql))
            .collect();
        // Not vacuous: stock gives each pinned outcome.
        for ((sql, pinned), stock_gave) in STATEMENTS.iter().zip(&expected) {
            assert_eq!(
                stock_gave, pinned,
                "stock (file_backed={file_backed}) on `{sql}`"
            );
        }
        stock_outcomes.push(expected);
    }
    asupersync::test_utils::run_test(|| async {
        let mut mismatches = Vec::new();
        for (file_backed, expected) in [false, true].into_iter().zip(&stock_outcomes) {
            for prepared in [false, true] {
                let path = if file_backed {
                    dir.path()
                        .join(format!("fsqlite-{prepared}.db"))
                        .to_str()
                        .unwrap()
                        .to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                for ((sql, _), expected) in STATEMENTS.iter().zip(expected) {
                    let got = fsqlite_outcome(&conn, sql, prepared).await;
                    if &got != expected {
                        mismatches.push(format!(
                            "file_backed={file_backed} prepared={prepared} `{sql}`:\n    \
                             got  {got}\n    want {expected}"
                        ));
                    }
                }
                conn.close().await.unwrap();
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} outcome(s) differ from stock:\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}
