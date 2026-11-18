#![recursion_limit = "512"]
//! bd-d8s37: an ON CONFLICT target that names an expression index's key
//! (`ON CONFLICT (lower(tag))`) is that index's conflict target, as in stock.
//! fsqlite refused every expression target with "ON CONFLICT clause does not
//! match any PRIMARY KEY or UNIQUE constraint", so an UPSERT through an
//! expression index never ran, and its DO UPDATE could not be FK-checked.
//!
//! Every statement's outcome (`changes()` and RETURNING rows, or its error
//! text) and the final tables match bundled SQLite 3.54.1, on `:memory:` and
//! on a file, executed directly and prepared.

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

fn stock_error(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message))
        | rusqlite::Error::SqlInputError { msg: message, .. } => message.clone(),
        other => other.to_string(),
    }
}

fn stock_outcome(conn: &rusqlite::Connection, sql: &str) -> String {
    let result = (|| -> rusqlite::Result<String> {
        let mut statement = conn.prepare(sql)?;
        let columns = statement.column_count();
        let mut rows: Vec<Vec<String>> = Vec::new();
        if sql.contains("RETURNING") {
            let mut cursor = statement.query([])?;
            while let Some(row) = cursor.next()? {
                rows.push(
                    (0..columns)
                        .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                        .collect(),
                );
            }
        } else {
            statement.execute([])?;
        }
        drop(statement);
        rows.sort();
        let changes: i64 = conn.query_row("SELECT changes()", [], |row| row.get(0))?;
        Ok(format!("changes={changes} rows={rows:?}"))
    })();
    result.unwrap_or_else(|error| format!("error: {}", stock_error(&error)))
}

async fn fsqlite_outcome(conn: &Connection, sql: &str, prepared: bool) -> String {
    let returning = sql.contains("RETURNING");
    let result: Result<Vec<Vec<String>>, String> = async {
        let rows = match (prepared, returning) {
            (true, true) => {
                let statement = conn.prepare(sql).await.map_err(|e| e.to_string())?;
                statement.query().await.map_err(|e| e.to_string())?
            }
            (true, false) => {
                let statement = conn.prepare(sql).await.map_err(|e| e.to_string())?;
                statement.execute().await.map_err(|e| e.to_string())?;
                Vec::new()
            }
            (false, true) => conn.query(sql).await.map_err(|e| e.to_string())?,
            (false, false) => {
                conn.execute(sql).await.map_err(|e| e.to_string())?;
                Vec::new()
            }
        };
        Ok(rows
            .iter()
            .map(|row| row.values().iter().map(render_fsqlite).collect())
            .collect())
    }
    .await;
    match result {
        Ok(mut rows) => {
            rows.sort();
            let changes = conn
                .query("SELECT changes()")
                .await
                .expect("changes()")
                .first()
                .map(|row| render_fsqlite(&row.values()[0]))
                .unwrap_or_default();
            format!("changes={changes} rows={rows:?}")
        }
        Err(error) => format!("error: {error}"),
    }
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn.prepare(sql).expect("stock verify prepares");
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock verify runs")
}

async fn fsqlite_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite verify `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect()
}

const SETUP: &[&str] = &[
    "PRAGMA foreign_keys=ON",
    "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
    "INSERT INTO parent VALUES (1), (2)",
    "CREATE TABLE c(id INTEGER PRIMARY KEY, tag TEXT, k INTEGER, \
       parent_id INTEGER REFERENCES parent(id))",
    "CREATE UNIQUE INDEX c_lower ON c(lower(tag))",
    "CREATE UNIQUE INDEX c_mix ON c(k, abs(id - 100))",
    "INSERT INTO c VALUES (10, 'Ab', 1, 1)",
    "CREATE TABLE p(id INTEGER PRIMARY KEY, tag TEXT, live INTEGER)",
    "CREATE UNIQUE INDEX p_live ON p(lower(tag)) WHERE live = 1",
    "INSERT INTO p VALUES (1, 'A', 1), (2, 'B', 0)",
    "CREATE TABLE w(id INTEGER PRIMARY KEY, tag TEXT, n INTEGER) WITHOUT ROWID",
    "CREATE UNIQUE INDEX w_lower ON w(lower(tag))",
    "INSERT INTO w VALUES (1, 'Xy', 0)",
];

const STEPS: &[&str] = &[
    "INSERT INTO c VALUES (20, 'AB', 2, 1) ON CONFLICT (lower(tag)) DO UPDATE SET k = k + 10",
    "INSERT INTO c VALUES (21, 'aB', 3, 1) ON CONFLICT (lower(tag)) DO NOTHING",
    // The DO UPDATE result is FK-checked: 99 has no parent.
    "INSERT INTO c VALUES (22, 'ab', 4, 1) ON CONFLICT (lower(tag)) DO UPDATE SET parent_id = 99",
    "INSERT INTO c VALUES (23, 'ab', 5, 1) ON CONFLICT (lower(tag)) \
       DO UPDATE SET parent_id = excluded.k - 3",
    "INSERT INTO c VALUES (24, 'zz', 6, 1) ON CONFLICT (lower(tag)) DO UPDATE SET k = 0",
    // Negatives: no index has upper(tag), and the index's collation is BINARY.
    "INSERT INTO c VALUES (25, 'ab', 7, 1) ON CONFLICT (upper(tag)) DO NOTHING",
    "INSERT INTO c VALUES (25, 'ab', 7, 1) ON CONFLICT (lower(tag) COLLATE NOCASE) DO NOTHING",
    "INSERT INTO c VALUES (25, 'ab', 7, 1) ON CONFLICT (lower(tag) COLLATE BINARY) DO NOTHING",
    // A column and an expression, in either order.
    "INSERT INTO c VALUES (110, 'q', 11, 1) ON CONFLICT (abs(id - 100), k) \
       DO UPDATE SET tag = 'mix-hit'",
    "INSERT INTO c VALUES (90, 'q2', 11, 1) ON CONFLICT (k, abs(id - 100)) \
       DO UPDATE SET tag = 'mix-hit2' RETURNING id, tag",
    // A partial expression index is the target only with its WHERE.
    "INSERT INTO p VALUES (3, 'a', 1) ON CONFLICT (lower(tag)) WHERE live = 1 \
       DO UPDATE SET tag = 'partial-hit'",
    "INSERT INTO p VALUES (4, 'b', 1) ON CONFLICT (lower(tag)) WHERE live = 1 \
       DO UPDATE SET tag = 'partial-miss'",
    "INSERT INTO p VALUES (5, 'a', 1) ON CONFLICT (lower(tag)) DO NOTHING",
    "INSERT INTO w VALUES (2, 'xY', 5) ON CONFLICT (lower(tag)) DO UPDATE SET n = excluded.n + 1",
];

const VERIFY: &[&str] = &[
    "SELECT * FROM c ORDER BY id",
    "SELECT * FROM p ORDER BY id",
    "SELECT * FROM w ORDER BY id",
];

#[test]
fn upsert_expression_targets_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in SETUP {
            stock.execute_batch(sql).unwrap();
        }
        let expected: Vec<String> = STEPS.iter().map(|sql| stock_outcome(&stock, sql)).collect();
        let tables: Vec<Vec<Vec<String>>> =
            VERIFY.iter().map(|sql| stock_rows(&stock, sql)).collect();
        // Not vacuous: stock runs the DO UPDATE through the expression index,
        // FK-checks it, and refuses the targets no index matches.
        assert_eq!(expected[0], "changes=1 rows=[]", "{expected:?}");
        assert_eq!(
            expected[2], "error: FOREIGN KEY constraint failed",
            "{expected:?}"
        );
        assert!(
            expected[5].starts_with("error: ON CONFLICT clause"),
            "{expected:?}"
        );
        assert!(
            expected[6].starts_with("error: ON CONFLICT clause"),
            "{expected:?}"
        );
        assert_eq!(expected[7], "changes=0 rows=[]", "{expected:?}");
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("upsert.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let mode = format!("file_backed={file_backed} prepared={prepared}");
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql)
                        .await
                        .unwrap_or_else(|e| panic!("{mode}: `{sql}`: {e}"));
                }
                for (sql, expected) in STEPS.iter().zip(&expected) {
                    assert_eq!(
                        &fsqlite_outcome(&conn, sql, prepared).await,
                        expected,
                        "{mode}: `{sql}`"
                    );
                }
                for (sql, expected) in VERIFY.iter().zip(&tables) {
                    assert_eq!(&fsqlite_rows(&conn, sql).await, expected, "{mode}: `{sql}`");
                }
            }
        }
    });
}
