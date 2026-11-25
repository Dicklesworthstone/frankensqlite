//! bd-5xi8y: TEMP DDL `DROP TRIGGER` / `DROP INDEX` / `DROP VIEW` must mark the
//! drop as connection-local so `execute_drop` calls `note_temp_schema_change()`
//! instead of `increment_schema_cookie()` on the read-only main transaction
//! opened by `statement_is_temp_only_ddl()`, and unqualified drops must shadow
//! MAIN with TEMP matching stock SQLite 3.54.1.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use tempfile::tempdir;

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
        let mut stmt = conn.prepare(sql)?;
        let cols = stmt.column_count();
        let mut rows = Vec::new();
        let mut cursor = stmt.query([])?;
        while let Some(row) = cursor.next()? {
            rows.push(
                (0..cols)
                    .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect(),
            );
        }
        Ok(rows)
    })();
    match result {
        Ok(rows) => format!("rows={rows:?}"),
        Err(
            rusqlite::Error::SqliteFailure(_, Some(msg)) | rusqlite::Error::SqlInputError { msg, .. },
        ) => format!("error: {msg}"),
        Err(err) => format!("error: {err}"),
    }
}

async fn fsqlite_outcome(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) => {
            let rendered: Vec<Vec<String>> = rows
                .iter()
                .map(|r| r.values().iter().map(render_fsqlite).collect())
                .collect();
            format!("rows={rendered:?}")
        }
        Err(err) => format!("error: {err}"),
    }
}

struct Scenario {
    name: &'static str,
    steps: &'static [&'static str],
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "temp trigger after non-compiling body drops cleanly",
        steps: &[
            "CREATE TABLE t(a, b)",
            "CREATE TABLE lg(m)",
            "CREATE TEMP TRIGGER tr AFTER INSERT ON t WHEN 0 BEGIN INSERT INTO lg VALUES (NEW.zz); END",
            "INSERT INTO t VALUES (1, 1)",
            "DROP TRIGGER tr",
            "INSERT INTO t VALUES (2, 3)",
            "SELECT a, b FROM t ORDER BY a",
            "CREATE TEMP TRIGGER tr2 AFTER INSERT ON t BEGIN INSERT INTO lg VALUES (NEW.a); END",
            "DROP TRIGGER temp.tr2",
            "INSERT INTO t VALUES (4, 5)",
            "SELECT count(*) FROM lg",
        ],
    },
    Scenario {
        name: "unqualified DROP TRIGGER shadows main with temp",
        steps: &[
            "CREATE TABLE t(a)",
            "CREATE TABLE lg(src, v)",
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO lg VALUES ('main', NEW.a); END",
            "CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO lg VALUES ('temp', NEW.a); END",
            "INSERT INTO t VALUES (1)",
            "DROP TRIGGER tr",
            "INSERT INTO t VALUES (2)",
            "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'tr'",
            "SELECT count(*) FROM sqlite_temp_master WHERE type = 'trigger' AND name = 'tr'",
            "DROP TRIGGER tr",
            "INSERT INTO t VALUES (3)",
            "SELECT src, v FROM lg ORDER BY v, src",
        ],
    },
    Scenario {
        name: "explicit main DROP TRIGGER preserves temp trigger",
        steps: &[
            "CREATE TABLE t(a)",
            "CREATE TABLE lg(src, v)",
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO lg VALUES ('main', NEW.a); END",
            "CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO lg VALUES ('temp', NEW.a); END",
            "DROP TRIGGER main.tr",
            "INSERT INTO t VALUES (10)",
            "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'tr'",
            "SELECT count(*) FROM sqlite_temp_master WHERE type = 'trigger' AND name = 'tr'",
            "SELECT src, v FROM lg ORDER BY v, src",
            "DROP TRIGGER temp.tr",
        ],
    },
    Scenario {
        name: "temp index drop and main/temp index shadowing",
        steps: &[
            "CREATE TABLE t(a, b)",
            "CREATE INDEX ix ON t(a)",
            "CREATE TEMP TABLE tt(a, b)",
            "CREATE INDEX temp.ix ON tt(a)",
            "DROP INDEX ix",
            "SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = 'ix'",
            "SELECT count(*) FROM sqlite_temp_master WHERE type = 'index' AND name = 'ix'",
            "CREATE INDEX temp.ix ON tt(b)",
            "DROP INDEX main.ix",
            "SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = 'ix'",
            "SELECT count(*) FROM sqlite_temp_master WHERE type = 'index' AND name = 'ix'",
            "DROP INDEX temp.ix",
            "SELECT count(*) FROM sqlite_temp_master WHERE type = 'index' AND name = 'ix'",
        ],
    },
    Scenario {
        name: "dropping shadowed temp view preserves main view and its INSTEAD OF trigger",
        steps: &[
            "CREATE TABLE lg(v)",
            "CREATE VIEW v AS SELECT 1 AS x",
            "CREATE TRIGGER vt INSTEAD OF INSERT ON v BEGIN INSERT INTO lg VALUES (NEW.x); END",
            "CREATE TEMP VIEW v AS SELECT 2 AS x",
            "DROP VIEW v",
            "SELECT x FROM v",
            "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'vt'",
            "INSERT INTO v VALUES (42)",
            "SELECT v FROM lg",
        ],
    },
    Scenario {
        name: "missing qualified temp drops match stock diagnostics",
        steps: &[
            "DROP TRIGGER IF EXISTS temp.nope",
            "DROP INDEX IF EXISTS temp.nope",
            "DROP TRIGGER temp.nope",
            "DROP INDEX temp.nope",
        ],
    },
];

#[test]
fn temp_drop_trigger_index_view_matches_stock_oracle() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for scenario in SCENARIOS {
                let _dir = if file_backed {
                    Some(tempdir().expect("tempdir"))
                } else {
                    None
                };
                let (stock, fsql) = if let Some(dir) = &_dir {
                    let stock_path = dir.path().join("stock.db");
                    let fsql_path = dir.path().join("fsql.db");
                    (
                        rusqlite::Connection::open(&stock_path).expect("open stock file"),
                        Connection::open(fsql_path.to_str().expect("utf8 path"))
                            .await
                            .expect("open fsql file"),
                    )
                } else {
                    (
                        rusqlite::Connection::open_in_memory().expect("open stock mem"),
                        Connection::open(":memory:").await.expect("open fsql mem"),
                    )
                };

                for &sql in scenario.steps {
                    let expected = stock_outcome(&stock, sql);
                    let actual = fsqlite_outcome(&fsql, sql).await;
                    assert_eq!(
                        actual, expected,
                        "scenario='{}' file_backed={file_backed} sql=`{sql}`",
                        scenario.name
                    );
                }
                fsql.close().await.expect("close fsql");
            }
        }
    });
}
