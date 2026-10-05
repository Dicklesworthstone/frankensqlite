#![recursion_limit = "512"]

//! bd-az48n: ALTER TABLE on an attached schema's table (ADD COLUMN, RENAME
//! COLUMN, DROP COLUMN, RENAME TO) runs on the attached connection, as every
//! other attached write already did, instead of failing with "not yet
//! supported", and inside an explicit transaction it commits and rolls back
//! with it; and a TEMP table's `rootpage` in `sqlite_temp_master` is a
//! positive integer. Compared with stock SQLite (rusqlite).
//!
//! Not covered: schema-qualified table-valued pragmas
//! (`temp.pragma_table_info(...)`) are still a syntax error; and after RENAME
//! TO, stock stores every reference to the table as `"new"` (always quoted)
//! where FrankenSQLite quotes it only when needed, in the MAIN schema too, so
//! the stored text is compared only before the rename.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

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

async fn frank(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

fn stock(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(
            rusqlite::Error::SqliteFailure(_, Some(m)) | rusqlite::Error::SqlInputError { msg: m, .. },
        ) => return vec![vec![format!("<ERR {m}>")]],
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    match st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
    {
        Ok(rows) => rows,
        Err(rusqlite::Error::SqliteFailure(_, Some(m))) => vec![vec![format!("<ERR {m}>")]],
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

/// Phases of statements run on both engines; each outcome (ok / error
/// message) is compared, then every query of the phase.
const PHASES: &[(&[&str], &[&str])] = &[
    (
        &[
            "CREATE TEMP TABLE tt(x INTEGER PRIMARY KEY, y TEXT DEFAULT 'q')",
            "CREATE TABLE m(a)",
            "ATTACH ':memory:' AS aux",
            "CREATE TABLE aux.t(a, b, d)",
            "CREATE INDEX aux.ix ON t(b)",
            "INSERT INTO aux.t VALUES (1, 2, 3)",
            "BEGIN",
            "ALTER TABLE aux.t ADD COLUMN z DEFAULT 9",
            "SELECT z FROM aux.t",
            "ROLLBACK",
            "BEGIN",
            "ALTER TABLE aux.t ADD COLUMN c DEFAULT 7",
            "COMMIT",
            "ALTER TABLE aux.t RENAME COLUMN b TO bb",
            "ALTER TABLE aux.t DROP COLUMN d",
        ],
        &[
            "SELECT type, name, tbl_name, rootpage > 0, typeof(rootpage) FROM sqlite_temp_master ORDER BY name",
            "SELECT type, name, tbl_name, rootpage > 0, typeof(rootpage) FROM temp.sqlite_master ORDER BY name",
            "SELECT * FROM aux.t",
            "SELECT z FROM aux.t",
            "PRAGMA aux.table_info(t)",
            "SELECT type, name, tbl_name, sql FROM aux.sqlite_master ORDER BY name",
            "SELECT name FROM main.sqlite_master ORDER BY name",
        ],
    ),
    (
        &[
            "ALTER TABLE aux.t RENAME TO t2",
            "INSERT INTO aux.t2 VALUES (4, 5, 6)",
        ],
        &[
            "SELECT * FROM aux.t2 ORDER BY a",
            "SELECT * FROM aux.t",
            "PRAGMA aux.table_info(t2)",
            "SELECT type, name, tbl_name FROM aux.sqlite_master ORDER BY name",
            "SELECT bb FROM aux.t2 INDEXED BY ix WHERE bb = 5",
        ],
    ),
];

/// Runs every phase against both engines. With `file_backed`, each engine
/// attaches its own database file, and stock then opens FrankenSQLite's
/// altered file: `integrity_check` must pass and its rows must match.
async fn run_phases(file_backed: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("frank.db");
    let (frank_aux, stock_aux) = if file_backed {
        (
            dir.path().join("frank_aux.db").to_str().expect("utf-8 path").to_owned(),
            dir.path().join("stock_aux.db").to_str().expect("utf-8 path").to_owned(),
        )
    } else {
        (":memory:".to_owned(), ":memory:".to_owned())
    };
    let f = Connection::open(path.to_str().expect("utf-8 path"))
        .await
        .expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    let mut failures = Vec::new();
    for (steps, queries) in PHASES {
        for sql in *steps {
            let (frank_sql, stock_sql) = if sql.starts_with("ATTACH") {
                (
                    format!("ATTACH '{frank_aux}' AS aux"),
                    format!("ATTACH '{stock_aux}' AS aux"),
                )
            } else {
                ((*sql).to_owned(), (*sql).to_owned())
            };
            let fo = match f.execute(&frank_sql).await {
                Ok(_) => "ok".to_owned(),
                Err(e) => format!("error: {e}"),
            };
            let so = match r.execute_batch(&stock_sql) {
                Ok(()) => "ok".to_owned(),
                Err(rusqlite::Error::SqliteFailure(_, Some(m))) => format!("error: {m}"),
                Err(e) => format!("error: {e}"),
            };
            if fo != so {
                failures.push(format!("`{sql}`: frank {fo:?} vs stock {so:?}"));
            }
        }
        for sql in *queries {
            let (fv, sv) = (frank(&f, sql).await, stock(&r, sql));
            if fv != sv {
                failures.push(format!("`{sql}`:\n  frank {fv:?}\n  stock {sv:?}"));
            }
        }
    }
    f.close().await.expect("close");
    if file_backed {
        let reopened = rusqlite::Connection::open(&frank_aux).expect("stock opens frank aux");
        for sql in ["PRAGMA integrity_check", "SELECT * FROM t2 ORDER BY a"] {
            let (fv, sv) = (stock(&reopened, sql), stock(&r, &sql.replace("t2", "aux.t2")));
            let sv = if sql.starts_with("PRAGMA") {
                vec![vec!["'ok'".to_owned()]]
            } else {
                sv
            };
            if fv != sv {
                failures.push(format!(
                    "stock reading frank's aux file, `{sql}`:\n  got {fv:?}\n  want {sv:?}"
                ));
            }
        }
    }
    failures
}

#[test]
fn temp_and_attached_schemas_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            for failure in run_phases(file_backed).await {
                failures.push(format!("[file_backed={file_backed}] {failure}"));
            }
        }
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
