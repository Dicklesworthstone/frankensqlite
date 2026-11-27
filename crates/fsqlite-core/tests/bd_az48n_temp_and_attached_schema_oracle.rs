#![recursion_limit = "512"]

//! bd-az48n: ALTER TABLE on an attached schema's table (ADD COLUMN, RENAME
//! COLUMN, DROP COLUMN, RENAME TO) runs on the attached connection, as every
//! other attached write already did, instead of failing with "not yet
//! supported", and inside an explicit transaction it commits and rolls back
//! with it; a TEMP table's `rootpage` in `sqlite_temp_master` is a positive
//! integer; and schema-qualified table-valued functions / eponymous pragmas
//! (`temp.pragma_table_info(...)`, `aux.json_each(...)`, `temp.pragma_database_list`)
//! follow SQLite's `sqlite3LocateTable` catalog lookup and fallback semantics.
//! Compared with stock SQLite (rusqlite).
//!
//! Not covered: after RENAME TO, stock stores every reference to the table as
//! `"new"` (always quoted) where FrankenSQLite quotes it only when needed, in
//! the MAIN schema too (tracked separately under `bd-w4pr1.29`), so the stored
//! text is compared only before the rename.

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

#[test]
fn schema_qualified_tvf_and_attached_pragma_table_info_match_stock() {
    let setup = [
        "CREATE TABLE m(ma INT, mb TEXT)",
        "CREATE TEMP TABLE tt(tx INTEGER PRIMARY KEY, ty TEXT)",
        "CREATE INDEX tix ON tt(ty)",
        "CREATE TEMP VIEW tv AS SELECT tx FROM tt",
        "CREATE TEMP TRIGGER ttr AFTER INSERT ON tt BEGIN SELECT 1; END",
        "ATTACH ':memory:' AS aux",
        "CREATE TABLE aux.c(ax INT, ay TEXT)",
        "CREATE INDEX aux.cix ON c(ax)",
        "CREATE VIEW aux.cv AS SELECT ax FROM c",
        "CREATE TABLE main.json_each(z INT)",
        "INSERT INTO main.json_each VALUES (99)",
        "CREATE TABLE main.pragma_database_list(z INT)",
        "INSERT INTO main.pragma_database_list VALUES (77)",
    ];
    let queries = [
        // Attached pragma_table_info / table_xinfo / index_list / index_info (qualified & unqualified).
        "SELECT cid, name, type FROM pragma_table_info('c', 'aux') ORDER BY cid",
        "SELECT cid, name, type FROM pragma_table_info('c') ORDER BY cid",
        "SELECT cid, name, type FROM pragma_table_info('c', NULL) ORDER BY cid",
        "SELECT cid, name, type FROM pragma_table_info('c', 'main') ORDER BY cid",
        "SELECT cid, name, type FROM pragma_table_info('c', 'temp') ORDER BY cid",
        "SELECT cid, name, type FROM pragma_table_info('tt', 'temp') ORDER BY cid",
        "SELECT cid, name, type FROM pragma_table_info('m', 'main') ORDER BY cid",
        "SELECT cid, name, type FROM pragma_table_info('c', 'nosuch') ORDER BY cid",
        "SELECT cid, name, type FROM pragma_table_info('cv') ORDER BY cid",
        "SELECT cid, name, type FROM pragma_table_xinfo('c') ORDER BY cid",
        "SELECT name FROM pragma_index_list('c') ORDER BY name",
        "SELECT seqno, cid, name FROM pragma_index_info('cix') ORDER BY seqno",
        "PRAGMA table_info(c)",
        "PRAGMA index_list(c)",
        "PRAGMA index_info(cix)",
        // Schema-qualified table-valued pragmas and json_each (matching SQLite's sqlite3LocateTable).
        "SELECT cid, name, type FROM temp.pragma_table_info('c', 'aux') ORDER BY cid",
        "SELECT cid, name, type FROM main.pragma_table_info('c', 'aux') ORDER BY cid",
        "SELECT cid, name, type FROM aux.pragma_table_info('c') ORDER BY cid",
        "SELECT cid, name, type FROM temp.pragma_table_info('tt') ORDER BY cid",
        "SELECT cid, name, type FROM temp.pragma_table_info('m') ORDER BY cid",
        "SELECT cid, name, type FROM nosuch.pragma_table_info('m') ORDER BY cid",
        "SELECT value FROM temp.json_each('[1, 2]') ORDER BY value",
        "SELECT value FROM aux.json_each('[3]')",
        "SELECT value FROM nosuch.json_each('[4]')",
        // Shadowing: real table in main blocks main.tvf(...), while temp.tvf(...) still hits eponymous.
        "SELECT * FROM main.json_each('[1]')",
        "SELECT * FROM json_each('[1]')",
        "SELECT z FROM main.json_each",
        "SELECT z FROM main.pragma_database_list",
        "SELECT count(*) FROM temp.pragma_database_list",
        "SELECT count(*) FROM aux.pragma_database_list",
        "SELECT count(*) FROM nosuch.pragma_database_list",
        "SELECT count(*) FROM temp.pragma_database_list()",
        "SELECT * FROM main.pragma_database_list()",
        // Non-vtab table called as TVF, and unknown schema-qualified TVF.
        "SELECT * FROM main.m(1)",
        "SELECT * FROM main.sqlite_master(1)",
        "SELECT * FROM temp.sqlite_master(1)",
        "SELECT * FROM aux.sqlite_master(1)",
        "SELECT * FROM nosuch.sqlite_master(1)",
        "SELECT * FROM nosuch.nosuch_tvf(1)",
        // Subquery and CTE interaction with schema-qualified TVFs / parenless pragmas.
        "SELECT (SELECT count(*) FROM temp.pragma_table_info('c', 'aux'))",
        "SELECT (SELECT sum(value) FROM temp.json_each('[10, 20, 30]'))",
        "WITH pragma_database_list(x) AS (SELECT 1) SELECT count(*) FROM temp.pragma_database_list",
        // TEMP catalog rootpage checks across table, index, view, trigger.
        "SELECT type, name, rootpage > 0, typeof(rootpage) FROM sqlite_temp_master ORDER BY type, name",
        "SELECT type, name, rootpage > 0, typeof(rootpage) FROM temp.sqlite_master ORDER BY type, name",
    ];

    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for sql in setup {
        r.execute_batch(sql).expect(sql);
    }

    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().expect("temp dir");
            let path = if file_backed {
                dir.path()
                    .join("bd_az48n_tvf.db")
                    .to_str()
                    .expect("utf-8")
                    .to_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&path).await.expect("frank open");
            for sql in setup {
                f.execute(sql).await.expect(sql);
            }
            for sql in queries {
                let (fv, sv) = (frank(&f, sql).await, stock(&r, sql));
                if fv != sv {
                    failures.push(format!(
                        "[file_backed={file_backed}] `{sql}`:\n  frank {fv:?}\n  stock {sv:?}"
                    ));
                }
            }
            f.close().await.expect("close");
        }
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
