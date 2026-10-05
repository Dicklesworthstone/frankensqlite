#![recursion_limit = "512"]

//! bd-6i9c5 (b): once a table holds rowid 9223372036854775807, SQLite's
//! `OP_NewRowid` picks a random unused rowid in `1..=2^62` for an automatic
//! rowid (an AUTOINCREMENT table fails SQLITE_FULL instead). fsqlite failed
//! "rowid overflow: maximum rowid reached" for every automatic rowid.
//!
//! The rowids are random, so each case compares invariants of the result with
//! rusqlite (row and distinct-rowid counts, range, the max row, the indexed
//! and `last_insert_rowid()` views of the new rows, integrity), for ad hoc and
//! prepared inserts, in memory and file-backed.

use fsqlite_core::connection::Connection;
use fsqlite_error::ErrorCode;
use fsqlite_types::value::SqliteValue;

const MAX: &str = "9223372036854775807";

struct Case {
    name: &'static str,
    setup: &'static [&'static str],
    /// Run once each, the same way as `insert`, before it.
    before: &'static [&'static str],
    insert: &'static str,
    /// How many times the insert runs (a prepared statement is reused).
    runs: usize,
}

const CASES: &[Case] = &[
    Case {
        name: "ipk",
        setup: &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v)",
            "CREATE INDEX t_v ON t(v)",
            "INSERT INTO t VALUES (9223372036854775807, 'max')",
        ],
        before: &[],
        insert: "INSERT INTO t(v) VALUES ('a')",
        runs: 1,
    },
    Case {
        name: "ipk_repeated",
        setup: &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v)",
            "CREATE INDEX t_v ON t(v)",
            "INSERT INTO t VALUES (9223372036854775807, 'max')",
        ],
        before: &[],
        insert: "INSERT INTO t(v) VALUES ('a')",
        runs: 20,
    },
    // No index: a prepared single-row insert takes the direct insert lane.
    Case {
        name: "ipk_unindexed_repeated",
        setup: &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v)",
            "INSERT INTO t VALUES (9223372036854775807, 'max')",
        ],
        before: &[],
        insert: "INSERT INTO t(v) VALUES ('a')",
        runs: 20,
    },
    // The insert of the max rowid itself goes through the same lane first, so
    // the lane's append hint ends at 9223372036854775807.
    Case {
        name: "explicit_max_then_automatic",
        setup: &["CREATE TABLE t(id INTEGER PRIMARY KEY, v)"],
        before: &["INSERT INTO t VALUES (9223372036854775807, 'max')"],
        insert: "INSERT INTO t(v) VALUES ('a')",
        runs: 3,
    },
    Case {
        name: "rowid_multi_values",
        setup: &[
            "CREATE TABLE t(v)",
            "INSERT INTO t(rowid, v) VALUES (9223372036854775807, 'max')",
        ],
        before: &[],
        insert: "INSERT INTO t(v) VALUES ('a'), ('a'), ('a')",
        runs: 1,
    },
    Case {
        name: "insert_select",
        setup: &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v)",
            "CREATE INDEX t_v ON t(v)",
            "INSERT INTO t VALUES (9223372036854775807, 'max')",
        ],
        before: &[],
        insert: "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 50) \
                 INSERT INTO t(v) SELECT 'a' FROM n",
        runs: 1,
    },
    Case {
        name: "temp_table",
        setup: &[
            "CREATE TEMP TABLE t(id INTEGER PRIMARY KEY, v)",
            "INSERT INTO t VALUES (9223372036854775807, 'max')",
        ],
        before: &[],
        insert: "INSERT INTO t(v) VALUES ('a')",
        runs: 3,
    },
    Case {
        name: "max_below_top_still_appends",
        setup: &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v)",
            "INSERT INTO t VALUES (9223372036854775806, 'max')",
        ],
        before: &[],
        insert: "INSERT INTO t(v) VALUES ('a')",
        runs: 1,
    },
];

/// Invariants over `t` after the inserts. `'a'` marks every inserted row.
const CHECKS: &[&str] = &[
    "SELECT count(*), count(DISTINCT rowid), min(rowid) > 0, max(rowid) FROM t",
    "SELECT count(*), sum(rowid BETWEEN 1 AND 4611686018427387904) FROM t WHERE v = 'a' \
     AND rowid <> 9223372036854775807",
    "SELECT v FROM t WHERE rowid = 9223372036854775807",
    "SELECT last_insert_rowid() IN (SELECT rowid FROM t WHERE v = 'a')",
    "PRAGMA integrity_check",
];

/// AUTOINCREMENT tables at the top of the range fail SQLITE_FULL and keep
/// their rows.
const FULL_CASES: &[(&str, &[&str])] = &[
    (
        "autoincrement_max_row",
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v)",
            "INSERT INTO t VALUES (9223372036854775807, 'max')",
        ],
    ),
    (
        "autoincrement_max_sequence",
        &[
            "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v)",
            "INSERT INTO t VALUES (9223372036854775807, 'max')",
            "DELETE FROM t",
        ],
    ),
];

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

async fn rows_f(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
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

async fn open_pair(target: &str, setup: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(target).await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        f.execute(sql)
            .await
            .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"));
        r.execute(sql, []).unwrap();
    }
    (f, r)
}

async fn run_insert(
    f: &Connection,
    sql: &str,
    runs: usize,
    prepared: bool,
) -> Result<(), fsqlite_error::FrankenError> {
    if prepared {
        let stmt = f.prepare(sql).await?;
        for _ in 0..runs {
            stmt.execute().await?;
        }
    } else {
        for _ in 0..runs {
            f.execute(sql).await?;
        }
    }
    Ok(())
}

#[test]
fn automatic_rowids_past_the_max_rowid_match_sqlite() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = |name: String| {
                if file_backed {
                    dir.path().join(name).to_string_lossy().into_owned()
                } else {
                    ":memory:".to_owned()
                }
            };
            for case in CASES {
                for prepared in [false, true] {
                    let what = format!("{} (prepared {prepared}, file {file_backed})", case.name);
                    let (f, r) = open_pair(&target(format!("{}_{prepared}.db", case.name)), case.setup)
                        .await;
                    for sql in case.before {
                        r.execute(sql, []).unwrap();
                        run_insert(&f, sql, 1, prepared)
                            .await
                            .unwrap_or_else(|e| panic!("{what}: `{sql}`: {e}"));
                    }
                    for _ in 0..case.runs {
                        r.execute(case.insert, []).unwrap();
                    }
                    run_insert(&f, case.insert, case.runs, prepared)
                        .await
                        .unwrap_or_else(|e| panic!("{what}: `{}`: {e}", case.insert));
                    for check in CHECKS {
                        assert_eq!(
                            rows_f(&f, check).await,
                            rows_r(&r, check),
                            "{what}: `{check}`"
                        );
                    }
                }
            }
            for (name, setup) in FULL_CASES {
                for prepared in [false, true] {
                    let what = format!("{name} (prepared {prepared}, file {file_backed})");
                    let (f, r) = open_pair(&target(format!("{name}_{prepared}.db")), setup).await;
                    let insert = "INSERT INTO t(v) VALUES ('a')";
                    let stock = r.execute(insert, []).unwrap_err();
                    assert_eq!(
                        stock.sqlite_error_code(),
                        Some(rusqlite::ErrorCode::DiskFull),
                        "{what}: stock {stock}"
                    );
                    let error = run_insert(&f, insert, 1, prepared)
                        .await
                        .expect_err(&format!("{what}: must fail SQLITE_FULL like stock"));
                    assert_eq!(error.error_code(), ErrorCode::Full, "{what}: {error}");
                    let check = format!("SELECT count(*), max(rowid) = {MAX} FROM t");
                    assert_eq!(rows_f(&f, &check).await, rows_r(&r, &check), "{what}");
                }
            }
        });
    }
}
