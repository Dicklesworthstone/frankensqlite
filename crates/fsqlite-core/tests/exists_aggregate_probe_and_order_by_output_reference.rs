//! Two wrong-answer/crash cases found while reviewing the GH#419 correlated
//! EXISTS fast paths, each checked against stock SQLite (rusqlite):
//!
//! 1. The direct single-equality EXISTS probe answered "does a matching row
//!    exist" even when the subquery's result list is an aggregate. An
//!    aggregate without GROUP BY (`SELECT count(*) ... WHERE ...`) always
//!    yields one row, so `EXISTS` is true and `NOT EXISTS` is false for every
//!    outer row, matching or not.
//! 2. `SELECT 1 FROM t ORDER BY 1` (also inside EXISTS or a scalar subquery),
//!    `SELECT 1 AS a ... ORDER BY a`, and swapped aliases such as
//!    `SELECT z AS w, w AS z ... ORDER BY z` overflowed the stack: the sort-key
//!    resolver re-resolved the selected expression as another ORDER BY output
//!    reference, so the literal `1` re-read as ordinal 1 forever.

// The async engine futures nest deeply; match the other oracle suites.
#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE kx(x, t TEXT)",
    "INSERT INTO kx VALUES (1, 'a'), (2, 'b'), (5, 'e')",
    "CREATE TABLE oc(z INTEGER, w TEXT)",
    "INSERT INTO oc VALUES (1, 'a'), (2, 'q'), (3, 'c'), (NULL, NULL)",
];

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("{b:?}"),
    }
}

fn render_sqlite(value: rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("{b:?}"),
    }
}

fn sqlite_rows(sql: &str) -> Vec<Vec<String>> {
    let conn = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for statement in SETUP {
        conn.execute_batch(statement).expect("rusqlite setup");
    }
    let mut stmt = conn.prepare(sql).expect("rusqlite prepare");
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width)
            .map(|index| row.get::<_, rusqlite::types::Value>(index).map(render_sqlite))
            .collect::<Result<Vec<_>, _>>()
    })
    .expect("rusqlite query")
    .collect::<Result<Vec<_>, _>>()
    .expect("rusqlite rows")
}

async fn fsqlite_rows(sql: &str) -> Vec<Vec<String>> {
    let conn = Connection::open(":memory:").await.expect("open fsqlite");
    for statement in SETUP {
        conn.execute(statement).await.expect("fsqlite setup");
    }
    conn.query(sql)
        .await
        .unwrap_or_else(|error| panic!("fsqlite query failed for {sql}: {error}"))
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect()
}

fn owned(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|cell| (*cell).to_owned()).collect())
        .collect()
}

async fn assert_matches_sqlite(sql: &str, expected: &[&[&str]]) {
    let expected = owned(expected);
    assert_eq!(sqlite_rows(sql), expected, "stock SQLite oracle for {sql}");
    assert_eq!(fsqlite_rows(sql).await, expected, "fsqlite for {sql}");
}

#[test]
fn correlated_exists_over_aggregate_result_is_always_one_row() {
    asupersync::test_utils::run_test(|| async {
        // Every outer row has an aggregate row, so NOT EXISTS keeps none.
        assert_matches_sqlite(
            "SELECT z FROM oc WHERE NOT EXISTS \
             (SELECT count(*) FROM kx WHERE kx.x = oc.z) ORDER BY rowid",
            &[],
        )
        .await;
        assert_matches_sqlite(
            "SELECT z FROM oc WHERE NOT EXISTS \
             (SELECT max(x) FROM kx WHERE kx.x = oc.z) ORDER BY rowid",
            &[],
        )
        .await;
        assert_matches_sqlite(
            "SELECT count(*) FROM oc WHERE NOT EXISTS \
             (SELECT count(*) FROM kx WHERE kx.x = oc.z)",
            &[&["0"]],
        )
        .await;
        assert_matches_sqlite(
            "SELECT z FROM oc WHERE EXISTS \
             (SELECT count(*) FROM kx WHERE kx.x = oc.z + 100) ORDER BY rowid",
            &[&["1"], &["2"], &["3"], &["NULL"]],
        )
        .await;
        assert_matches_sqlite(
            "SELECT EXISTS (SELECT count(*) FROM kx WHERE x > 100)",
            &[&["1"]],
        )
        .await;
        // A non-aggregate result list still answers per matching row.
        assert_matches_sqlite(
            "SELECT z FROM oc WHERE NOT EXISTS \
             (SELECT 1 FROM kx WHERE kx.x = oc.z) ORDER BY rowid",
            &[&["3"], &["NULL"]],
        )
        .await;
    });
}

#[test]
fn order_by_output_reference_to_a_literal_or_swapped_alias_terminates() {
    asupersync::test_utils::run_test(|| async {
        assert_matches_sqlite("SELECT 1 FROM kx ORDER BY 1", &[&["1"], &["1"], &["1"]]).await;
        assert_matches_sqlite(
            "SELECT 1 AS a FROM kx ORDER BY a",
            &[&["1"], &["1"], &["1"]],
        )
        .await;
        assert_matches_sqlite("SELECT EXISTS (SELECT 1 FROM kx ORDER BY 1)", &[&["1"]]).await;
        assert_matches_sqlite("SELECT (SELECT 1 FROM kx ORDER BY 1)", &[&["1"]]).await;
        assert_matches_sqlite(
            "SELECT z FROM oc WHERE EXISTS \
             (SELECT 1 FROM kx WHERE kx.x = oc.z ORDER BY 1) ORDER BY rowid",
            &[&["1"], &["2"]],
        )
        .await;
        // `ORDER BY z` names the second output column (source column w), so
        // rows sort by w, not by the source column z.
        assert_matches_sqlite(
            "SELECT z AS w, w AS z FROM oc ORDER BY z",
            &[
                &["NULL", "NULL"],
                &["1", "'a'"],
                &["3", "'c'"],
                &["2", "'q'"],
            ],
        )
        .await;
    });
}
