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
    // GH#428: an inner column with no COLLATE clause and TEXT affinity,
    // compared with outer NOCASE / RTRIM / typeless columns.
    "CREATE TABLE k(t TEXT, i INTEGER)",
    "INSERT INTO k VALUES ('1', 1), ('02', 2), ('abc', NULL), (NULL, 4), ('ABC ', 5)",
    "CREATE TABLE oc2(w TEXT COLLATE NOCASE, v TEXT COLLATE RTRIM, z INTEGER, x)",
    "INSERT INTO oc2 VALUES ('abc', 'abc', 1, 1), ('ABC', 'ABC', 2, '2'), \
     ('Abc ', 'ABC', 3, '02'), (NULL, NULL, NULL, NULL), ('zzz', '1  ', 5, 5.0)",
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

/// GH#427: EXISTS depends only on whether the subquery yields a row. SQLite
/// never evaluates a non-aggregate result list, so result expressions that
/// would raise must not surface, uncorrelated or correlated.
#[test]
fn exists_does_not_evaluate_its_result_list() {
    asupersync::test_utils::run_test(|| async {
        assert_matches_sqlite("SELECT EXISTS (SELECT json('bad') FROM kx)", &[&["1"]]).await;
        assert_matches_sqlite(
            "SELECT EXISTS (SELECT abs(-9223372036854775807 - 1) FROM kx)",
            &[&["1"]],
        )
        .await;
        assert_matches_sqlite(
            "SELECT z FROM oc WHERE EXISTS \
             (SELECT json('bad') FROM kx WHERE kx.x = oc.z) ORDER BY rowid",
            &[&["1"], &["2"]],
        )
        .await;
        assert_matches_sqlite(
            "SELECT z FROM oc WHERE NOT EXISTS \
             (SELECT json('bad') FROM kx WHERE kx.x = oc.z OR 0) ORDER BY rowid",
            &[&["3"], &["NULL"]],
        )
        .await;
    });
}

/// GH#427: a correlated EXISTS in the result list over an aggregate subquery
/// is true for every outer row, like the WHERE-clause form above.
#[test]
fn result_list_exists_over_aggregate_result_is_always_one_row() {
    asupersync::test_utils::run_test(|| async {
        let all_true: &[&[&str]] = &[&["1", "1"], &["2", "1"], &["3", "1"], &["NULL", "1"]];
        for sql in [
            "SELECT z, EXISTS (SELECT count(*) FROM kx WHERE kx.x = oc.z) FROM oc ORDER BY rowid",
            "SELECT z, EXISTS (SELECT max(t) FROM kx WHERE kx.x = oc.z) FROM oc ORDER BY rowid",
            "SELECT z, EXISTS (SELECT count(*) FROM kx WHERE kx.x = oc.z OR 0) \
             FROM oc ORDER BY rowid",
            "SELECT z, EXISTS (SELECT count(*) FROM kx WHERE kx.x = oc.z LIMIT 5) \
             FROM oc ORDER BY rowid",
        ] {
            assert_matches_sqlite(sql, all_true).await;
        }
        // The non-aggregate form still answers per matching row.
        assert_matches_sqlite(
            "SELECT z, EXISTS (SELECT 1 FROM kx WHERE kx.x = oc.z) FROM oc ORDER BY rowid",
            &[&["1", "1"], &["2", "1"], &["3", "0"], &["NULL", "0"]],
        )
        .await;
        // LIMIT 0 on an aggregate yields no row.
        assert_matches_sqlite(
            "SELECT z, EXISTS (SELECT count(*) FROM kx WHERE kx.x = oc.z LIMIT 0) \
             FROM oc ORDER BY rowid",
            &[&["1", "0"], &["2", "0"], &["3", "0"], &["NULL", "0"]],
        )
        .await;
    });
}

/// GH#428: comparing an inner column with an outer column follows SQLite's
/// operand rules. The left column's collation wins, and a column with no
/// COLLATE clause counts as BINARY. Two operands that both carry an affinity,
/// neither numeric, compare without conversion, so TEXT '1' is not the typeless
/// column's integer 1.
#[test]
fn correlated_column_comparisons_use_sqlite_operand_rules() {
    asupersync::test_utils::run_test(|| async {
        let abc: &[&[&str]] = &[&["'abc'"]];
        for sql in [
            "SELECT w FROM oc2 WHERE EXISTS (SELECT 1 FROM k WHERE k.t = oc2.w) ORDER BY rowid",
            "SELECT w FROM oc2 WHERE EXISTS (SELECT 1 FROM k WHERE k.t = oc2.w OR 0) ORDER BY rowid",
            "SELECT w FROM oc2 WHERE EXISTS \
             (SELECT 1 FROM k WHERE k.t = oc2.w OR 0 LIMIT 1) ORDER BY rowid",
            "SELECT v FROM oc2 WHERE EXISTS (SELECT 1 FROM k WHERE k.t = oc2.v OR 0) ORDER BY rowid",
        ] {
            assert_matches_sqlite(sql, abc).await;
        }
        for sql in [
            "SELECT x FROM oc2 WHERE EXISTS (SELECT 1 FROM k WHERE k.t = oc2.x) ORDER BY rowid",
            "SELECT x FROM oc2 WHERE EXISTS \
             (SELECT 1 FROM k WHERE k.t = oc2.x LIMIT 1) ORDER BY rowid",
        ] {
            assert_matches_sqlite(sql, &[&["'02'"]]).await;
        }
        assert_matches_sqlite(
            "SELECT x FROM oc2 WHERE EXISTS \
             (SELECT 1 FROM k WHERE (k.t, k.i) = (oc2.x, oc2.z) OR 0) ORDER BY rowid",
            &[],
        )
        .await;
        assert_matches_sqlite(
            "SELECT w, (SELECT count(*) FROM k WHERE k.t = oc2.w) FROM oc2 ORDER BY rowid",
            &[
                &["'abc'", "1"],
                &["'ABC'", "0"],
                &["'Abc '", "0"],
                &["NULL", "0"],
                &["'zzz'", "0"],
            ],
        )
        .await;
        assert_matches_sqlite(
            "SELECT x, (SELECT count(*) FROM k WHERE k.t = oc2.x) FROM oc2 ORDER BY rowid",
            &[&["1", "0"], &["'2'", "0"], &["'02'", "1"], &["NULL", "0"], &["5", "0"]],
        )
        .await;
        // The same operand rules apply to a plain join.
        assert_matches_sqlite(
            "SELECT oc2.x, k.t = oc2.x FROM oc2, k WHERE k.t = '1' ORDER BY oc2.rowid",
            &[
                &["1", "0"],
                &["'2'", "0"],
                &["'02'", "0"],
                &["NULL", "NULL"],
                &["5", "0"],
            ],
        )
        .await;
        // IN applies the left operand's collation, so NOCASE matches here.
        assert_matches_sqlite(
            "SELECT w FROM oc2 WHERE w IN (SELECT t FROM k) ORDER BY rowid",
            &[&["'abc'"], &["'ABC'"], &["'Abc '"]],
        )
        .await;
    });
}
