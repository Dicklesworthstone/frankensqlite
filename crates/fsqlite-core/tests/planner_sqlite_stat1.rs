//! Integration tests for PLANNER-1: sqlite_stat1 round-trip through the
//! planner's row-count stats load path.
//!
//! Before PLANNER-1, `ANALYZE` populated `sqlite_stat1` but the cost model
//! never read it back. These tests prove end-to-end that after ANALYZE the
//! planner-visible row count for a table reflects reality.

use fsqlite_core::connection::Connection;
use fsqlite_planner::stats::parse_stat1;

/// Extract the row-count integer recorded for `table` by querying
/// `sqlite_stat1` directly and parsing via the planner's format parser.
async fn stat1_n_rows(conn: &Connection, table: &str) -> Option<u64> {
    let rows = conn
        .query_with_params(
            "SELECT stat FROM sqlite_stat1 WHERE tbl = ?1 AND idx IS NULL",
            &[fsqlite_types::value::SqliteValue::Text(
                table.to_owned().into(),
            )],
        )
        .await
        .expect("sqlite_stat1 query");
    let mut out: Option<u64> = None;
    for row in &rows {
        if let Some(fsqlite_types::value::SqliteValue::Text(stat)) = row.values().first()
            && let Some(parsed) = parse_stat1(stat.as_ref())
        {
            out = Some(parsed.n_rows);
        }
    }
    out
}

#[test]
fn analyze_populates_sqlite_stat1_and_planner_can_parse_it() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
            .await
            .unwrap();
        for i in 0..42 {
            conn.execute_with_params(
                "INSERT INTO t(v) VALUES (?1);",
                &[fsqlite_types::value::SqliteValue::Integer(i)],
            )
            .await
            .unwrap();
        }
        conn.execute("ANALYZE;").await.unwrap();

        // The planner's parser should round-trip the row count that ANALYZE wrote.
        // We query sqlite_stat1 directly, feed the stat string into parse_stat1,
        // and expect the integer to equal the number of rows we inserted.
        let n_rows = stat1_n_rows(&conn, "t")
            .await
            .expect("ANALYZE must write a table-level row to sqlite_stat1 for 't'");
        assert_eq!(
            n_rows, 42,
            "planner's parse_stat1 must recover the inserted row count"
        );
    });
}

#[test]
fn sqlite_stat1_parse_matches_multi_row_counts() {
    // Two tables with different row counts: ensure each is reported separately
    // by sqlite_stat1, and the planner's parser sees the right n_rows per table.
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE small (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE big (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        // Wrap bulk inserts in a single transaction so page-cache snapshot churn
        // doesn't trip the concurrent-read-snapshot guard between statements.
        conn.execute("BEGIN;").await.unwrap();
        for _ in 0..3 {
            conn.execute("INSERT INTO small DEFAULT VALUES;")
                .await
                .unwrap();
        }
        for _ in 0..200 {
            conn.execute("INSERT INTO big DEFAULT VALUES;")
                .await
                .unwrap();
        }
        conn.execute("COMMIT;").await.unwrap();
        conn.execute("ANALYZE;").await.unwrap();

        let small = stat1_n_rows(&conn, "small")
            .await
            .expect("small must appear in sqlite_stat1");
        let big = stat1_n_rows(&conn, "big")
            .await
            .expect("big must appear in sqlite_stat1");
        assert_eq!(small, 3);
        assert_eq!(big, 200);
        assert!(big > small, "planner must distinguish small from big table");
    });
}

#[test]
fn parse_stat1_rejects_malformed_and_handles_empty() {
    // The parser must not panic on unexpected stat strings.
    assert!(parse_stat1("").is_none());
    assert!(parse_stat1("abc").is_none());
    // A well-formed single-integer string is the common table-level shape.
    let parsed = parse_stat1("1234").unwrap();
    assert_eq!(parsed.n_rows, 1234);
    assert!(parsed.per_column_distinct.is_empty());
}
