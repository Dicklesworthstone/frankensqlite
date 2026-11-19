#![recursion_limit = "512"]
//! bd-uy8iy: in a GROUP BY group whose max() ties an INTEGER and a REAL of
//! equal value, fsqlite printed the later tie's spelling (8.0 where stock
//! prints 8), while typeof(max(n)) and a bare column already came from the
//! first tie. The connection's grouped evaluator took the extremum with
//! `Iterator::max_by`, which returns the last of equal maxima. Stock's max()
//! keeps the first one in scan order and replaces it only on a strictly
//! greater value; min() keeps the first of equal minima too.
//!
//! The keeper compares against bundled SQLite 3.54.1 on `:memory:` and on a
//! file: HAVING max(n), a + 0 beside max(n), ORDER BY max(n), the plain GROUP
//! BY and the min() twins, with ties INTEGER-then-REAL and REAL-then-INTEGER
//! and a bare column. The negatives a naive "always keep the first row" fails:
//! a later strictly larger value wins max(), a later strictly smaller one wins
//! min(), and NULLs are skipped.
//!
//! These shapes run in the connection's grouped evaluator, which strict mode
//! refuses (`group_by_fallback`). With strict on, a shape either answers as
//! stock does or is refused with that error; the run without strict checks
//! every shape's answer.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f:?}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f:?}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:02x?}"),
    }
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn.prepare(sql).expect("stock prepares");
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .expect("stock runs")
}

/// The rows, or `None` when strict mode refused the grouped evaluator.
async fn fsqlite_rows(conn: &Connection, sql: &str, strict: bool) -> Option<Vec<Vec<String>>> {
    match conn.query(sql).await {
        Ok(rows) => Some(
            rows.iter()
                .map(|row| row.values().iter().map(render_fsqlite).collect())
                .collect(),
        ),
        Err(e) if strict && e.to_string().contains("decision_reason=group_by_fallback") => None,
        Err(e) => panic!("fsqlite `{sql}` (strict={strict}): {e}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE g(a, n, b)",
    // Group 1: INTEGER then REAL tie. Group 2: REAL then INTEGER tie.
    // Group 3: a later strictly larger max and a later strictly smaller min.
    // Group 4: NULLs around a tie.
    "INSERT INTO g VALUES (1, 8, 'x'), (1, 8.0, 'y'), (2, 8.0, 'p'), (2, 8, 'q'), \
     (3, 5, 'r'), (3, 9.0, 's'), (3, 2.0, 't'), (4, NULL, 'u'), (4, 3.0, 'v'), (4, NULL, 'w'), \
     (4, 3, 'z')",
];

const QUERIES: &[&str] = &[
    "SELECT a, max(n), typeof(max(n)), b FROM g GROUP BY a HAVING max(n) > 2 ORDER BY a",
    "SELECT a + 0, max(n), typeof(max(n)) FROM g GROUP BY a ORDER BY 1",
    "SELECT a, max(n), typeof(max(n)) FROM g GROUP BY a ORDER BY max(n), a",
    "SELECT a, max(n), typeof(max(n)), b FROM g GROUP BY a ORDER BY a",
    "SELECT a, min(n), typeof(min(n)), b FROM g GROUP BY a HAVING min(n) > 1 ORDER BY a",
    "SELECT a + 0, min(n), typeof(min(n)) FROM g GROUP BY a ORDER BY 1",
    "SELECT a, min(n), typeof(min(n)) FROM g GROUP BY a ORDER BY min(n), a",
    "SELECT a, min(n), typeof(min(n)), b FROM g GROUP BY a ORDER BY a",
    "SELECT a, max(n), min(n), count(n) FROM g GROUP BY a HAVING count(*) > 1 ORDER BY a",
];

#[test]
fn grouped_extremum_ties_keep_the_first_value_like_stock() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    let expected: Vec<_> = QUERIES.iter().map(|sql| stock_rows(&stock, sql)).collect();
    // Not vacuous: stock prints the first tie's spelling, and a later strictly
    // larger value wins.
    assert_eq!(expected[3][0][1], "int:8", "{:?}", expected[3]);
    assert_eq!(expected[3][1][1], "real:8.0", "{:?}", expected[3]);
    assert_eq!(expected[3][2][1], "real:9.0", "{:?}", expected[3]);
    assert_eq!(expected[7][2][1], "real:2.0", "{:?}", expected[7]);
    asupersync::test_utils::run_test(|| async move {
        for file_backed in [false, true] {
            for strict in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("uy8iy.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                conn.set_reject_mem_fallback(strict);
                conn.set_strict_mem_fallback_rejection(strict);
                let mut refused = 0usize;
                for (sql, want) in QUERIES.iter().zip(&expected) {
                    match fsqlite_rows(&conn, sql, strict).await {
                        Some(got) => assert_eq!(
                            &got, want,
                            "file_backed={file_backed} strict={strict}: {sql}"
                        ),
                        None => refused += 1,
                    }
                }
                eprintln!(
                    "bd-uy8iy: file_backed={file_backed} strict={strict}: {refused} of {} \
                     shapes refused",
                    QUERIES.len()
                );
                // Without strict, every shape answers.
                assert!(strict || refused == 0);
            }
        }
    });
}
