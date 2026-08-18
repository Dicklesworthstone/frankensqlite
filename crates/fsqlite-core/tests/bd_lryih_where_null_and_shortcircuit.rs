//! bd-lryih (P10 residual) — a WHERE clause of the shape `NULL AND E`
//! (E = an erroring uncorrelated, non-VDBE-lowerable subquery such as
//! `(SELECT count(*) FROM json_each('bare'))`) must match stock sqlite3 3.46.1:
//! `NULL` absorbs `AND` when the filter is seeking-true, so E is short-circuited
//! and never evaluated → the row is filtered and the query returns empty, NOT an
//! error. Under `NOT(...)` the polarity flips (seeking-false), so E IS evaluated
//! and its error surfaces — exactly stock's polarity-dependent behavior.
//!
//! Root cause (pre-fix): a plain single-table SELECT with a non-correlated
//! subquery buried in WHERE fell through the dispatch cascade to the VDBE
//! compiled path, whose prepare-time `rewrite_in_expr` EAGERLY executes the
//! uncorrelated subquery (it only folds `0 AND E`, never `NULL AND E`), so E
//! errored before any truth-context short-circuit ran. Fix: route this shape to
//! the semantic fallback `execute_join_select`, whose per-row WHERE filter
//! (`eval_expr_truthiness`) already short-circuits with correct polarity.
//!
//! Guards prove no over-fold and no over-capture: `1 AND E` / `NULL OR E` /
//! `NOT(NULL AND E)` must still evaluate E and error; `1 AND Egood` /
//! `0 OR Egood` / a bare valid subquery must return the same rows as stock.
//!
//! Oracle: stock rusqlite (3.46.x). Each probe must AGREE with stock.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    let out = stmt
        .query_map([], |row| {
            let mut cols = Vec::with_capacity(n);
            for i in 0..n {
                let v: rusqlite::types::Value = row.get_unwrap(i);
                cols.push(match v {
                    rusqlite::types::Value::Null => "NULL".to_owned(),
                    rusqlite::types::Value::Integer(x) => x.to_string(),
                    rusqlite::types::Value::Real(f) => format!("{f}"),
                    rusqlite::types::Value::Text(s) => format!("'{s}'"),
                    rusqlite::types::Value::Blob(b) => format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    ),
                });
            }
            Ok(cols)
        })
        .map_err(|e| e.to_string())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| e.to_string())?;
    Ok(out)
}

async fn frank_exec(conn: &Connection, sql: &str) {
    conn.execute(sql).await.expect("setup exec (fsqlite)");
}

#[test]
fn bd_lryih_where_null_and_shortcircuit_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.expect("open fsqlite");
        let oracle = rusqlite::Connection::open_in_memory().expect("open rusqlite");

        frank_exec(&frank, "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT);").await;
        frank_exec(&frank, "INSERT INTO t VALUES(1,'bare'),(2,'[1,2]');").await;
        oracle
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT); \
                 INSERT INTO t VALUES(1,'bare'),(2,'[1,2]');",
            )
            .expect("setup (rusqlite)");

        // Ebad errors (malformed JSON); Egood = 2 (valid, truthy).
        const EBAD: &str = "(SELECT count(*) FROM json_each('bare'))";
        const EGOOD: &str = "(SELECT count(*) FROM json_each('[1,2]'))";

        // (where_clause, must_error) — must_error is stock-verified (3.46.1).
        let probes: Vec<(String, bool)> = vec![
            // P10: NULL absorbs AND seeking-true → E short-circuited → empty, no error.
            (format!("NULL AND {EBAD}"), false),
            // P12: under NOT, polarity flips → E evaluated → error.
            (format!("NOT(NULL AND {EBAD})"), true),
            // P11: NULL does NOT absorb OR seeking-true → E evaluated → error.
            (format!("NULL OR {EBAD}"), true),
            // Guard: 1 AND E must evaluate E (true left does not skip AND right) → error.
            (format!("1 AND {EBAD}"), true),
            // Over-capture safety: 1 AND Egood evaluates Egood (=2, truthy) → rows 1,2.
            (format!("1 AND {EGOOD}"), false),
            // Over-capture safety: 0 OR Egood evaluates Egood → rows 1,2.
            (format!("0 OR {EGOOD}"), false),
            // P9 cross-check: 0 AND Ebad folds to FALSE → empty, no error.
            (format!("0 AND {EBAD}"), false),
            // Bare valid subquery (not short-circuitable) stays correct → rows 1,2.
            (EGOOD.to_owned(), false),
        ];

        for (where_clause, must_error) in &probes {
            let sql = format!("SELECT id FROM t WHERE {where_clause};");
            let f = frank_rows(&frank, &sql).await;
            let s = sqlite_rows(&oracle, &sql);

            assert_eq!(
                f.is_err(),
                *must_error,
                "fsqlite error-state mismatch vs expectation for `{sql}`: got {f:?}"
            );
            assert_eq!(
                s.is_err(),
                *must_error,
                "stock oracle error-state mismatch vs expectation for `{sql}`: got {s:?} \
                 (if this fires, the expectation is wrong, not the engine)"
            );
            if !*must_error {
                assert_eq!(
                    f.as_ref().ok(),
                    s.as_ref().ok(),
                    "fsqlite vs stock row divergence for `{sql}`"
                );
            }
        }

        frank.close().await.expect("close fsqlite");
    });
}
