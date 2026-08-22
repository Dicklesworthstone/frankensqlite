//! bd-and-or-short-circuit-value-jump-gaps-dkswh, residual P10/P12 — the
//! polarity-dependent `NULL AND E` truth-context rule vs stock sqlite3.
//!
//! In a *seeking-true* truth context (a bare `WHERE`), `NULL AND E` can never be
//! TRUE (`NULL AND E` ∈ {FALSE, NULL}, neither is TRUE), so stock sqlite3 never
//! evaluates the erroring uncorrelated subquery `E` — the row is simply excluded.
//! Under `NOT` the inner `NULL AND E` becomes *seeking-false*: `NOT(NULL AND E)`
//! is TRUE only when `NULL AND E` is FALSE, which requires evaluating `E`, so
//! stock DOES evaluate it (and errors when `E` errors).
//!
//! frankensqlite currently hoists/executes the uncorrelated subquery at prepare
//! (`rewrite_in_expr`), before the truth-context short-circuit can protect it, so
//! the seeking-true `WHERE NULL AND E` errors where stock returns no rows.
//!
//! `json_each('bare')` raises "malformed JSON" in BOTH engines, so it is the
//! erroring operand `E`; the ONLY way the row survives-without-error (P10) or
//! errors (P12) is the polarity-correct short-circuit.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn render(v: &SqliteValue) -> String {
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
        .map(|r| r.values().iter().map(render).collect())
        .collect())
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
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
                    rusqlite::types::Value::Blob(b) => {
                        format!(
                            "X'{}'",
                            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                        )
                    }
                });
            }
            Ok(cols)
        })
        .map_err(|e| e.to_string())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| e.to_string())?;
    Ok(out)
}

async fn setup(frank: &Connection, stock: &rusqlite::Connection) {
    frank
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY);")
        .await
        .expect("frank create");
    frank
        .execute("INSERT INTO t(id) VALUES (1),(2);")
        .await
        .expect("frank insert");
    stock
        .execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t(id) VALUES (1),(2);")
        .expect("stock setup");
}

// The seeking-true polarity fold landed in rewrite_in_expr (bd-lryih P10/P12,
// 45d02fe49): `WHERE NULL AND E` now short-circuits to no rows without executing
// the uncorrelated subquery E, while `WHERE NOT (NULL AND E)` stays eager and
// errors (both engines). Un-ignored 2026-08-18 — verified green vs sqlite3
// 3.46.1 at HEAD.
#[test]
fn bd_and_or_null_and_polarity_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.expect("open frank");
        let stock = rusqlite::Connection::open_in_memory().expect("open stock");
        setup(&frank, &stock).await;

        // (sql, must_error) — stock-verified expectation; both engines must agree.
        let probes: &[(&str, bool)] = &[
            // P10: seeking-true WHERE — NULL AND E is never TRUE → E skipped → no rows, no error.
            (
                "SELECT id FROM t WHERE NULL AND (SELECT count(*) FROM json_each('bare')) ORDER BY id;",
                false,
            ),
            // P12: under NOT — inner is seeking-false → E MUST be evaluated → both error.
            (
                "SELECT id FROM t WHERE NOT (NULL AND (SELECT count(*) FROM json_each('bare'))) ORDER BY id;",
                true,
            ),
            // Guard: a plain WHERE over the erroring subquery still evaluates it → both error.
            (
                "SELECT id FROM t WHERE (SELECT count(*) FROM json_each('bare')) ORDER BY id;",
                true,
            ),
            // Sanity: NULL AND <true> also yields no rows (no error either way).
            ("SELECT id FROM t WHERE NULL AND 1 ORDER BY id;", false),
        ];

        for (sql, must_error) in probes {
            let f = frank_rows(&frank, sql).await;
            let s = stock_rows(&stock, sql);
            assert_eq!(
                s.is_err(),
                *must_error,
                "stock expectation wrong for `{sql}`: {s:?}"
            );
            assert_eq!(
                f.is_err(),
                *must_error,
                "fsqlite error-state mismatch vs stock for `{sql}`: got {f:?}"
            );
            if !*must_error {
                assert_eq!(
                    f.as_ref().ok(),
                    s.as_ref().ok(),
                    "fsqlite vs stock row divergence for `{sql}`"
                );
            }
        }

        frank.close().await.expect("close frank");
    });
}
