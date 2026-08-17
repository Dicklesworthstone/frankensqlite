//! bd-lryih (scalar residual) — FROM-less pure-scalar searched-CASE `WHEN`
//! conditions must short-circuit AND/OR left-to-right, exactly like stock
//! sqlite3 and like the FROM-bearing VDBE codegen path.
//!
//! A pure-scalar FROM-less `SELECT` (no subquery / WHERE / ORDER BY) routes
//! through `compile_expression_select` → `emit_case_expr` (connection.rs). Its
//! searched-CASE branch evaluated the whole WHEN condition eagerly, so
//! `CASE WHEN 1 OR <erroring> …` errored where stock short-circuits `1 OR …`
//! to TRUE and never evaluates the erroring operand.
//!
//! Companion to facet-1 (interpreter path `eval_expr_with_subqueries`,
//! 5a7b00e1b) and to the FROM-bearing codegen path
//! (`emit_searched_case_when_condition`) which already short-circuits. This
//! keeper isolates the FROM-less scalar expression-compiler organ.
//!
//! Oracle: stock rusqlite (3.46.x). Each probe must AGREE with stock — same
//! value on success, or both error. The guards prove we do NOT over-fold:
//! `0 OR E` and `1 AND E` must still evaluate `E` (and error) in both engines.

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

#[test]
fn bd_lryih_scalar_case_when_short_circuit_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.expect("open fsqlite");
        let oracle = rusqlite::Connection::open_in_memory().expect("open rusqlite");

        // (sql, must_error) — must_error is the stock-verified expectation, and
        // both engines must agree with it.
        let probes: &[(&str, bool)] = &[
            // TRUE-absorbs-OR: `1 OR E` short-circuits → E never evaluated → THEN.
            (
                "SELECT CASE WHEN 1 OR json_extract('bare','$.a') THEN 1 ELSE 0 END;",
                false,
            ),
            // Guard: `0 OR E` must still evaluate E (no short-circuit) → error.
            (
                "SELECT CASE WHEN 0 OR json_extract('bare','$.a') THEN 1 ELSE 0 END;",
                true,
            ),
            // Guard: `1 AND E` must still evaluate E (AND does not skip on true left) → error.
            (
                "SELECT CASE WHEN 1 AND json_extract('bare','$.a') THEN 1 ELSE 0 END;",
                true,
            ),
            // Nested: left OR already true short-circuits the erroring right.
            (
                "SELECT CASE WHEN (1 OR json_extract('bare','$.a')) OR json_extract('nope','$.b') THEN 7 ELSE 0 END;",
                false,
            ),
            // Ordinary multi-WHEN CASE still selects the right arm.
            (
                "SELECT CASE WHEN 0 THEN 'a' WHEN 1 THEN 'b' ELSE 'c' END;",
                false,
            ),
            // ELSE fall-through unaffected.
            ("SELECT CASE WHEN 0 THEN 'x' ELSE 'y' END;", false),
        ];

        for (sql, must_error) in probes {
            let f = frank_rows(&frank, sql).await;
            let s = sqlite_rows(&oracle, sql);

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
                    "fsqlite vs stock value divergence for `{sql}`"
                );
            }
        }

        frank.close().await.expect("close fsqlite");
    });
}
