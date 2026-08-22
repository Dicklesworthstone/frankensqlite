//! bd-lryih (P9 residual) — a JOIN-ON constraint of the shape
//! `ON (0 AND E) OR <eq>` must match stock sqlite3: the constant-false
//! `0 AND E` conjunct folds to FALSE at prepare, so the erroring uncorrelated
//! subquery `E` is dropped from the tree and never hoisted/executed, leaving
//! `FALSE OR <eq>` == `<eq>`.
//!
//! Root cause (pre-fix): the prepare-time SELECT rewrite (`rewrite_in_select_core`
//! in connection.rs) walked WHERE/HAVING/result columns but did NOT descend into
//! the FROM clause's JOIN constraints, so the Tier-1 `0 AND E`→FALSE fold in
//! `rewrite_in_expr` never reached ON expressions. The erroring uncorrelated
//! subquery inside `(0 AND E)` was therefore eagerly hoisted and errored, where
//! stock 3.46.1 returns rows.
//!
//! Companion to the scalar-CASE keeper (`bd_lryih_scalar_case_when_short_circuit`)
//! and the WHERE/CASE facets closed under dkswh. The guards prove we do NOT
//! over-fold: only `0 AND E` / `E AND 0` folds — `1 AND E`, `0 OR E`, and a bare
//! `E` must still evaluate `E` (and error) in both engines.
//!
//! Oracle: stock rusqlite (3.46.x). Each probe must AGREE with stock — same
//! value on success, or both error.

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
fn bd_lryih_join_on_short_circuit_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.expect("open fsqlite");
        let oracle = rusqlite::Connection::open_in_memory().expect("open rusqlite");

        // Identical two-row fixture in both engines.
        frank_exec(&frank, "CREATE TABLE t(id INTEGER, bad TEXT);").await;
        frank_exec(&frank, "INSERT INTO t VALUES(1,'x'),(2,'y');").await;
        oracle
            .execute_batch(
                "CREATE TABLE t(id INTEGER, bad TEXT); INSERT INTO t VALUES(1,'x'),(2,'y');",
            )
            .expect("setup (rusqlite)");

        // `E` = an erroring uncorrelated subquery (json_each on malformed JSON).
        const E: &str = "(SELECT count(*) FROM json_each('bare'))";

        // (sql, must_error) — must_error is the stock-verified expectation, and
        // both engines must agree with it.
        let probes: Vec<(String, bool)> = vec![
            // P9 fold: `0 AND E` conjunct drops E → `FALSE OR eq` == eq → 2 rows.
            (
                format!("SELECT count(*) FROM t a JOIN t b ON (0 AND {E}) OR a.id=b.id;"),
                false,
            ),
            // Commutative: `E AND 0` also folds to FALSE.
            (
                format!("SELECT count(*) FROM t a JOIN t b ON ({E} AND 0) OR a.id=b.id;"),
                false,
            ),
            // Guard: `1 AND E` must still evaluate E (AND does not skip on true left) → error.
            (
                format!("SELECT count(*) FROM t a JOIN t b ON (1 AND {E}) OR a.id=b.id;"),
                true,
            ),
            // Guard: `0 OR E` does NOT fold (only 0 AND E folds) → E evaluated → error.
            (
                format!("SELECT count(*) FROM t a JOIN t b ON (0 OR {E}) OR a.id=b.id;"),
                true,
            ),
            // Guard: a bare erroring subquery in ON is still evaluated → error.
            (
                format!("SELECT count(*) FROM t a JOIN t b ON {E} OR a.id=b.id;"),
                true,
            ),
            // Unaffected: an ordinary ON with no constant-false AND is unchanged.
            (
                "SELECT count(*) FROM t a JOIN t b ON a.id=b.id;".to_owned(),
                false,
            ),
        ];

        for (sql, must_error) in &probes {
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
