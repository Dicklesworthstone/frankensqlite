//! bd-x25ka(a) (REVIEW3-P3) positional short-circuit: `(subquery) AND <never-true>`
//! — the subquery on the LEFT — must be evaluated (and may raise its error) in a
//! FROM-less WHERE / HAVING / JOIN-ON, exactly like stock's left-to-right
//! evaluation; only a cost-reordered table WHERE may short-circuit it. frank used
//! to fold the whole `AND` to 0 regardless of position, silently dropping the
//! left subquery's error. A never-true constant on the LEFT still short-circuits
//! in every context (kept folding).
//!
//! Differential against rusqlite (bundled C SQLite): for each shape frank and
//! stock must AGREE — both error, or both succeed with identical rows.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn frank_rows_text(rows: &[fsqlite_core::connection::Row]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f}"),
                    SqliteValue::Text(s) => s.to_string(),
                    SqliteValue::Blob(b) => format!("blob:{}", b.len()),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn stock_rows_text(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<String>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let ncols = stmt.column_count();
    let rows = stmt
        .query_map([], |r| {
            let cells: Vec<String> = (0..ncols)
                .map(|i| match r.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                    rusqlite::types::ValueRef::Integer(n) => n.to_string(),
                    rusqlite::types::ValueRef::Real(f) => format!("{f}"),
                    rusqlite::types::ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
                    rusqlite::types::ValueRef::Blob(b) => format!("blob:{}", b.len()),
                })
                .collect();
            Ok(cells.join("|"))
        })
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r.map_err(|e| e.to_string())?);
    }
    Ok(out)
}

/// Assert frank agrees with stock on every query: same error-ness, and identical
/// rows when both succeed.
async fn assert_matches_stock(schema: &[&str], rows: &[&str], queries: &[&str]) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("bd_x25ka.db");
    {
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        for stmt in schema {
            stock.execute_batch(stmt).unwrap();
        }
        for stmt in rows {
            stock.execute_batch(stmt).unwrap();
        }
    }
    let stock = rusqlite::Connection::open(&db_path).unwrap();
    let conn = Connection::open(db_path.to_str().unwrap()).await.unwrap();

    for q in queries {
        let stock_res = stock_rows_text(&stock, q);
        let frank_res = conn.query(q).await;

        match (&frank_res, &stock_res) {
            (Ok(fr), Ok(sr)) => {
                assert_eq!(frank_rows_text(fr), *sr, "bd-x25ka: rows diverged on `{q}`");
            }
            (Err(_), Err(_)) => { /* both error: agree */ }
            (Ok(fr), Err(se)) => panic!(
                "bd-x25ka: frank SUCCEEDED (rows={:?}) but stock ERRORED ({se}) on `{q}` — \
                 a dead-branch error was silently swallowed",
                frank_rows_text(fr)
            ),
            (Err(fe), Ok(sr)) => {
                panic!("bd-x25ka: frank ERRORED ({fe}) but stock SUCCEEDED (rows={sr:?}) on `{q}`")
            }
        }
    }
}

#[test]
fn bd_x25ka_positional_shortcircuit_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        assert_matches_stock(
            &["CREATE TABLE t(a INTEGER);", "CREATE TABLE u(x INTEGER);"],
            &[
                "INSERT INTO t(a) VALUES (1), (2);",
                "INSERT INTO u(x) VALUES (10);",
            ],
            &[
                // Left never-true const short-circuits everywhere — no error, 0 rows.
                "SELECT 1 WHERE NULL AND (SELECT a, a FROM t);",
                "SELECT x FROM u WHERE NULL AND (SELECT a, a FROM t);",
                // Subquery on the LEFT (a 2-column subquery used as a scalar is a
                // hard error). FROM-less WHERE + JOIN-ON evaluate it left-to-right;
                // a table WHERE may cost-reorder. Whatever stock does, frank must
                // match — the point is frank must not SILENTLY fold it away where
                // stock raises.
                "SELECT 1 WHERE (SELECT a, a FROM t) AND NULL;",
                "SELECT t1.a FROM t t1 JOIN t t2 ON (SELECT a, a FROM t) AND NULL;",
                "SELECT x FROM u WHERE (SELECT a, a FROM t) AND NULL;",
                // Control: a well-formed dead subquery on the left is filtered, not
                // errored, in every context.
                "SELECT 1 WHERE (SELECT a FROM t LIMIT 1) AND NULL;",
                "SELECT x FROM u WHERE (SELECT a FROM t LIMIT 1) AND NULL;",
            ],
        )
        .await;
    });
}
