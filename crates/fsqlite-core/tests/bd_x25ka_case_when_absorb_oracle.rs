//! bd-x25ka(c) (REVIEW3-P3) CASE-WHEN absorb: inside a `CASE WHEN <test> ...`,
//! a LITERAL `X AND 0` compile-folds to FALSE and DROPS X's name/aggregate
//! resolution — exactly like every other context (stock: `WHERE nosuchcol AND 0`
//! is also silent). A never-true constant that is NOT the literal integer 0
//! (`X AND FALSE`, `X AND NULL`) does NOT absorb — X is resolved and raises
//! `no such column`.
//!
//! (The bead's original "CASE absorbs X AND FALSE while WHERE errors" model was
//! disproven by the sqlite3 oracle; this keeper encodes the corrected model.)
//!
//! Differential against rusqlite (bundled C SQLite): frank must AGREE on both
//! error-ness and result rows.

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

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<String>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let ncols = stmt.column_count();
    let mut out = Vec::new();
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    while let Some(r) = rows.next().map_err(|e| e.to_string())? {
        let cells: Vec<String> = (0..ncols)
            .map(|i| match r.get_ref(i).unwrap() {
                rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                rusqlite::types::ValueRef::Integer(n) => n.to_string(),
                rusqlite::types::ValueRef::Real(f) => format!("{f}"),
                rusqlite::types::ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
                rusqlite::types::ValueRef::Blob(b) => format!("blob:{}", b.len()),
            })
            .collect();
        out.push(cells.join("|"));
    }
    Ok(out)
}

async fn assert_matches_stock(schema: &[&str], rows: &[&str], queries: &[&str]) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("bd_x25ka_case.db");
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
        let stock_res = stock_rows(&stock, q);
        let frank_res = conn.query(q).await;
        match (&frank_res, &stock_res) {
            (Ok(fr), Ok(sr)) => assert_eq!(
                frank_rows_text(fr),
                *sr,
                "bd-x25ka(c): rows diverged on `{q}`"
            ),
            (Err(_), Err(_)) => {}
            (Ok(fr), Err(se)) => panic!(
                "bd-x25ka(c): frank SUCCEEDED ({:?}) but stock ERRORED ({se}) on `{q}`",
                frank_rows_text(fr)
            ),
            (Err(fe), Ok(sr)) => panic!(
                "bd-x25ka(c): frank ERRORED ({fe}) but stock SUCCEEDED ({sr:?}) on `{q}` — \
                 a literal `X AND 0` must drop X's resolution inside CASE WHEN"
            ),
        }
    }
}

#[test]
fn bd_x25ka_case_when_absorb_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        assert_matches_stock(
            &["CREATE TABLE t(a INTEGER);"],
            &["INSERT INTO t(a) VALUES (1);"],
            &[
                // Literal `X AND 0` absorbs -> 'n', X NOT resolved (matches stock).
                "SELECT CASE WHEN nosuchcol AND 0 THEN 'y' ELSE 'n' END;",
                "SELECT CASE WHEN 0 AND nosuchcol THEN 'y' ELSE 'n' END;",
                "SELECT CASE WHEN (SELECT abs(-9223372036854775807 - 1)) AND 0 THEN 'y' ELSE 'n' END;",
                // NOT the literal 0 -> X resolved -> both ERROR.
                "SELECT CASE WHEN nosuchcol AND FALSE THEN 'y' ELSE 'n' END;",
                "SELECT CASE WHEN nosuchcol AND NULL THEN 'y' ELSE 'n' END;",
                // Controls: valid CASE resolves and evaluates identically.
                "SELECT CASE WHEN a > 0 THEN 'y' ELSE 'n' END FROM t;",
                "SELECT CASE WHEN a AND 0 THEN 'y' ELSE 'n' END FROM t;",
            ],
        )
        .await;
    });
}
