//! bd-815xj: a PROVABLY never-true, scan-backed HAVING / JOIN-ON filter is
//! dropped by stock's planner WITHOUT materializing its (uncorrelated) subqueries.
//! frank used to leave `(subquery) AND NULL` (subquery LEFT) unfolded in
//! HAVING / JOIN-ON (bd-x25ka(a)'s positional gate only folded a cost-reordered
//! table WHERE), then materialized the subquery and raised its RUNTIME error where
//! stock returns []. The fix collapses a whole never-true scan-backed HAVING/ON to
//! 0 before the eager subquery hoist.
//!
//! Uses `(SELECT abs(i64::MIN))` — integer overflow only when EVALUATED, so a
//! planner drop / short-circuit that never runs it produces no error.
//!
//! Differential against rusqlite: frank must AGREE with stock (error-ness + rows).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SUB: &str = "(SELECT abs(-9223372036854775807 - 1))";

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

async fn assert_matches_stock(schema: &[&str], rows: &[&str], queries: &[(&str, String)]) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("bd_815xj.db");
    {
        let stock = rusqlite::Connection::open(&db_path).unwrap();
        for s in schema {
            stock.execute_batch(s).unwrap();
        }
        for s in rows {
            stock.execute_batch(s).unwrap();
        }
    }
    let stock = rusqlite::Connection::open(&db_path).unwrap();
    let conn = Connection::open(db_path.to_str().unwrap()).await.unwrap();

    for (label, q) in queries {
        let stock_res = stock_rows(&stock, q);
        let frank_res = conn.query(q).await;
        match (&frank_res, &stock_res) {
            (Ok(fr), Ok(sr)) => assert_eq!(frank_rows_text(fr), *sr, "[{label}] rows diverged on `{q}`"),
            (Err(_), Err(_)) => {}
            (Ok(fr), Err(se)) => panic!(
                "[{label}] frank SUCCEEDED ({:?}) but stock ERRORED ({se}) on `{q}`",
                frank_rows_text(fr)
            ),
            (Err(fe), Ok(sr)) => panic!(
                "[{label}] frank ERRORED ({fe}) but stock SUCCEEDED ({sr:?}) on `{q}` — \
                 frank materialized a dead subquery stock's planner drops (bd-815xj)"
            ),
        }
    }
}

#[test]
fn bd_815xj_scan_backed_never_true_filter_planner_drop() {
    asupersync::test_utils::run_test(|| async {
        let q = |s: &str| s.replace("SUB", SUB);
        assert_matches_stock(
            &["CREATE TABLE u(x INTEGER);"],
            &["INSERT INTO u(x) VALUES (10), (20);"],
            &[
                // Whole never-true HAVING / JOIN-ON over a scan -> the sub is
                // dropped, no error (rusqlite short-circuits it).
                ("having-drop", q("SELECT 1 FROM u GROUP BY x HAVING SUB AND NULL")),
                ("join-on-drop", q("SELECT u1.x FROM u u1 JOIN u u2 ON SUB AND NULL")),
                ("having-nogroup", q("SELECT count(*) FROM u HAVING SUB AND NULL")),
                // `(sub) AND never-true` under an OR with a LIVE term is ALSO
                // dropped by rusqlite's planner (the AND-branch is never-true), so
                // the sub is never materialized — frank must match, not error.
                ("having-or-live", q("SELECT 1 FROM u GROUP BY x HAVING (SUB AND NULL) OR x>0")),
                ("table-where-or", q("SELECT x FROM u WHERE (SUB AND NULL) OR x=10")),
                ("join-on-or", q("SELECT u1.x FROM u u1 JOIN u u2 ON (SUB AND NULL) OR u1.x=u2.x")),
                // Control — table WHERE whole never-true is likewise dropped.
                ("table-where", q("SELECT x FROM u WHERE SUB AND NULL")),
                // Control — FROM-less: NO scan, so it stays left-to-right; whatever
                // rusqlite does (error vs value), frank must match.
                ("fromless-where", q("SELECT 1 WHERE SUB AND NULL")),
                // Control — left const short-circuits -> no error, sub not run.
                ("left-const-having", q("SELECT 1 FROM u GROUP BY x HAVING NULL AND SUB")),
                // Guard against over-drop: a valid HAVING is unaffected.
                ("valid-having", "SELECT x FROM u GROUP BY x HAVING count(*) > 0 ORDER BY x".to_owned()),
            ],
        )
        .await;
    });
}
