//! bd-x25ka(b) (REVIEW3-P3): a HAVING clause must name/aggregate-resolve its
//! column references at prepare, exactly like WHERE / JOIN-ON — a bare
//! `HAVING nosuchcol` (and `HAVING NULL AND nosuchcol`) raises `no such column`
//! in stock, but frank silently returned rows. bd-kcvra landed this resolution
//! for WHERE / JOIN-ON only; HAVING was the residual hole.
//!
//! Differential against rusqlite (bundled C SQLite): frank must AGREE with stock
//! on error-ness for every shape.

use fsqlite_core::connection::Connection;

fn stock_is_err(conn: &rusqlite::Connection, sql: &str) -> bool {
    // Prepare-time errors (name resolution) surface at prepare; execute the
    // statement fully so runtime errors are captured too.
    (|| -> Result<(), rusqlite::Error> {
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query([])?;
        while rows.next()?.is_some() {}
        Ok(())
    })()
    .is_err()
}

async fn assert_errness_matches_stock(schema: &[&str], rows: &[&str], queries: &[&str]) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("bd_x25ka_having.db");
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
        let stock_err = stock_is_err(&stock, q);
        let frank_err = conn.query(q).await.is_err();
        assert_eq!(
            frank_err, stock_err,
            "bd-x25ka(b): error-ness diverged on `{q}` (frank_err={frank_err}, stock_err={stock_err})"
        );
    }
}

#[test]
fn bd_x25ka_having_name_resolves_like_stock() {
    asupersync::test_utils::run_test(|| async {
        assert_errness_matches_stock(
            &["CREATE TABLE t(a INTEGER, b INTEGER);"],
            &["INSERT INTO t(a, b) VALUES (1, 10), (2, 20);"],
            &[
                // HAVING must raise `no such column` — bare and behind a never-true
                // AND, with and without GROUP BY.
                "SELECT 1 FROM t GROUP BY a HAVING nosuchcol;",
                "SELECT 1 FROM t GROUP BY a HAVING NULL AND nosuchcol;",
                "SELECT count(*) FROM t HAVING nosuchcol;",
                "SELECT count(*) FROM t HAVING NULL AND nosuchcol;",
                // Controls: a resolvable HAVING must still SUCCEED (no over-rejection).
                "SELECT a FROM t GROUP BY a HAVING count(*) > 0;",
                "SELECT count(*) FROM t HAVING count(*) > 1;",
                "SELECT a, sum(b) FROM t GROUP BY a HAVING sum(b) > 5;",
            ],
        )
        .await;
    });
}
