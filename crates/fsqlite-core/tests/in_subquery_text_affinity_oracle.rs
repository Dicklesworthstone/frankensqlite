//! bd-a0oa9: `IN (subquery)` with a TEXT-affinity LHS must apply NUMERIC
//! comparison affinity, so text-numeric rows ('02','2.0') match an integer
//! subquery result — matching stock SQLite.
//!
//! Repro (from the bead): stock sqlite3 3.46.1 returns id 1,2,3; fsqlite
//! (pre-fix) returns only 1 because it probes with the LHS column's own TEXT
//! affinity instead of the combined comparison affinity.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &str = "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT); \
     INSERT INTO categories VALUES (2,'docs'),(3,'other'); \
     CREATE TABLE t2 (id INTEGER PRIMARY KEY, cid TEXT); \
     INSERT INTO t2 VALUES (1,'2'),(2,'02'),(3,'2.0');";
const QUERY: &str =
    "SELECT id FROM t2 WHERE cid IN (SELECT id FROM categories WHERE name='docs') ORDER BY id;";

#[test]
fn in_subquery_text_lhs_applies_numeric_affinity_like_stock() {
    // ── Stock SQLite reference ──
    let stock: Vec<i64> = {
        let c = rusqlite::Connection::open_in_memory().expect("open stock");
        c.execute_batch(SETUP).expect("stock setup");
        let mut stmt = c.prepare(QUERY).expect("stock prepare");
        stmt.query_map([], |r| r.get::<_, i64>(0))
            .expect("stock query")
            .collect::<Result<Vec<_>, _>>()
            .expect("stock rows")
    };
    assert_eq!(stock, vec![1, 2, 3], "premise: stock matches '2','02','2.0'");

    // ── FrankenSQLite under test ──
    asupersync::test_utils::run_test(move || async move {
        let conn = Connection::open(":memory:").await.expect("open fsqlite");
        for stmt in SETUP.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            conn.execute(stmt).await.expect("fsqlite setup stmt");
        }

        // The compiled program applies NUMERIC (`Affinity … C`) to both the
        // subquery value before `IdxInsert` and to `cid` before the membership
        // probe, so '02'/'2.0' coerce to 2 and match — verified via EXPLAIN
        // during triage (bd-a0oa9). This keeper locks the row-level contract.
        let rows = conn.query(QUERY).await.expect("fsqlite query");
        let frank: Vec<i64> = rows
            .iter()
            .map(|r| match r.values()[0] {
                SqliteValue::Integer(n) => n,
                ref other => panic!("id not integer: {other:?}"),
            })
            .collect();

        assert_eq!(
            frank, stock,
            "IN (subquery) with TEXT LHS must apply NUMERIC comparison affinity: \
             '02' and '2.0' must match integer 2 (frank={frank:?} stock={stock:?})"
        );
    });
}
