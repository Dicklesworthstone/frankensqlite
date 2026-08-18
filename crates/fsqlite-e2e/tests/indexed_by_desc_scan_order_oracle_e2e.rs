//! bd-gh-indexed-by-desc-scan-order (GH #224) — Oracle-parity e2e:
//! `... FROM t INDEXED BY <idx>` with no usable WHERE constraint must use the
//! forced index as a FULL index scan (covering when possible), streaming rows in
//! the index's stored order — descending for a `DESC` index — exactly like C
//! SQLite. Frank previously ignored the forced index for an unconstrained scan
//! and silently full-scanned the table (rowid order), so a `DESC` index printed
//! ascending/insertion order instead of C SQLite's descending order.
//!
//! Every scenario compares the query result IN ORDER (never sorted) against
//! rusqlite (= C SQLite), because ORDER is precisely what regresses. The EQP
//! scenario additionally asserts both engines report the forced index as a
//! COVERING scan. Insert order (30,10,20) is deliberately distinct from ascending
//! (10,20,30), descending (30,20,10), AND rowid/table-scan order (30,10,20), so a
//! table-scan fallback is caught by row order alone.
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

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

/// Query frank, preserving row order (no sort).
async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect()
}

/// Query rusqlite (= C SQLite), preserving row order (no sort).
fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap_or_else(|e| panic!("csql prep `{sql}`: {e}"));
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
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
        Ok(out)
    })
    .unwrap_or_else(|e| panic!("csql run `{sql}`: {e}"))
    .map(Result::unwrap)
    .collect()
}

/// Flattened EXPLAIN QUERY PLAN text for frank (all cells of all rows joined).
async fn frank_plan(conn: &Connection, sql: &str) -> String {
    conn.query(&format!("EXPLAIN QUERY PLAN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("frank EQP `{sql}`: {e}"))
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(render_frank)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>()
        .join("  ///  ")
}

/// Flattened EXPLAIN QUERY PLAN text for rusqlite (detail column of all rows).
fn sqlite_plan(conn: &rusqlite::Connection, sql: &str) -> String {
    let mut stmt = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap_or_else(|e| panic!("csql EQP prep `{sql}`: {e}"));
    stmt.query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .join("  ///  ")
}

/// Build a fresh frank + rusqlite pair, run the DDL/DML setup on both.
async fn fresh(setup: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in setup {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank setup `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("csql setup `{s}`: {e}"));
    }
    (f, r)
}

/// Assert frank and C SQLite return byte-identical rows IN ORDER for `query`.
async fn assert_ordered_parity(setup: &[&str], query: &str, label: &str) {
    let (f, r) = fresh(setup).await;
    let got = frank_rows(&f, query).await;
    let want = sqlite_rows(&r, query);
    assert_eq!(
        got, want,
        "{label}: row ORDER diverged for `{query}`\n  frank: {got:?}\n  csql:  {want:?}"
    );
}

const DESC_IDX: [&str; 3] = [
    "CREATE TABLE t (a INTEGER)",
    "CREATE INDEX idx ON t(a DESC)",
    "INSERT INTO t VALUES (30),(10),(20)",
];
const ASC_IDX: [&str; 3] = [
    "CREATE TABLE t (a INTEGER)",
    "CREATE INDEX idx ON t(a)",
    "INSERT INTO t VALUES (30),(10),(20)",
];

/// GH #224 core repro: DESC index, unconstrained forced scan → descending order.
// RED keeper (confirmed 6/6 fail at HEAD): the forced INDEXED BY index is
// ignored for an unconstrained scan (frank table-scans in rowid order), so both
// ASC and DESC forced scans diverge from C SQLite's stored index order. The fix
// is a planner+codegen full-index-scan feature (covering + non-covering; forward
// Rewind/Next for ASC, reverse Last/Prev for DESC; EQP `USING COVERING INDEX`;
// LIMIT streaming). Un-ignore when it lands — bd-gh-indexed-by-desc-scan-order.
#[test]
fn select_desc_index_scan_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        assert_ordered_parity(
            &DESC_IDX,
            "SELECT a FROM t INDEXED BY idx",
            "select_desc_index_scan",
        )
        .await;
    });
}

/// Control: ASC index, unconstrained forced scan → ascending order.
#[test]
fn select_asc_index_scan_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        assert_ordered_parity(
            &ASC_IDX,
            "SELECT a FROM t INDEXED BY idx",
            "select_asc_index_scan",
        )
        .await;
    });
}

/// Multi-row, multi-column covering DESC index full scan → stored order parity.
#[test]
fn select_desc_multicol_index_scan_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let setup = [
            "CREATE TABLE t (a INTEGER, b TEXT)",
            "CREATE INDEX idx ON t(a DESC, b)",
            "INSERT INTO t VALUES (2,'y'),(1,'x'),(2,'a'),(3,'z'),(1,'m')",
        ];
        assert_ordered_parity(
            &setup,
            "SELECT a, b FROM t INDEXED BY idx",
            "select_desc_multicol_index_scan",
        )
        .await;
    });
}

/// Non-covering `SELECT *` forced DESC scan (needs table lookup) → order parity.
#[test]
fn select_star_desc_index_non_covering_order() {
    asupersync::test_utils::run_test(|| async {
        let setup = [
            "CREATE TABLE t (a INTEGER, b TEXT)",
            "CREATE INDEX idx ON t(a DESC)",
            "INSERT INTO t VALUES (30,'c'),(10,'a'),(20,'b')",
        ];
        assert_ordered_parity(
            &setup,
            "SELECT * FROM t INDEXED BY idx",
            "select_star_desc_index",
        )
        .await;
    });
}

/// LIMIT streams straight off the forced DESC index scan in stored order.
#[test]
fn select_desc_index_scan_with_limit_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        assert_ordered_parity(
            &DESC_IDX,
            "SELECT a FROM t INDEXED BY idx LIMIT 2",
            "select_desc_index_scan_limit",
        )
        .await;
    });
}

/// EXPLAIN QUERY PLAN: both engines must report the forced index as the covering
/// access path for an unconstrained `INDEXED BY` scan (C SQLite: `SCAN t USING
/// COVERING INDEX idx`; frank renders the covering index off the emitted
/// opcodes). Frank previously reported a plain table `SCAN t`.
#[test]
fn explain_query_plan_forced_covering_index() {
    asupersync::test_utils::run_test(|| async {
        for setup in [&DESC_IDX, &ASC_IDX] {
            let (f, r) = fresh(setup).await;
            let query = "SELECT a FROM t INDEXED BY idx";
            let fplan = frank_plan(&f, query).await;
            let rplan = sqlite_plan(&r, query);
            assert!(
                rplan.contains("COVERING INDEX idx"),
                "csql EQP should use covering idx: {rplan}"
            );
            assert!(
                fplan.contains("COVERING INDEX idx"),
                "frank EQP should use the forced covering index, got: {fplan}"
            );
        }
    });
}
