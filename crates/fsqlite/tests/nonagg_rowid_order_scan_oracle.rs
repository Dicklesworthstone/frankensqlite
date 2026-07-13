//! bd-nonagg-rowid-order-scan (+ -desc): `SELECT ... FROM t ORDER BY <rowid> [ASC|DESC] [LIMIT k]` with
//! NO WHERE needs NO sorter — a forward table scan (Rewind+Next) is already ascending rowid order and a
//! reverse walk (Last+Prev) is descending, and the rowid is unique so there are no ties. Previously
//! `resolve_order_by_index_plan` returned None for the rowid, so all rows were sorted. ASC routes to the
//! plain scan; DESC to the unbounded descending rowid-range scan (both apply LIMIT/OFFSET, stop early).
//! Both directions also admit a plain-scan-safe WHERE (no MATCH, no subquery) via a descending-capable
//! `codegen_select_full_scan`. Compared IN OUTPUT ORDER against C SQLite; scan-served cases assert NO
//! sorter opcode; a subquery filter or a non-rowid ORDER BY still uses the sorter (correctness kept).
use fsqlite::Connection;
use fsqlite_types::SqliteValue;
fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
// ORDERED: preserve row order (the lever's correctness is about output order).
fn frank_ord(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    c.query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}
fn sqlite_ord(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}
fn has_sorter(c: &Connection, sql: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with("Sorter")))
}
fn setup(ddl: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in ddl {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}
fn ins(f: &Connection, r: &rusqlite::Connection, s: &str) {
    f.execute(s).unwrap();
    r.execute_batch(s).unwrap();
}
fn cmp_ord(f: &Connection, r: &rusqlite::Connection, sql: &str, l: &str) {
    assert_eq!(
        frank_ord(f, sql),
        sqlite_ord(r, sql),
        "[{l}] order diverged: `{sql}`"
    );
}
#[test]
fn nonagg_rowid_order_scan_matches_sqlite() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);",
        "CREATE INDEX idx_c ON t(c);",
    ]);
    for i in 1..=600_i64 {
        let a = if i % 17 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 20)
        };
        let c = if i % 19 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 12)
        };
        ins(
            &f,
            &r,
            &format!("INSERT INTO t VALUES ({i}, {a}, {c}, 'v{}');", i % 7),
        );
    }
    // Served by a scan, NO sorter, verified in output order: WHERE-less ORDER BY rowid (ASC or DESC),
    // and — the new cut — a plain-scan-safe filtered ORDER BY rowid ASC.
    let scans = [
        // ASC, no WHERE
        "SELECT * FROM t ORDER BY id",
        "SELECT * FROM t ORDER BY id LIMIT 10",
        "SELECT id, x FROM t ORDER BY id ASC LIMIT 5 OFFSET 3",
        "SELECT rowid, x FROM t ORDER BY rowid LIMIT 3", // explicit rowid keyword
        "SELECT id FROM t ORDER BY id LIMIT 1",          // top-1
        // DESC (bd-nonagg-rowid-order-scan-desc): Last + Prev, no sorter.
        "SELECT * FROM t ORDER BY id DESC",
        "SELECT * FROM t ORDER BY id DESC LIMIT 10", // latest-10
        "SELECT id, x FROM t ORDER BY id DESC LIMIT 5 OFFSET 3",
        "SELECT id FROM t ORDER BY id DESC LIMIT 1", // bottom-1
        // filtered ASC (bd-nonagg-rowid-order-scan filtered): plain-scan-safe WHERE, no sorter.
        "SELECT * FROM t WHERE c = 5 ORDER BY id", // eq on an indexed column
        "SELECT * FROM t WHERE x = 'v3' ORDER BY id LIMIT 20", // eq on a non-indexed column
        "SELECT id FROM t WHERE a > 10 AND c < 8 ORDER BY id LIMIT 5", // AND
        "SELECT * FROM t WHERE c IN (1,3,5) ORDER BY id", // IN (value list)
        "SELECT * FROM t WHERE a BETWEEN 3 AND 9 ORDER BY id LIMIT 10",
        "SELECT * FROM t WHERE x IS NOT NULL ORDER BY id LIMIT 15",
        "SELECT c FROM t WHERE x LIKE 'v%' ORDER BY id LIMIT 7", // LIKE (not MATCH)
        // filtered DESC (bd-nonagg-rowid-order-scan-desc): Last + Prev + plain-scan-safe WHERE, no sorter.
        "SELECT * FROM t WHERE c = 5 ORDER BY id DESC",
        "SELECT * FROM t WHERE x = 'v3' ORDER BY id DESC LIMIT 20",
        "SELECT id FROM t WHERE a > 10 AND c < 8 ORDER BY id DESC LIMIT 5",
        "SELECT * FROM t WHERE a BETWEEN 3 AND 9 ORDER BY id DESC LIMIT 10",
        "SELECT * FROM t WHERE a < 5 ORDER BY id DESC",
    ];
    for sql in scans {
        cmp_ord(&f, &r, sql, "rowid-order-scan");
        assert!(
            !has_sorter(&f, sql),
            "ORDER BY rowid (scan-safe) must NOT open a sorter: `{sql}`"
        );
    }
    // NOT bypassed (still correct via the sorter): a subquery filter (declined by
    // where_is_plain_scan_safe) in either direction, or a non-rowid ORDER BY.
    let sorted = [
        "SELECT * FROM t WHERE a IN (SELECT 3) ORDER BY id", // IN (subquery) ASC -> declines
        "SELECT * FROM t WHERE a IN (SELECT 3) ORDER BY id DESC", // IN (subquery) DESC -> declines
        "SELECT * FROM t WHERE a > (SELECT 5) ORDER BY id",  // scalar subquery -> declines
        "SELECT * FROM t ORDER BY x LIMIT 10",               // non-rowid ORDER BY
        "SELECT * FROM t ORDER BY x DESC",                   // non-rowid DESC
    ];
    for sql in sorted {
        cmp_ord(&f, &r, sql, "still-sorts");
        assert!(
            has_sorter(&f, sql),
            "subquery / non-rowid ORDER BY should still sort: `{sql}`"
        );
    }
}
