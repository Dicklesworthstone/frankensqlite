//! bd-count-rowid-in: `SELECT COUNT(*) FROM t WHERE <rowid> IN (<int literals>)` counts existing rows
//! with one SeekRowid per listed value instead of full-scanning. The count is compared against C SQLite;
//! the optimization is confirmed by the ABSENCE of a `Rewind` in the plan.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;
fn count_f(c: &Connection, sql: &str) -> i64 {
    let rows = c
        .query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"));
    match rows.first().and_then(|r| r.values().first()) {
        Some(SqliteValue::Integer(n)) => *n,
        other => panic!("expected integer count, got {other:?} for `{sql}`"),
    }
}
fn count_r(c: &rusqlite::Connection, sql: &str) -> i64 {
    c.query_row(sql, [], |row| row.get::<_, i64>(0)).unwrap()
}
fn has_op(c: &Connection, sql: &str, prefix: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with(prefix)))
}
fn setup() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);",
        "CREATE INDEX idx_c ON t(c);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=300_i64 {
        let s = format!(
            "INSERT INTO t VALUES ({i}, {}, {}, 'v{}');",
            i % 20,
            i % 12,
            i % 7
        );
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    (f, r)
}
fn cmp(f: &Connection, r: &rusqlite::Connection, sql: &str, no_rewind: Option<bool>) {
    match no_rewind {
        Some(true) => assert!(
            !has_op(f, sql, "Rewind"),
            "rowid-IN COUNT must not full-scan (Rewind): `{sql}`"
        ),
        Some(false) => assert!(
            has_op(f, sql, "Rewind"),
            "control COUNT should full-scan: `{sql}`"
        ),
        None => {} // plan not asserted (served by a different existing optimization) — only the count matters
    }
    assert_eq!(count_f(f, sql), count_r(r, sql), "count diverged: `{sql}`");
}
#[test]
fn count_rowid_in_matches_sqlite() {
    let (f, r) = setup();
    // rowid IN COUNT: SeekRowid loop, no Rewind, count matches.
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE id IN (5, 25, 45)",
        Some(true),
    ); // all present -> 3
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE id IN (99999, 5, 250)",
        Some(true),
    ); // one absent -> 2
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE id IN (5, 5, 25, 25, 45)",
        Some(true),
    ); // duplicates -> 3
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE id IN (99999, 88888)",
        Some(true),
    ); // all absent -> 0
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id IN (1)", Some(true)); // single -> 1
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE id IN (1, 2, 3, 298, 299, 300)",
        Some(true),
    ); // boundary -> 6
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE 7 = id OR id = 8",
        Some(true),
    ); // OR-of-rowid-eq (an IN)
    // Control: a non-rowid IN still full-scans (Rewind), and stays correct.
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE a IN (3, 5)",
        Some(false),
    ); // a is not the rowid
    // Regression: rowid IN + residual is served by an existing indexed path (not mine); verify the count.
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE id IN (5, 25, 45) AND c = 1",
        None,
    );
}
