//! bd-count-rowid-eq: `SELECT COUNT(*) FROM t WHERE <rowid> = <int literal>` counts the single row (0 or
//! 1) with one SeekRowid instead of full-scanning. The rowid (IPK) has no secondary index, so this shape
//! is NOT diverted to the aggregate index-eq seek (bd-2dgf5) — it reaches codegen_select_count_star and
//! would otherwise Rewind. Counts are compared against C SQLite; the optimization is confirmed by the
//! ABSENCE of a `Rewind` in the plan. Non-integer constants and placeholders use the adjacent
//! MustBeInt-coerced rowid path; only a non-rowid predicate correctly declines.
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
    c.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with(prefix)))
}

fn cmp(f: &Connection, r: &rusqlite::Connection, sql: &str, no_rewind: Option<bool>) {
    match no_rewind {
        Some(true) => assert!(
            !has_op(f, sql, "Rewind"),
            "rowid-eq COUNT must not full-scan (Rewind): `{sql}`"
        ),
        Some(false) => assert!(
            has_op(f, sql, "Rewind"),
            "control COUNT should full-scan (Rewind): `{sql}`"
        ),
        None => {}
    }
    assert_eq!(count_f(f, sql), count_r(r, sql), "count diverged: `{sql}`");
}

#[test]
fn count_rowid_eq_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, y INTEGER, z REAL);",
        "CREATE INDEX idx_y ON t(y);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=300_i64 {
        let s = format!("INSERT INTO t VALUES ({i}, {}, {}.5);", i % 20, i % 10);
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }

    // rowid = <int literal>: single SeekRowid, no Rewind, count matches (0 or 1).
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id = 5", Some(true)); // present -> 1
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id = 150", Some(true)); // present -> 1
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE id = 99999",
        Some(true),
    ); // absent -> 0
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id = 1", Some(true)); // first -> 1
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id = 300", Some(true)); // last -> 1
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE 5 = id", Some(true)); // reversed operand order -> 1

    // A REAL rowid literal uses the coerced seek: MustBeInt rejects 2.5 rather than truncating it to 2.
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id = 2.5", Some(true));
    // A placeholder also routes through MustBeInt at runtime; plan asserted only (no bind needed).
    assert!(
        !has_op(&f, "SELECT COUNT(*) FROM t WHERE id = ?", "Rewind"),
        "param rowid-eq COUNT must use the coerced seek"
    );
    // Control: a non-rowid indexed column is served by the aggregate seek (bd-2dgf5), NOT this lever —
    // only assert the count. A non-indexed usage would full-scan; z has no index.
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE z = 3.5", Some(false)); // non-indexed REAL -> full scan
}
