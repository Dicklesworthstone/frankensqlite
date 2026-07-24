//! bd-count-rowid-eq-coerced: `SELECT COUNT(*) FROM t WHERE <rowid> = <non-integer-literal constant>`
//! (placeholder / real / text) counts the single row via `MustBeInt` (INTEGER-affinity coerce) + one
//! SeekRowid instead of full-scanning. A non-exact integer (2.5, 'abc', NULL) rejects to count 0 exactly
//! as SQLite; '5' / 5.0 coerce to 5. Counts are compared against C SQLite; the optimization is confirmed
//! by the ABSENCE of a `Rewind`. The integer-literal case (bd-count-rowid-eq) is regressed too.
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
            "rowid-eq-coerced COUNT must not full-scan (Rewind): `{sql}`"
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
fn count_rowid_eq_coerced_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let schema = "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER);";
    f.execute(schema).unwrap();
    r.execute_batch(schema).unwrap();
    for i in 1..=300_i64 {
        let s = format!("INSERT INTO t VALUES ({i}, {});", i % 10);
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }

    // Integer literal still seeks (regression: bd-count-rowid-eq, no MustBeInt).
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id = 5", Some(true)); // -> 1

    // Real / text constants seek via MustBeInt coercion, count byte-exact.
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id = 5.0", Some(true)); // exact real -> 1
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id = 2.5", Some(true)); // non-exact real -> 0
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id = '5'", Some(true)); // numeric text -> 1
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE id = 'abc'",
        Some(true),
    ); // non-numeric text -> 0
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE id = 99999.0",
        Some(true),
    ); // exact real, absent -> 0
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE id = 300.0",
        Some(true),
    ); // last row -> 1
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE '25' = id", Some(true)); // reversed operand, text -> 1

    // `= NULL` is always empty; count 0 whether it seeks or scans (plan not asserted).
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE id = NULL", None);

    // Placeholder routes to the coerced seek (plan asserted; MustBeInt handles the bound type at runtime).
    assert!(
        !has_op(&f, "SELECT COUNT(*) FROM t WHERE id = ?", "Rewind"),
        "param rowid-eq COUNT should seek (no Rewind)"
    );

    // Control: a non-rowid predicate still full-scans.
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE x = 3", Some(false));
}
