//! Regression + coercion oracle for `codegen_select_rowid_lookup`: `SELECT <cols> FROM t WHERE <rowid> =
//! <non-integer-literal constant>` (placeholder / real / text). The lookup seeks the single row via one
//! SeekRowid; a non-exact key (2.5 / 'abc' / NULL) must reject to the EMPTY result (via MustBeInt coerce),
//! NOT be truncated to a wrong rowid (`id = 2.5` must not return row 2). '5' / 5.0 coerce to 5. Result rows
//! are compared against C SQLite; the seek is confirmed by the ABSENCE of a `Rewind`. The integer-literal
//! case stays byte-identical.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn rows_f(c: &Connection, sql: &str) -> Vec<i64> {
    c.query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|r| match r.values().first() {
            Some(SqliteValue::Integer(n)) => *n,
            other => panic!("expected integer col, got {other:?} for `{sql}`"),
        })
        .collect()
}

fn rows_r(c: &rusqlite::Connection, sql: &str) -> Vec<i64> {
    let mut stmt = c.prepare(sql).unwrap();
    let out = stmt.query_map([], |row| row.get::<_, i64>(0)).unwrap();
    out.map(Result::unwrap).collect()
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
            "rowid-eq-coerced lookup must not full-scan (Rewind): `{sql}`"
        ),
        Some(false) => assert!(
            has_op(f, sql, "Rewind"),
            "control should full-scan (Rewind): `{sql}`"
        ),
        None => {}
    }
    assert_eq!(rows_f(f, sql), rows_r(r, sql), "rows diverged for `{sql}`");
}

#[test]
fn nonagg_rowid_eq_coerced_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let schema = "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER);";
    f.execute(schema).unwrap();
    r.execute_batch(schema).unwrap();
    for i in 1..=300_i64 {
        let s = format!("INSERT INTO t VALUES ({i}, {});", i * 10);
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }

    // Integer literal still seeks and is byte-identical (no MustBeInt).
    cmp(&f, &r, "SELECT x FROM t WHERE id = 5", Some(true)); // -> [50]

    // Real / text constants seek via MustBeInt coercion; result byte-exact.
    cmp(&f, &r, "SELECT x FROM t WHERE id = 5.0", Some(true)); // exact real -> [50]
    cmp(&f, &r, "SELECT x FROM t WHERE id = 2.5", Some(true)); // non-exact real -> [] (was wrongly [20])
    cmp(&f, &r, "SELECT x FROM t WHERE id = '5'", Some(true)); // numeric text -> [50]
    cmp(&f, &r, "SELECT x FROM t WHERE id = 'abc'", Some(true)); // non-numeric text -> []
    cmp(&f, &r, "SELECT x FROM t WHERE id = 300.0", Some(true)); // last row -> [3000]
    cmp(&f, &r, "SELECT x FROM t WHERE id = 99999.0", Some(true)); // exact real, absent -> []
    cmp(&f, &r, "SELECT x FROM t WHERE id = 0.5", Some(true)); // below first rowid -> []
    cmp(&f, &r, "SELECT x FROM t WHERE '25' = id", Some(true)); // reversed operand, text -> [250]

    // `= NULL` is always empty; plan not asserted.
    cmp(&f, &r, "SELECT x FROM t WHERE id = NULL", None);

    // Placeholder routes to the coerced seek (plan asserted; MustBeInt handles the bound type at runtime,
    // proven byte-exact by the literal cases above which take the identical MustBeInt path).
    assert!(
        !has_op(&f, "SELECT x FROM t WHERE id = ?", "Rewind"),
        "param rowid-eq lookup should seek (no Rewind)"
    );

    // Control: a non-rowid predicate still full-scans.
    cmp(&f, &r, "SELECT x FROM t WHERE x = 30", Some(false)); // x[3]=30 -> [30]
}
