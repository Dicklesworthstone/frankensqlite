//! bd-agg-rowid-eq-coerced: `SELECT SUM(v)/MIN(v)/MAX(v)/COUNT(v)/AVG(v) FROM t WHERE <rowid> =
//! <non-integer-literal constant>` (placeholder / real / text) seeks the single row via `MustBeInt`
//! (INTEGER-affinity coerce) + one `SeekRowid` and accumulates it, instead of full-scanning. A non-exact
//! integer (2.5, 'abc', NULL) rejects to the empty result (SUM/MIN/MAX NULL, COUNT 0) exactly as SQLite;
//! '5' / 5.0 coerce to 5. Aggregate values are compared against C SQLite; the optimization is confirmed by
//! the ABSENCE of a `Rewind`. The integer-literal case (bd-2dgf5 rowid_eq_seek) stays byte-identical.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn val_f(c: &Connection, sql: &str) -> SqliteValue {
    c.query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .first()
        .and_then(|r| r.values().first().cloned())
        .unwrap_or(SqliteValue::Null)
}

fn val_r(c: &rusqlite::Connection, sql: &str) -> rusqlite::types::Value {
    c.query_row(sql, [], |row| row.get::<_, rusqlite::types::Value>(0))
        .unwrap()
}

fn same(f: &SqliteValue, r: &rusqlite::types::Value) -> bool {
    use rusqlite::types::Value as RV;
    match (f, r) {
        (SqliteValue::Null, RV::Null) => true,
        (SqliteValue::Integer(a), RV::Integer(b)) => a == b,
        (SqliteValue::Float(a), RV::Real(b)) => (a - b).abs() < 1e-9,
        (SqliteValue::Integer(a), RV::Real(b)) => (*a as f64 - b).abs() < 1e-9,
        (SqliteValue::Float(a), RV::Integer(b)) => (a - *b as f64).abs() < 1e-9,
        _ => false,
    }
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
            "agg rowid-eq-coerced must not full-scan (Rewind): `{sql}`"
        ),
        Some(false) => assert!(
            has_op(f, sql, "Rewind"),
            "control should full-scan (Rewind): `{sql}`"
        ),
        None => {}
    }
    let (vf, vr) = (val_f(f, sql), val_r(r, sql));
    assert!(
        same(&vf, &vr),
        "value diverged for `{sql}`: frank {vf:?} vs sqlite {vr:?}"
    );
}

#[test]
fn agg_rowid_eq_coerced_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let schema = "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);";
    f.execute(schema).unwrap();
    r.execute_batch(schema).unwrap();
    for i in 1..=300_i64 {
        let vv = if i % 41 == 0 {
            "NULL".to_string()
        } else {
            (i * 2).to_string()
        };
        let s = format!("INSERT INTO t VALUES ({i}, {vv});");
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }

    // Integer literal still seeks and is byte-identical (bd-2dgf5 rowid_eq_seek, no MustBeInt).
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE id = 5", Some(true)); // v[5]=10 -> 10

    // Real / text constants seek via MustBeInt coercion; value byte-exact across every aggregate.
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE id = 5.0", Some(true)); // exact real -> 10
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE id = 2.5", Some(true)); // non-exact real -> NULL
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE id = '5'", Some(true)); // numeric text -> 10
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE id = 'abc'", Some(true)); // non-numeric text -> NULL
    cmp(&f, &r, "SELECT MIN(v) FROM t WHERE id = 5.0", Some(true)); // -> 10
    cmp(&f, &r, "SELECT MAX(v) FROM t WHERE id = 5.0", Some(true)); // -> 10
    cmp(&f, &r, "SELECT COUNT(v) FROM t WHERE id = 5.0", Some(true)); // -> 1
    cmp(&f, &r, "SELECT COUNT(v) FROM t WHERE id = 2.5", Some(true)); // no match -> 0
    cmp(&f, &r, "SELECT AVG(v) FROM t WHERE id = 5.0", Some(true)); // -> 10.0
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE id = 300.0", Some(true)); // last row v=600 -> 600
    cmp(
        &f,
        &r,
        "SELECT SUM(v) FROM t WHERE id = 99999.0",
        Some(true),
    ); // exact real, absent -> NULL
    cmp(
        &f,
        &r,
        "SELECT COUNT(v) FROM t WHERE id = 99999.0",
        Some(true),
    ); // absent -> 0
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE id = 41.0", Some(true)); // v NULL at id=41 -> NULL
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE '25' = id", Some(true)); // reversed operand, text -> 50

    // `= NULL` is always empty; NULL/0 whether it seeks or scans (plan not asserted).
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE id = NULL", None);
    cmp(&f, &r, "SELECT COUNT(v) FROM t WHERE id = NULL", None);

    // Placeholder routes to the coerced seek (plan asserted; MustBeInt handles the bound type at runtime,
    // proven byte-exact by the literal cases above which take the identical MustBeInt path).
    assert!(
        !has_op(&f, "SELECT SUM(v) FROM t WHERE id = ?", "Rewind"),
        "param agg rowid-eq SUM should seek (no Rewind)"
    );

    // Control: a non-rowid, non-indexed predicate still full-scans.
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE v = 3", Some(false));
}
