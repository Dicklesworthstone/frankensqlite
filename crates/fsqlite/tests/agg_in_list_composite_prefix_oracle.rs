//! bd-in-list-composite-prefix-probe: `SELECT COUNT(*)/SUM(v) FROM t WHERE a IN (<int list>)` seeks the
//! ASCENDING LEADING column of a composite `(a, b)` index via a 1-field PREFIX probe `[value]` instead of
//! full-scanning — including `a=value, b=NULL` rows (which the old 2-field `[value, i64::MIN]` probe would
//! skip, since a NULL trailing column sorts before i64::MIN). Byte-exact vs C SQLite; the seek is confirmed
//! by the ABSENCE of a `Rewind`. A single-column index keeps the exact 2-field probe (unchanged).
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
            "composite-prefix IN seek must not full-scan (Rewind): `{sql}`"
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
fn agg_in_list_composite_prefix_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    // Composite (a, b) index, NO single-column idx_a -> a IN (list) must use the composite leading column.
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, v INTEGER);",
        "CREATE INDEX idx_ab ON t(a, b);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=400_i64 {
        let a = i % 20; // 20 distinct a-values (0..19)
        // a==3 rows ALWAYS have b=NULL (the pure fix case); plus a scattering of other NULL b.
        let b = if a == 3 || i % 11 == 0 {
            "NULL".to_string()
        } else {
            (i % 5).to_string()
        };
        let s = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {});", i * 2);
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }

    // a=3 has ONLY b=NULL rows -> the fix is required to count/sum them at all.
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE a IN (3)", Some(true));
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE a IN (3)", Some(true));
    // Mixed trailing b (some NULL, some not) across several a-values.
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 0)",
        Some(true),
    );
    cmp(
        &f,
        &r,
        "SELECT SUM(v) FROM t WHERE a IN (3, 7, 0)",
        Some(true),
    );
    // Duplicate IN values must not double-count.
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE a IN (5, 5, 7)",
        Some(true),
    );
    cmp(
        &f,
        &r,
        "SELECT SUM(v) FROM t WHERE a IN (5, 5, 7)",
        Some(true),
    );
    // No-match list.
    cmp(
        &f,
        &r,
        "SELECT COUNT(*) FROM t WHERE a IN (999, 1000)",
        Some(true),
    );
    cmp(&f, &r, "SELECT SUM(v) FROM t WHERE a IN (999)", Some(true));
    // Single value with many rows.
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE a IN (7)", Some(true));
    // MIN/MAX/AVG over the composite prefix seek.
    cmp(&f, &r, "SELECT MIN(v) FROM t WHERE a IN (3, 8)", Some(true));
    cmp(&f, &r, "SELECT MAX(v) FROM t WHERE a IN (3, 8)", Some(true));
    cmp(&f, &r, "SELECT AVG(v) FROM t WHERE a IN (7, 9)", Some(true));

    // Control: single-column index still seeks with the exact 2-field probe (byte-identical path).
    let f2 = Connection::open(":memory:").unwrap();
    let r2 = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE u (id INTEGER PRIMARY KEY, a INTEGER, v INTEGER);",
        "CREATE INDEX idx_a ON u(a);",
    ] {
        f2.execute(s).unwrap();
        r2.execute_batch(s).unwrap();
    }
    for i in 1..=300_i64 {
        let s = format!("INSERT INTO u VALUES ({i}, {}, {});", i % 20, i * 2);
        f2.execute(&s).unwrap();
        r2.execute_batch(&s).unwrap();
    }
    cmp(
        &f2,
        &r2,
        "SELECT COUNT(*) FROM u WHERE a IN (3, 7)",
        Some(true),
    );
    cmp(
        &f2,
        &r2,
        "SELECT SUM(v) FROM u WHERE a IN (3, 7)",
        Some(true),
    );

    // Control: a non-indexed predicate still full-scans.
    cmp(&f, &r, "SELECT COUNT(*) FROM t WHERE v = 100", Some(false));
}
