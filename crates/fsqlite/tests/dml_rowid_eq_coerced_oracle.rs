//! Regression + coercion oracle for DELETE/UPDATE Pass-1 rowid-eq: `DELETE/UPDATE ... WHERE <rowid> =
//! <non-integer-literal constant>` (placeholder / real / text). Pass 1 collects the target row via one
//! SeekRowid; a non-exact key (2.5 / 'abc' / NULL) must reject to the empty collection (via MustBeInt
//! coerce) so NOTHING is mutated, NOT be truncated to a wrong rowid (`id = 2.5` must not delete/update
//! row 2). '5' / 5.0 coerce to 5. Resulting table state is compared against C SQLite. The integer-literal
//! case stays byte-identical.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn setup(f: &Connection, r: &rusqlite::Connection) {
    for c in [
        "DROP TABLE IF EXISTS t;",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER);",
    ] {
        f.execute(c).unwrap();
        r.execute_batch(c).unwrap();
    }
    for i in 1..=50_i64 {
        let s = format!("INSERT INTO t VALUES ({i}, {});", i * 10);
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
}

fn state_f(c: &Connection) -> Vec<(i64, i64)> {
    c.query("SELECT id, x FROM t ORDER BY id")
        .unwrap()
        .iter()
        .map(|row| {
            let v = row.values();
            match (v.first(), v.get(1)) {
                (Some(SqliteValue::Integer(a)), Some(SqliteValue::Integer(b))) => (*a, *b),
                other => panic!("unexpected row {other:?}"),
            }
        })
        .collect()
}

fn state_r(c: &rusqlite::Connection) -> Vec<(i64, i64)> {
    let mut stmt = c.prepare("SELECT id, x FROM t ORDER BY id").unwrap();
    let out = stmt
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap();
    out.map(Result::unwrap).collect()
}

fn has_op(c: &Connection, sql: &str, prefix: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with(prefix)))
}

/// Apply `dml` to both engines from an identical fresh table, then assert the resulting state matches.
fn check(f: &Connection, r: &rusqlite::Connection, dml: &str, no_rewind: Option<bool>) {
    setup(f, r);
    match no_rewind {
        Some(true) => assert!(
            !has_op(f, dml, "Rewind"),
            "rowid-eq DML must not full-scan Pass 1 (Rewind): `{dml}`"
        ),
        Some(false) => assert!(
            has_op(f, dml, "Rewind"),
            "control should full-scan (Rewind): `{dml}`"
        ),
        None => {}
    }
    f.execute(dml)
        .unwrap_or_else(|e| panic!("frank `{dml}`: {e}"));
    r.execute_batch(dml).unwrap();
    assert_eq!(state_f(f), state_r(r), "table state diverged after `{dml}`");
}

#[test]
fn dml_rowid_eq_coerced_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();

    // DELETE: integer literal (byte-identical), exact real, NON-exact real (the fix), text, absent, NULL.
    check(&f, &r, "DELETE FROM t WHERE id = 5", Some(true)); // deletes row 5
    check(&f, &r, "DELETE FROM t WHERE id = 5.0", Some(true)); // exact real -> deletes row 5
    check(&f, &r, "DELETE FROM t WHERE id = 2.5", Some(true)); // non-exact real -> deletes NOTHING (was row 2)
    check(&f, &r, "DELETE FROM t WHERE id = '10'", Some(true)); // numeric text -> deletes row 10
    check(&f, &r, "DELETE FROM t WHERE id = 'abc'", Some(true)); // non-numeric text -> deletes nothing
    check(&f, &r, "DELETE FROM t WHERE id = 99999.0", Some(true)); // absent -> nothing
    check(&f, &r, "DELETE FROM t WHERE id = NULL", None); // NULL -> nothing
    check(&f, &r, "DELETE FROM t WHERE id = 3.5", Some(true)); // non-exact -> nothing

    // UPDATE: same coercion cases; the SET rewrites x so a wrong row would corrupt state.
    check(
        &f,
        &r,
        "UPDATE t SET x = x + 1000 WHERE id = 20",
        Some(true),
    ); // int literal
    check(
        &f,
        &r,
        "UPDATE t SET x = x + 1000 WHERE id = 20.0",
        Some(true),
    ); // exact real -> row 20
    check(
        &f,
        &r,
        "UPDATE t SET x = x + 1000 WHERE id = 2.5",
        Some(true),
    ); // non-exact -> nothing (was row 2)
    check(
        &f,
        &r,
        "UPDATE t SET x = x + 1000 WHERE id = '30'",
        Some(true),
    ); // numeric text -> row 30
    check(
        &f,
        &r,
        "UPDATE t SET x = x + 1000 WHERE id = 'xyz'",
        Some(true),
    ); // non-numeric -> nothing
    // Rowid-rewriting UPDATE (Halloween safety) with a coerced key.
    check(
        &f,
        &r,
        "UPDATE t SET id = id + 10000 WHERE id = 7.0",
        Some(true),
    ); // exact real -> rewrite row 7

    // Control: a non-rowid predicate still full-scans.
    check(&f, &r, "DELETE FROM t WHERE x = 100", Some(false)); // x[10]=100 -> deletes row 10
}
