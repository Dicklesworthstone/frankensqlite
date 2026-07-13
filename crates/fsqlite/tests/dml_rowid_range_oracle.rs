//! bd-delete-rowid-range / bd-update-rowid-range: `DELETE|UPDATE ... WHERE <rowid> <range>` collects the
//! [lower, upper] slice with a bounded seek+walk in Pass 1 (SeekGE/SeekGT to the lower bound, stop past
//! the upper) instead of full-scanning. Integer-literal bounds only. The resulting table state is compared
//! byte-exact against C SQLite; the optimization is confirmed by the ABSENCE of a `Rewind` in the plan.
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
fn frank_state(c: &Connection) -> Vec<Vec<String>> {
    let mut r: Vec<Vec<String>> = c
        .query("SELECT id, a, c, x FROM t")
        .unwrap()
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    r.sort();
    r
}
fn sqlite_state(c: &rusqlite::Connection) -> Vec<Vec<String>> {
    let mut stmt = c.prepare("SELECT id, a, c, x FROM t").unwrap();
    let mut r: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            Ok((0..4)
                .map(|i| match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                    rusqlite::types::Value::Null => "NULL".to_owned(),
                    rusqlite::types::Value::Integer(x) => x.to_string(),
                    rusqlite::types::Value::Real(f) => format!("{f:?}"),
                    rusqlite::types::Value::Text(s) => format!("'{s}'"),
                    rusqlite::types::Value::Blob(b) => format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    ),
                })
                .collect::<Vec<_>>())
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    r.sort();
    r
}
fn has_op(c: &Connection, sql: &str, prefix: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with(prefix)))
}
fn fresh() -> (Connection, rusqlite::Connection) {
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
fn check(dml: &str, no_rewind: bool) {
    let (f, r) = fresh();
    if no_rewind {
        assert!(
            !has_op(&f, dml, "Rewind"),
            "rowid-range DML must not full-scan (Rewind): `{dml}`"
        );
    } else {
        assert!(
            has_op(&f, dml, "Rewind"),
            "control DML should full-scan: `{dml}`"
        );
    }
    f.execute(dml).unwrap();
    r.execute_batch(dml).unwrap();
    assert_eq!(
        frank_state(&f),
        sqlite_state(&r),
        "state diverged after `{dml}`"
    );
}
#[test]
fn dml_rowid_range_matches_sqlite() {
    // DELETE: LOWER-bounded range -> SeekGE/SeekGT + walk, no Rewind, byte-exact resulting table.
    check("DELETE FROM t WHERE id BETWEEN 50 AND 100", true);
    check("DELETE FROM t WHERE id > 250", true); // lower-only, exclusive
    check("DELETE FROM t WHERE id >= 100 AND id <= 150", true); // both inclusive
    check("DELETE FROM t WHERE id > 100 AND id < 110", true); // both exclusive
    check("DELETE FROM t WHERE id BETWEEN 999 AND 1099", true); // lower-bounded empty range (all absent)
    check("DELETE FROM t WHERE id >= 298", true); // near the end
    // UPDATE: same lower-bounded range shapes, incl. a SET that rewrites the rowid.
    check("UPDATE t SET x = 'r' WHERE id BETWEEN 50 AND 100", true);
    check("UPDATE t SET a = a + 1 WHERE id > 250", true);
    check("UPDATE t SET c = c * 2 WHERE id >= 100 AND id <= 150", true);
    check(
        "UPDATE t SET id = id + 10000 WHERE id BETWEEN 60 AND 62",
        true,
    ); // rewrites the ROWID
    check("UPDATE t SET x = 'e' WHERE id BETWEEN 999 AND 1099", true); // empty range -> no-op
    // Controls: UPPER-ONLY ranges (no lower bound to seek to) decline to the Rewind walk this cut, as do
    // a non-rowid range and a residual conjunction; all stay correct.
    check("DELETE FROM t WHERE id < 20", false); // upper-only -> Rewind + early stop
    check("DELETE FROM t WHERE id <= 3", false); // upper-only
    check("DELETE FROM t WHERE a BETWEEN 5 AND 10", false); // a is not the rowid
    check("DELETE FROM t WHERE id BETWEEN 50 AND 100 AND c = 5", false); // residual -> not bare range
    check(
        "UPDATE t SET x = 'c' WHERE id BETWEEN 50 AND 100 AND c = 5",
        false,
    );
}
