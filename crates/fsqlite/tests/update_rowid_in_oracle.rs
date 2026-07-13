//! bd-update-rowid-in: `UPDATE ... WHERE <rowid> IN (<int literals>)` collects the listed rows with one
//! SeekRowid each (Pass 1) instead of full-scanning, then the unchanged Pass 2 applies the SET rewrite.
//! The resulting table state is compared byte-exact against C SQLite; the optimization is confirmed by
//! the ABSENCE of a `Rewind` (the full-scan marker) in the UPDATE plan.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;
fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(), SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"), SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
fn frank_state(c: &Connection) -> Vec<Vec<String>> {
    let mut r: Vec<Vec<String>> = c.query("SELECT id, a, c, x FROM t").unwrap().iter().map(|row| row.values().iter().map(render).collect()).collect();
    r.sort(); r
}
fn sqlite_state(c: &rusqlite::Connection) -> Vec<Vec<String>> {
    let mut stmt = c.prepare("SELECT id, a, c, x FROM t").unwrap();
    let mut r: Vec<Vec<String>> = stmt.query_map([], |row| {
        Ok((0..4).map(|i| match row.get_unwrap::<_, rusqlite::types::Value>(i) {
            rusqlite::types::Value::Null => "NULL".to_owned(), rusqlite::types::Value::Integer(x) => x.to_string(),
            rusqlite::types::Value::Real(f) => format!("{f:?}"), rusqlite::types::Value::Text(s) => format!("'{s}'"),
            rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
        }).collect::<Vec<_>>())
    }).unwrap().map(Result::unwrap).collect();
    r.sort(); r
}
fn has_op(c: &Connection, sql: &str, prefix: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with(prefix)))
}
fn fresh() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap(); let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in ["CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);", "CREATE INDEX idx_c ON t(c);"] {
        f.execute(s).unwrap(); r.execute_batch(s).unwrap();
    }
    for i in 1..=300_i64 {
        let s = format!("INSERT INTO t VALUES ({i}, {}, {}, 'v{}');", i % 20, i % 12, i % 7);
        f.execute(&s).unwrap(); r.execute_batch(&s).unwrap();
    }
    (f, r)
}
fn check_update(upd: &str, no_rewind: bool) {
    let (f, r) = fresh();
    if no_rewind {
        assert!(!has_op(&f, upd, "Rewind"), "rowid-IN UPDATE must not full-scan (Rewind): `{upd}`");
    } else {
        assert!(has_op(&f, upd, "Rewind"), "control UPDATE should full-scan: `{upd}`");
    }
    f.execute(upd).unwrap();
    r.execute_batch(upd).unwrap();
    assert_eq!(frank_state(&f), sqlite_state(&r), "state diverged after `{upd}`");
}
#[test]
fn update_rowid_in_matches_sqlite() {
    // rowid IN: SeekRowid loop, no Rewind, byte-exact resulting table.
    check_update("UPDATE t SET x = 'zz' WHERE id IN (5, 25, 45)", true);
    check_update("UPDATE t SET a = a + 100 WHERE id IN (99999, 5, 250)", true);   // one absent, expression SET
    check_update("UPDATE t SET x = 'q', a = 7 WHERE id IN (5, 5, 25)", true);      // duplicates, multi-assign
    check_update("UPDATE t SET c = c * 2 WHERE id IN (1)", true);                  // single value
    check_update("UPDATE t SET x = 'none' WHERE id IN (99999, 88888)", true);      // all absent -> no-op
    check_update("UPDATE t SET id = id + 10000 WHERE id IN (5, 25, 45)", true);    // updates the ROWID itself
    check_update("UPDATE t SET c = c + 1 WHERE id IN (1, 2, 3, 298, 299, 300)", true); // boundary rowids
    // bd-update-rowid-in-residual: `rowid IN (ints) AND <residual>` now seeks the listed rows and filters.
    check_update("UPDATE t SET x = 'r' WHERE id IN (5, 25, 45) AND c = 5", true);        // eq residual
    check_update("UPDATE t SET a = a + 1 WHERE id IN (10, 20, 30, 40) AND c > 3", true); // range residual + expr SET
    check_update("UPDATE t SET x = 'm' WHERE id IN (100, 200, 300) AND c != 5 AND x = 'v3'", true); // multi
    check_update("UPDATE t SET x = 'n' WHERE id IN (5, 25, 45) AND c = 999", true);      // residual matches nothing
    check_update("UPDATE t SET id = id + 5000 WHERE id IN (5, 25, 45) AND c = 5", true); // residual + ROWID rewrite
    // Control: a non-rowid IN must still full-scan (Rewind), and stay correct.
    check_update("UPDATE t SET x = 'ctl' WHERE a IN (3, 5)", false);               // a is not the rowid
}
