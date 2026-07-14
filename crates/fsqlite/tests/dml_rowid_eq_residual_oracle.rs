//! bd-delete-rowid-eq-residual / bd-update-rowid-eq-residual: `DELETE|UPDATE ... WHERE <rowid> = <const>
//! AND <residual>` seeks the single target row and applies the residual (the compare-and-delete /
//! optimistic-lock `WHERE id = ? AND version = ?` shape) instead of full-scanning. The resulting table
//! state is compared byte-exact against C SQLite; the optimization is confirmed by the ABSENCE of a
//! `Rewind` in the plan.
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
            "rowid-eq-residual DML must not full-scan (Rewind): `{dml}`"
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
fn dml_rowid_eq_residual_matches_sqlite() {
    // DELETE: single SeekRowid + residual, no Rewind, byte-exact resulting table.
    check("DELETE FROM t WHERE id = 25 AND c = 1", true); // residual matches -> deletes
    check("DELETE FROM t WHERE id = 25 AND c = 999", true); // residual fails -> deletes nothing
    check("DELETE FROM t WHERE id = 99999 AND c = 5", true); // rowid miss -> deletes nothing
    check("DELETE FROM t WHERE id = 40 AND c > 3", true); // range residual
    check("DELETE FROM t WHERE id = 24 AND c != 5 AND x = 'v3'", true); // multi-conjunct residual
    check("DELETE FROM t WHERE 13 = id AND c = 1", true); // rowid on the RHS
    check("DELETE FROM t WHERE id = 17 AND a IS NULL", true); // 17 -> a = 17 (not null) -> no delete
    // UPDATE: the optimistic-lock shape `WHERE id = ? AND version = ?`.
    check("UPDATE t SET x = 'r' WHERE id = 25 AND c = 1", true); // matches -> updates
    check("UPDATE t SET x = 'r' WHERE id = 25 AND c = 999", true); // residual fails -> no update
    check("UPDATE t SET a = a + 1 WHERE id = 99999 AND c = 5", true); // rowid miss -> no update
    check(
        "UPDATE t SET c = c + 1 WHERE id = 50 AND c BETWEEN 2 AND 8",
        true,
    ); // range residual
    check("UPDATE t SET id = id + 5000 WHERE id = 60 AND c = 0", true); // ROWID rewrite + residual
    // Controls: predicates over two unindexed, non-rowid columns must still
    // full-scan (Rewind), and stay correct. `c` is deliberately excluded here
    // because it has an index and belongs to the indexed-equality DML lane.
    check("DELETE FROM t WHERE a = 5 AND x = 'v1'", false);
    check("UPDATE t SET x = 'c' WHERE a = 5 AND x = 'v1'", false);
}
