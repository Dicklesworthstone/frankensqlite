//! bd-delete-index-eq / bd-update-index-eq: `DELETE|UPDATE ... WHERE <single-col-ASC-integer-indexed> =
//! <int literal>` collects the matching rowids with an index seek (fresh Pass-1 read cursor) instead of
//! full-scanning. The resulting table state is compared byte-exact against C SQLite; the optimization is
//! confirmed by the ABSENCE of a `Rewind`. NULLs in the indexed column verify the seek skips them.
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
        let cc = if i % 13 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 12)
        }; // NULLs sprinkled in the indexed col
        let s = format!(
            "INSERT INTO t VALUES ({i}, {}, {}, 'v{}');",
            i % 20,
            cc,
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
            "index-eq DML must not full-scan (Rewind): `{dml}`"
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
fn dml_index_eq_matches_sqlite() {
    // DELETE: index seek, no Rewind, byte-exact resulting table (must not touch the NULL-c rows).
    check("DELETE FROM t WHERE c = 5", true); // ~25 matches
    check("DELETE FROM t WHERE c = 0", true); // many matches
    check("DELETE FROM t WHERE c = 999", true); // zero matches -> deletes nothing
    check("DELETE FROM t WHERE 7 = c", true); // value on the LHS
    // UPDATE: including updates that rewrite the indexed column and the rowid.
    check("UPDATE t SET x = 'z' WHERE c = 5", true);
    check("UPDATE t SET a = a + 1 WHERE c = 0", true);
    check("UPDATE t SET c = c + 100 WHERE c = 3", true); // rewrites the INDEXED column itself
    check("UPDATE t SET id = id + 5000 WHERE c = 7", true); // rewrites the ROWID
    check("UPDATE t SET x = 'n' WHERE c = 999", true); // zero matches -> no-op
    // bd-*-index-eq-residual: `<indexed> = <int> AND <residual>` seeks the index and filters per candidate.
    check("DELETE FROM t WHERE c = 5 AND x = 'v3'", true); // eq residual on a non-indexed col
    check("DELETE FROM t WHERE c = 0 AND a > 10", true); // range residual
    check("DELETE FROM t WHERE c = 7 AND a != 5 AND x = 'v2'", true); // multi-conjunct residual
    check("DELETE FROM t WHERE c = 5 AND x = 'nope'", true); // residual matches nothing
    check("UPDATE t SET x = 'r' WHERE c = 5 AND a > 3", true); // update + residual
    check("UPDATE t SET c = c + 100 WHERE c = 3 AND a < 15", true); // indexed-col rewrite + residual
    // Controls: a non-indexed column eq must still full-scan (Rewind), and stay correct.
    check("DELETE FROM t WHERE a = 5", false); // a is not indexed
}
