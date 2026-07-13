//! bd-delete-rowid-in: `DELETE ... WHERE <rowid> IN (<int literals>)` collects the listed rows with one
//! SeekRowid each (Pass 1) instead of full-scanning the table, then the unchanged Pass 2 deletes them.
//! The resulting table state is compared byte-exact against C SQLite; the optimization is confirmed by
//! the ABSENCE of a `Rewind` (the full-scan marker) in the DELETE plan.
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
fn frank_state(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    let mut r: Vec<Vec<String>> = c
        .query(sql)
        .unwrap()
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    r.sort();
    r
}
fn sqlite_state(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
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
fn op_count(c: &Connection, sql: &str, opcode: &str) -> usize {
    c.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .filter(
            |row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.as_ref() == opcode),
        )
        .count()
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
fn check_delete(del: &str, unique_rowids: Option<usize>) {
    let (f, r) = fresh();
    if let Some(unique_rowids) = unique_rowids {
        assert!(
            !has_op(&f, del, "Rewind"),
            "rowid-IN DELETE must not full-scan (Rewind): `{del}`"
        );
        assert_eq!(
            op_count(&f, del, "RowSetAdd"),
            unique_rowids,
            "optimized DELETE must add each unique literal rowid exactly once: `{del}`"
        );
        assert_eq!(
            op_count(&f, del, "SeekRowid"),
            unique_rowids + 1,
            "optimized DELETE needs one probe per unique literal plus the shared Pass-2 seek: `{del}`"
        );
    } else {
        assert!(
            has_op(&f, del, "Rewind"),
            "control DELETE should full-scan: `{del}`"
        );
    }
    let f_affected = f.execute(del).unwrap();
    let r_affected = r.execute(del, []).unwrap();
    assert_eq!(
        f_affected as u64, r_affected as u64,
        "affected-row count diverged for `{del}`"
    );
    for (label, state_sql) in [
        ("table scan", "SELECT id, a, c, x FROM t"),
        (
            "forced secondary-index scan",
            "SELECT id, a, c, x FROM t INDEXED BY idx_c",
        ),
    ] {
        assert_eq!(
            frank_state(&f, state_sql),
            sqlite_state(&r, state_sql),
            "{label} state diverged after `{del}`"
        );
    }
    let f_integrity = f.query("PRAGMA integrity_check").unwrap();
    assert!(
        matches!(
            f_integrity.first().and_then(|row| row.values().first()),
            Some(SqliteValue::Text(result)) if result.as_ref() == "ok"
        ),
        "FrankenSQLite integrity_check failed after `{del}`: {f_integrity:?}"
    );
    let r_integrity: String = r
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        r_integrity, "ok",
        "SQLite integrity_check failed after `{del}`"
    );
}
#[test]
fn delete_rowid_in_matches_sqlite() {
    // rowid IN: SeekRowid loop, no Rewind, byte-exact resulting table.
    check_delete("DELETE FROM t WHERE id IN (5, 25, 45)", Some(3));
    check_delete("DELETE FROM t WHERE id IN (99999, 5, 250)", Some(3)); // one absent, two present
    check_delete("DELETE FROM t WHERE id IN (5, 5, 25, 25, 45)", Some(3)); // duplicates in list
    check_delete("DELETE FROM t WHERE id IN (1)", Some(1)); // single value
    check_delete("DELETE FROM t WHERE id IN (99999, 88888)", Some(2)); // all absent -> deletes nothing
    check_delete(
        "DELETE FROM t WHERE id IN (1, 2, 3, 298, 299, 300)",
        Some(6),
    );
    // The shared extractor also normalizes OR equalities, including rowid on the RHS.
    check_delete("DELETE FROM t WHERE id = 7 OR 8 = id OR id = 7", Some(2));

    // Residual rowid-IN plans still seek each listed rowid, then filter before RowSetAdd.
    check_delete("DELETE FROM t WHERE id IN (5, 25, 45) AND c = 5", Some(3));
    check_delete(
        "DELETE FROM t WHERE id IN (10, 20, 30, 40) AND c > 3",
        Some(4),
    );
    check_delete(
        "DELETE FROM t WHERE id IN (100, 200, 300) AND c != 5 AND x = 'v3'",
        Some(3),
    );
    check_delete("DELETE FROM t WHERE id IN (5, 25, 45) AND c = 999", Some(3));
    check_delete(
        "DELETE FROM t WHERE id IN (99999, 5, 25) AND c IS NOT NULL",
        Some(3),
    );

    // Controls that are not exact integer-literal rowid sets must retain the filtered scan.
    check_delete("DELETE FROM t WHERE a IN (3, 5)", None); // a is not the rowid
    check_delete("DELETE FROM t WHERE id = 5 OR a = 6", None); // mixed-column OR
    check_delete("DELETE FROM t WHERE id = 5 OR id > 7", None); // mixed-operator OR
    check_delete("DELETE FROM t WHERE id IN (-5, 2)", None); // negative literal
    check_delete("DELETE FROM t WHERE id IN (NULL, 5)", None); // NULL changes IN semantics
    check_delete("DELETE FROM t WHERE id IN (2.0, 5)", None); // non-integer literal
    check_delete("DELETE FROM t WHERE id NOT IN (5, 25)", None);
    check_delete(
        "DELETE FROM t WHERE id IN (SELECT id FROM t WHERE id < 3)",
        None,
    );
}
