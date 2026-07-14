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
fn explain_ops(c: &Connection, sql: &str) -> Vec<String> {
    c.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .map(|row| match row.values().get(1) {
            Some(SqliteValue::Text(opcode)) => opcode.to_string(),
            other => panic!("EXPLAIN opcode column was not text: {other:?}"),
        })
        .collect()
}

fn op_count(ops: &[String], opcode: &str) -> usize {
    ops.iter().filter(|op| op.as_str() == opcode).count()
}

fn assert_range_plan(c: &Connection, sql: &str, expected_range_ops: Option<(&str, Option<&str>)>) {
    let ops = explain_ops(c, sql);
    assert_eq!(op_count(&ops, "RowSetRead"), 1, "plan: {ops:?}");
    assert_eq!(op_count(&ops, "RowSetAdd"), 1, "plan: {ops:?}");
    assert_eq!(op_count(&ops, "Next"), 1, "plan: {ops:?}");

    let rowset_read = ops
        .iter()
        .position(|op| op == "RowSetRead")
        .expect("the exact RowSetRead count was checked above");
    assert_eq!(
        ops.get(rowset_read + 1).map(String::as_str),
        Some("SeekRowid"),
        "Pass 2 must reseek each collected rowid immediately after RowSetRead: {ops:?}"
    );

    if let Some((lower_seek, upper_stop)) = expected_range_ops {
        assert_eq!(op_count(&ops, "Rewind"), 0, "plan: {ops:?}");
        assert_eq!(
            op_count(&ops, "SeekGE") + op_count(&ops, "SeekGT"),
            1,
            "range collection must have exactly one lower seek: {ops:?}"
        );
        assert_eq!(op_count(&ops, lower_seek), 1, "plan: {ops:?}");
        match upper_stop {
            Some(stop) => {
                assert_eq!(
                    op_count(&ops, "Gt") + op_count(&ops, "Ge"),
                    1,
                    "bounded range must have exactly one upper stop: {ops:?}"
                );
                assert_eq!(op_count(&ops, stop), 1, "plan: {ops:?}");
            }
            None => {
                assert_eq!(op_count(&ops, "Gt"), 0, "plan: {ops:?}");
                assert_eq!(op_count(&ops, "Ge"), 0, "plan: {ops:?}");
            }
        }
    } else {
        assert_eq!(op_count(&ops, "Rewind"), 1, "plan: {ops:?}");
        assert_eq!(op_count(&ops, "SeekGE"), 0, "plan: {ops:?}");
        assert_eq!(op_count(&ops, "SeekGT"), 0, "plan: {ops:?}");
    }
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

fn assert_integrity(f: &Connection, r: &rusqlite::Connection) {
    let frank = f.query("PRAGMA integrity_check").unwrap();
    assert!(matches!(
        frank.first().and_then(|row| row.values().first()),
        Some(SqliteValue::Text(result)) if result.to_string() == "ok"
    ));
    let sqlite: String = r
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(sqlite, "ok");
}

fn check(dml: &str, expected_range_ops: Option<(&str, Option<&str>)>) {
    let (f, r) = fresh();
    assert_range_plan(&f, dml, expected_range_ops);
    f.execute(dml).unwrap();
    r.execute_batch(dml).unwrap();
    assert_eq!(
        frank_state(&f),
        sqlite_state(&r),
        "state diverged after `{dml}`"
    );
    assert_integrity(&f, &r);
}

fn frank_returning(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = c
        .query(sql)
        .unwrap()
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    rows.sort();
    rows
}

fn sqlite_returning(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let width = stmt.column_count();
    let mut rows: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            Ok((0..width)
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
    rows.sort();
    rows
}
#[test]
fn dml_rowid_range_matches_sqlite() {
    // DELETE: LOWER-bounded range -> SeekGE/SeekGT + walk, no Rewind, byte-exact resulting table.
    check(
        "DELETE FROM t WHERE id BETWEEN 50 AND 100",
        Some(("SeekGE", Some("Gt"))),
    );
    check("DELETE FROM t WHERE id > 250", Some(("SeekGT", None))); // lower-only, exclusive
    check(
        "DELETE FROM t WHERE id >= 100 AND id <= 150",
        Some(("SeekGE", Some("Gt"))),
    ); // both inclusive
    check(
        "DELETE FROM t WHERE id > 100 AND id < 110",
        Some(("SeekGT", Some("Ge"))),
    ); // both exclusive
    check(
        "DELETE FROM t WHERE id BETWEEN 999 AND 1099",
        Some(("SeekGE", Some("Gt"))),
    ); // lower-bounded empty range (all absent)
    check("DELETE FROM t WHERE id >= 298", Some(("SeekGE", None))); // near the end
    check(
        "DELETE FROM t AS q WHERE 50 <= q.id AND 60 > q.rowid",
        Some(("SeekGE", Some("Ge"))),
    ); // reversed operands and alias-qualified IPK/rowid references
    check(
        "DELETE FROM t WHERE id >= 200 AND id < 100",
        Some(("SeekGE", Some("Ge"))),
    ); // contradictory bounds are an exact empty bounded walk

    // UPDATE: same lower-bounded range shapes, incl. a SET that rewrites the rowid.
    check(
        "UPDATE t SET x = 'r' WHERE id BETWEEN 50 AND 100",
        Some(("SeekGE", Some("Gt"))),
    );
    check(
        "UPDATE t SET a = a + 1 WHERE id > 250",
        Some(("SeekGT", None)),
    );
    check(
        "UPDATE t SET c = c * 2 WHERE id >= 100 AND id <= 150",
        Some(("SeekGE", Some("Gt"))),
    );
    check(
        "UPDATE t SET id = id + 10000 WHERE id BETWEEN 60 AND 62",
        Some(("SeekGE", Some("Gt"))),
    ); // rewrites the ROWID
    check(
        "UPDATE t SET x = 'e' WHERE id BETWEEN 999 AND 1099",
        Some(("SeekGE", Some("Gt"))),
    ); // empty range -> no-op

    // Controls: UPPER-ONLY ranges (no lower bound to seek to) decline to the Rewind walk this cut, as do
    // a non-rowid range, an unindexed residual conjunction, parameter bounds, and unary-negative
    // bounds. Use `a` for the residual control: `c` has an index and is intentionally eligible for
    // the independent indexed-equality DML lane.
    check("DELETE FROM t WHERE id < 20", None); // upper-only -> Rewind
    check("DELETE FROM t WHERE id <= 3", None); // upper-only
    check("DELETE FROM t WHERE a BETWEEN 5 AND 10", None); // a is not the rowid
    check("DELETE FROM t WHERE id BETWEEN 50 AND 100 AND a = 5", None); // residual -> not bare range
    check(
        "UPDATE t SET x = 'c' WHERE id BETWEEN 50 AND 100 AND a = 5",
        None,
    );
    check("UPDATE t SET x = 'neg' WHERE id > -2", None); // unary-negative lower bound

    // Parameter bounds decline the literal-only lane, while SET/WHERE slots and affected counts remain
    // byte-for-byte compatible with SQLite.
    let (f, r) = fresh();
    let parameterized = "UPDATE t SET x = ? WHERE id >= ? AND id < ?";
    assert_range_plan(&f, parameterized, None);
    let f_changed = f
        .execute_with_params(
            parameterized,
            &[
                SqliteValue::Text("bound".into()),
                SqliteValue::Integer(10),
                SqliteValue::Integer(14),
            ],
        )
        .unwrap();
    let r_changed = r
        .execute(parameterized, rusqlite::params!["bound", 10_i64, 14_i64])
        .unwrap();
    assert_eq!(f_changed, r_changed);
    assert_eq!(frank_state(&f), sqlite_state(&r));
    assert_integrity(&f, &r);

    // DELETE RETURNING reads the old row image in Pass 2; the bounded collector must not alter either
    // the returned set or the final table/index state.
    let (f, r) = fresh();
    let returning = "DELETE FROM t WHERE id BETWEEN 40 AND 44 RETURNING id, c, x";
    assert_range_plan(&f, returning, Some(("SeekGE", Some("Gt"))));
    assert_eq!(
        frank_returning(&f, returning),
        sqlite_returning(&r, returning)
    );
    assert_eq!(frank_state(&f), sqlite_state(&r));
    assert_integrity(&f, &r);
}
