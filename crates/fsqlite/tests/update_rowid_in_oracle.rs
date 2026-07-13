//! bd-update-rowid-in: `UPDATE ... WHERE <rowid> IN (<int literals>)` collects the listed rows with one
//! SeekRowid each (Pass 1) instead of full-scanning, then the unchanged Pass 2 applies the SET rewrite.
//! The resulting table state is compared byte-exact against C SQLite; the optimization is confirmed by
//! the ABSENCE of a `Rewind` (the full-scan marker) in the UPDATE plan.
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

fn assert_update_plan(c: &Connection, sql: &str, unique_seek_values: Option<usize>) {
    let ops = explain_ops(c, sql);
    assert_eq!(op_count(&ops, "RowSetRead"), 1, "plan: {ops:?}");
    match unique_seek_values {
        Some(unique_values) => {
            assert_eq!(op_count(&ops, "Rewind"), 0, "plan: {ops:?}");
            assert_eq!(op_count(&ops, "Next"), 0, "plan: {ops:?}");
            assert_eq!(op_count(&ops, "RowSetAdd"), unique_values, "plan: {ops:?}");
            let returning_reseek = usize::from(
                sql.split_ascii_whitespace()
                    .any(|token| token.eq_ignore_ascii_case("RETURNING")),
            );
            assert_eq!(
                op_count(&ops, "SeekRowid"),
                unique_values + 1 + returning_reseek,
                "one probe per unique literal, the Pass-2 apply seek, and the RETURNING re-seek when present: {ops:?}"
            );
        }
        None => {
            assert_eq!(op_count(&ops, "Rewind"), 1, "plan: {ops:?}");
            assert_eq!(op_count(&ops, "Next"), 1, "plan: {ops:?}");
            assert_eq!(op_count(&ops, "RowSetAdd"), 1, "plan: {ops:?}");
            assert_eq!(
                op_count(&ops, "SeekRowid"),
                1,
                "fallback must retain only the Pass-2 seek: {ops:?}"
            );
        }
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

fn check_update(upd: &str, unique_seek_values: Option<usize>) {
    let (f, r) = fresh();
    assert_update_plan(&f, upd, unique_seek_values);
    f.execute(upd).unwrap();
    r.execute_batch(upd).unwrap();
    assert_eq!(
        frank_state(&f),
        sqlite_state(&r),
        "state diverged after `{upd}`"
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
fn update_rowid_in_matches_sqlite() {
    // rowid IN: SeekRowid loop, no Rewind, byte-exact resulting table.
    check_update("UPDATE t SET x = 'zz' WHERE id IN (5, 25, 45)", Some(3));
    check_update(
        "UPDATE t SET a = a + 100 WHERE id IN (99999, 5, 250)",
        Some(3),
    ); // one absent, expression SET
    check_update(
        "UPDATE t SET x = 'q', a = 7 WHERE id IN (5, 5, 25)",
        Some(2),
    ); // duplicates, multi-assign
    check_update("UPDATE t SET c = c * 2 WHERE id IN (1)", Some(1)); // single value
    check_update(
        "UPDATE t SET x = 'none' WHERE id IN (99999, 88888)",
        Some(2),
    ); // all absent -> no-op
    check_update(
        "UPDATE t SET id = id + 10000 WHERE id IN (5, 25, 45)",
        Some(3),
    ); // updates the ROWID itself
    check_update(
        "UPDATE t SET c = c + 1 WHERE id IN (1, 2, 3, 298, 299, 300)",
        Some(6),
    ); // boundary rowids
    check_update(
        "UPDATE t AS q SET x = 'or' WHERE 45 = q.id OR q.rowid = 5 OR 25 = q.id",
        Some(3),
    ); // OR normalization, reversed operands, and alias-qualified rowid/IPK references

    // The residual lane added by d32a263a still performs one literal probe per rowid and filters each
    // candidate before RowSetAdd. A placeholder in the residual must retain its textual slot after SET.
    check_update(
        "UPDATE t SET x = 'ctl' WHERE id IN (5, 25, 45) AND c = 5",
        Some(3),
    );

    // Controls that are not exact integer-literal rowid sets must retain the filtered scan.
    check_update("UPDATE t SET x = 'ctl' WHERE a IN (3, 5)", None); // a is not the rowid
    check_update("UPDATE t SET x = 'ctl' WHERE id IN (-1, 5)", None); // unary-negative list member
    check_update("UPDATE t SET x = 'ctl' WHERE id IN (NULL, 5)", None); // NULL list member
    check_update("UPDATE t SET x = 'ctl' WHERE id NOT IN (5, 25)", None); // NOT IN

    // A parameterized list declines the literal-only optimization without disturbing textual placeholder
    // order (SET first, then WHERE) or the affected-row count.
    let (f, r) = fresh();
    let parameterized = "UPDATE t SET x = ? WHERE id IN (?, ?)";
    assert_update_plan(&f, parameterized, None);
    let f_changed = f
        .execute_with_params(
            parameterized,
            &[
                SqliteValue::Text("bound".into()),
                SqliteValue::Integer(5),
                SqliteValue::Integer(25),
            ],
        )
        .unwrap();
    let r_changed = r
        .execute(parameterized, rusqlite::params!["bound", 5_i64, 25_i64])
        .unwrap();
    assert_eq!(f_changed, r_changed);
    assert_eq!(frank_state(&f), sqlite_state(&r));
    assert_integrity(&f, &r);

    // A residual parameter is evaluated after each direct rowid probe. Its placeholder must still
    // use the WHERE-expression slot rather than inheriting the SET-expression offset.
    let (f, r) = fresh();
    let residual_parameterized = "UPDATE t SET x = ? WHERE id IN (5, 25, 45) AND c = ?";
    assert_update_plan(&f, residual_parameterized, Some(3));
    let f_changed = f
        .execute_with_params(
            residual_parameterized,
            &[
                SqliteValue::Text("residual".into()),
                SqliteValue::Integer(5),
            ],
        )
        .unwrap();
    let r_changed = r
        .execute(residual_parameterized, rusqlite::params!["residual", 5_i64])
        .unwrap();
    assert_eq!(f_changed, r_changed);
    assert_eq!(frank_state(&f), sqlite_state(&r));
    assert_integrity(&f, &r);

    // RETURNING remains in Pass 2 and must expose rewritten values for exactly the listed rows.
    let (f, r) = fresh();
    let returning =
        "UPDATE t SET a = a + 1000, x = 'ret' WHERE id IN (5, 25, 45) RETURNING id, a, x";
    assert_update_plan(&f, returning, Some(3));
    assert_eq!(
        frank_returning(&f, returning),
        sqlite_returning(&r, returning)
    );
    assert_eq!(frank_state(&f), sqlite_state(&r));
    assert_integrity(&f, &r);
}
