//! bd-delete-index-eq / bd-update-index-eq (parameterized): `DELETE|UPDATE ... WHERE <single-col-int-
//! indexed> = ?1 [AND <residual>]` seeks the index for a PLACEHOLDER value. The probe is coerced to the
//! column's INTEGER affinity (`Opcode::Affinity`), so a runtime integer / real / text / null bound seeks
//! authoritatively — verified against C SQLite across bound value types via prepared execution.
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
        };
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
fn to_rusqlite(v: &SqliteValue) -> rusqlite::types::Value {
    match v {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}
// Run a parameterized DML on both engines and diff the resulting table state.
fn check_param(dml: &str, params: &[SqliteValue], no_rewind: bool) {
    let (f, r) = fresh();
    if no_rewind {
        assert!(
            !has_op(&f, dml, "Rewind"),
            "parameterized index-eq must not full-scan (Rewind): `{dml}`"
        );
    }
    f.execute_with_params(dml, params)
        .unwrap_or_else(|e| panic!("frank `{dml}` params {params:?}: {e}"));
    let rparams: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
    r.execute(dml, rusqlite::params_from_iter(rparams.iter()))
        .unwrap();
    assert_eq!(
        frank_state(&f),
        sqlite_state(&r),
        "state diverged after `{dml}` params {params:?}"
    );
}
#[test]
fn dml_index_eq_param_matches_sqlite() {
    let sv = |v: SqliteValue| v;
    // Bare `c = ?1` across bound value types — affinity coercion must make each seek authoritative.
    check_param(
        "DELETE FROM t WHERE c = ?1",
        &[sv(SqliteValue::Integer(5))],
        true,
    ); // integer -> c=5
    check_param(
        "DELETE FROM t WHERE c = ?1",
        &[sv(SqliteValue::Integer(0))],
        true,
    ); // many matches
    check_param(
        "DELETE FROM t WHERE c = ?1",
        &[sv(SqliteValue::Integer(999))],
        true,
    ); // no match
    check_param(
        "DELETE FROM t WHERE c = ?1",
        &[sv(SqliteValue::Float(5.0))],
        true,
    ); // real 5.0 -> 5
    check_param(
        "DELETE FROM t WHERE c = ?1",
        &[sv(SqliteValue::Float(5.5))],
        true,
    ); // real 5.5 -> no match
    check_param(
        "DELETE FROM t WHERE c = ?1",
        &[sv(SqliteValue::Text("5".into()))],
        true,
    ); // text '5' -> 5
    check_param(
        "DELETE FROM t WHERE c = ?1",
        &[sv(SqliteValue::Text("abc".into()))],
        true,
    ); // text 'abc' -> no match
    check_param("DELETE FROM t WHERE c = ?1", &[sv(SqliteValue::Null)], true); // NULL -> matches nothing
    // UPDATE with a parameterized eq (and one that rewrites the indexed column).
    check_param(
        "UPDATE t SET x = ?2 WHERE c = ?1",
        &[
            sv(SqliteValue::Integer(7)),
            sv(SqliteValue::Text("z".into())),
        ],
        true,
    );
    check_param(
        "UPDATE t SET c = ?2 WHERE c = ?1",
        &[sv(SqliteValue::Integer(3)), sv(SqliteValue::Integer(103))],
        true,
    );
    // Parameterized eq + residual: `c = ?1 AND a > ?2`.
    check_param(
        "DELETE FROM t WHERE c = ?1 AND a > ?2",
        &[sv(SqliteValue::Integer(5)), sv(SqliteValue::Integer(10))],
        true,
    );
    check_param(
        "UPDATE t SET x = 'r' WHERE c = ?1 AND a < ?2",
        &[sv(SqliteValue::Integer(0)), sv(SqliteValue::Integer(8))],
        true,
    );
}

// TEXT-affinity ('B') indexed column: the probe is coerced to TEXT affinity so `name = <int/text>` seeks
// like the full-scan filter. Only a BINARY collation is admitted (NOCASE declines).
fn fresh_text(collation: &str) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let ddl = [
        "CREATE TABLE ut (id INTEGER PRIMARY KEY, name TEXT, a INTEGER);".to_owned(),
        format!("CREATE INDEX idx_name ON ut(name{collation});"),
    ];
    for s in &ddl {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=300_i64 {
        // Half the rows get a 'n<k>' name, half a bare numeric-text '<k>' name (so an integer bound coerces
        // to text and can match), with a few NULLs.
        let name = if i % 11 == 0 {
            "NULL".to_owned()
        } else if i % 2 == 0 {
            format!("'n{}'", i % 20)
        } else {
            format!("'{}'", i % 20)
        };
        let s = format!("INSERT INTO ut VALUES ({i}, {name}, {});", i % 7);
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    (f, r)
}
fn ut_state_f(c: &Connection) -> Vec<Vec<String>> {
    let mut r: Vec<Vec<String>> = c
        .query("SELECT id, name, a FROM ut")
        .unwrap()
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    r.sort();
    r
}
fn ut_state_r(c: &rusqlite::Connection) -> Vec<Vec<String>> {
    let mut stmt = c.prepare("SELECT id, name, a FROM ut").unwrap();
    let mut r: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            Ok((0..3)
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
fn check_text(collation: &str, dml: &str, params: &[SqliteValue], no_rewind: bool) {
    let (f, r) = fresh_text(collation);
    if no_rewind {
        assert!(
            !has_op(&f, dml, "Rewind"),
            "text index-eq must not full-scan (Rewind): `{dml}` [{collation}]"
        );
    } else {
        assert!(
            has_op(&f, dml, "Rewind"),
            "control should full-scan: `{dml}` [{collation}]"
        );
    }
    f.execute_with_params(dml, params)
        .unwrap_or_else(|e| panic!("frank `{dml}`: {e}"));
    let rparams: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
    r.execute(dml, rusqlite::params_from_iter(rparams.iter()))
        .unwrap();
    assert_eq!(
        ut_state_f(&f),
        ut_state_r(&r),
        "state diverged after `{dml}` params {params:?} [{collation}]"
    );
}
#[test]
fn dml_index_eq_param_text_matches_sqlite() {
    // BINARY-collation TEXT index: seeks with an Affinity('B')-coerced probe, no Rewind.
    check_text(
        "",
        "DELETE FROM ut WHERE name = ?1",
        &[SqliteValue::Text("n5".into())],
        true,
    ); // text -> 'n5'
    check_text(
        "",
        "DELETE FROM ut WHERE name = ?1",
        &[SqliteValue::Text("5".into())],
        true,
    ); // text -> '5'
    check_text(
        "",
        "DELETE FROM ut WHERE name = ?1",
        &[SqliteValue::Integer(5)],
        true,
    ); // int 5 -> TEXT '5'
    check_text(
        "",
        "DELETE FROM ut WHERE name = ?1",
        &[SqliteValue::Text("nope".into())],
        true,
    ); // no match
    check_text(
        "",
        "DELETE FROM ut WHERE name = ?1",
        &[SqliteValue::Null],
        true,
    ); // NULL -> none
    check_text(
        "",
        "UPDATE ut SET a = a + 1 WHERE name = ?1",
        &[SqliteValue::Text("n3".into())],
        true,
    );
    check_text(
        "",
        "DELETE FROM ut WHERE name = ?1 AND a > ?2",
        &[SqliteValue::Text("5".into()), SqliteValue::Integer(2)],
        true,
    ); // + residual
    check_text("", "DELETE FROM ut WHERE 'n7' = name", &[], true); // literal value on the LHS, no params
    // NOCASE index declines to a full scan (the coerced probe cannot agree with a NOCASE seek), stays correct.
    check_text(
        " COLLATE NOCASE",
        "DELETE FROM ut WHERE name = ?1",
        &[SqliteValue::Text("N5".into())],
        false,
    );
    check_text(
        " COLLATE NOCASE",
        "DELETE FROM ut WHERE name = ?1",
        &[SqliteValue::Integer(5)],
        false,
    );
}

// REAL ('E') and NUMERIC ('C') affinity indexed columns.
fn fresh_num() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE nt (id INTEGER PRIMARY KEY, amt REAL, qty NUMERIC, a INTEGER);",
        "CREATE INDEX idx_amt ON nt(amt);",
        "CREATE INDEX idx_qty ON nt(qty);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=200_i64 {
        let amt = format!("{}.{}", i % 10, if i % 2 == 0 { 0 } else { 5 }); // '0.0','1.5',... incl. whole reals
        let s = format!("INSERT INTO nt VALUES ({i}, {amt}, {}, {});", i % 15, i % 7);
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    (f, r)
}
fn nt_state_f(c: &Connection) -> Vec<Vec<String>> {
    let mut r: Vec<Vec<String>> = c
        .query("SELECT id, amt, qty, a FROM nt")
        .unwrap()
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    r.sort();
    r
}
fn nt_state_r(c: &rusqlite::Connection) -> Vec<Vec<String>> {
    let mut stmt = c.prepare("SELECT id, amt, qty, a FROM nt").unwrap();
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
fn check_num(dml: &str, params: &[SqliteValue], no_rewind: bool) {
    let (f, r) = fresh_num();
    if no_rewind {
        assert!(
            !has_op(&f, dml, "Rewind"),
            "numeric index-eq must not full-scan: `{dml}`"
        );
    }
    f.execute_with_params(dml, params)
        .unwrap_or_else(|e| panic!("frank `{dml}`: {e}"));
    let rparams: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
    r.execute(dml, rusqlite::params_from_iter(rparams.iter()))
        .unwrap();
    assert_eq!(
        nt_state_f(&f),
        nt_state_r(&r),
        "state diverged after `{dml}` params {params:?}"
    );
}
#[test]
fn dml_index_eq_param_numeric_matches_sqlite() {
    // REAL ('E'): whole-number and fractional reals, plus int/text bounds coerced to REAL.
    check_num(
        "DELETE FROM nt WHERE amt = ?1",
        &[SqliteValue::Float(3.0)],
        true,
    ); // real 3.0 (whole)
    check_num(
        "DELETE FROM nt WHERE amt = ?1",
        &[SqliteValue::Integer(3)],
        true,
    ); // int 3 -> REAL 3.0
    check_num(
        "DELETE FROM nt WHERE amt = ?1",
        &[SqliteValue::Float(3.5)],
        true,
    ); // fractional
    check_num(
        "DELETE FROM nt WHERE amt = ?1",
        &[SqliteValue::Text("3.0".into())],
        true,
    ); // text -> 3.0
    check_num(
        "DELETE FROM nt WHERE amt = ?1",
        &[SqliteValue::Float(99.9)],
        true,
    ); // no match
    check_num("DELETE FROM nt WHERE amt = ?1", &[SqliteValue::Null], true); // NULL -> none
    check_num(
        "UPDATE nt SET a = a + 1 WHERE amt = ?1",
        &[SqliteValue::Float(2.0)],
        true,
    );
    // NUMERIC ('C'): whole numbers stored as integers.
    check_num(
        "DELETE FROM nt WHERE qty = ?1",
        &[SqliteValue::Integer(5)],
        true,
    ); // int 5
    check_num(
        "DELETE FROM nt WHERE qty = ?1",
        &[SqliteValue::Float(5.0)],
        true,
    ); // real 5.0 -> NUMERIC 5
    check_num(
        "DELETE FROM nt WHERE qty = ?1 AND a > ?2",
        &[SqliteValue::Integer(3), SqliteValue::Integer(2)],
        true,
    ); // + residual
}
