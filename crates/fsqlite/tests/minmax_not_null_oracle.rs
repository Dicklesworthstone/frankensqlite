//! bd-minmax-redundant-not-null: `SELECT MIN(col)/MAX(col) [, ...] FROM t WHERE col IS NOT NULL` takes
//! the same index-seek fast path as the no-WHERE form (the filter is redundant — MIN/MAX ignore NULLs).
//! Byte-identical to C SQLite; declines when the filter is on a different column, is IS NULL, or the
//! aggregates aren't all MIN/MAX over that column.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;
fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(_) => "blob".into(),
    }
}
fn fr(c: &Connection, s: &str) -> Vec<Vec<String>> {
    c.query(s)
        .unwrap_or_else(|e| panic!("frank `{s}`: {e}"))
        .iter()
        .map(|r| r.values().iter().map(render).collect())
        .collect()
}
fn sq(c: &rusqlite::Connection, s: &str) -> Vec<Vec<String>> {
    let mut st = c.prepare(s).unwrap();
    let n = st.column_count();
    st.query_map([], |row| {
        let mut o = Vec::new();
        for i in 0..n {
            o.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(_) => "blob".to_owned(),
            });
        }
        Ok(o)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}
fn has_op(c: &Connection, s: &str, w: &str) -> bool {
    c.query(&format!("EXPLAIN {s}"))
        .unwrap()
        .iter()
        .any(|r| matches!(r.values().get(1), Some(SqliteValue::Text(op)) if op.to_string() == w))
}
#[test]
fn minmax_not_null_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, w INTEGER);",
        "CREATE INDEX idx_v ON t(v);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=400_i64 {
        let v = if i <= 6 {
            "NULL".to_owned()
        } else {
            format!("{}", ((i * 7) % 300) - 120)
        };
        let w = if i % 5 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 40)
        };
        let s = format!("INSERT INTO t VALUES ({i}, {v}, {w});");
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    let cmp = |s: &str| assert_eq!(fr(&f, s), sq(&r, s), "diverged: `{s}`");
    for s in [
        // Redundant IS NOT NULL -> same as no-WHERE (the lever).
        "SELECT MIN(v) FROM t WHERE v IS NOT NULL",
        "SELECT MAX(v) FROM t WHERE v IS NOT NULL",
        "SELECT MIN(v), MAX(v) FROM t WHERE v IS NOT NULL",
        "SELECT COALESCE(MAX(v), -1) FROM t WHERE v IS NOT NULL",
        // Declines (must still match): filter on a different column; IS NULL; non-MIN/MAX aggregate.
        "SELECT MIN(v) FROM t WHERE w IS NOT NULL",
        "SELECT MAX(v) FROM t WHERE v IS NULL",
        "SELECT COUNT(v) FROM t WHERE v IS NOT NULL",
        "SELECT SUM(v) FROM t WHERE v IS NOT NULL",
    ] {
        cmp(s);
    }
    // All-NULL v.
    for s in [
        "CREATE TABLE n (id INTEGER PRIMARY KEY, v INTEGER);",
        "CREATE INDEX idx_nv ON n(v);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=30 {
        let s = format!("INSERT INTO n VALUES ({i}, NULL);");
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    cmp("SELECT MIN(v), MAX(v) FROM n WHERE v IS NOT NULL");
    // Opcode gate: MAX(v) WHERE v IS NOT NULL uses the index seek (Last); different-column filter does not.
    assert!(
        has_op(&f, "SELECT MAX(v) FROM t WHERE v IS NOT NULL", "Last"),
        "MAX(v) WHERE v IS NOT NULL must use the index-end seek (Last)"
    );
    assert!(
        !has_op(&f, "SELECT MAX(v) FROM t WHERE w IS NOT NULL", "Last"),
        "MAX(v) WHERE w IS NOT NULL (different column) must decline"
    );
}
