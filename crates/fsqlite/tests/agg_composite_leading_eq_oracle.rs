//! bd-agg-composite-leading-eq: COUNT(*)/SUM(x)/MIN(x)/MAX(x) FROM t WHERE a=? where `a` LEADS a
//! composite (a,b) index (no single-col index on a) now seeks the a-block instead of full-scanning.
//! Byte-identical to C SQLite: existing keys (duplicate runs), absent keys (COUNT=0/SUM=NULL), NULLs;
//! regression: same on a single-column index.
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
fn agg_composite_leading_eq_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);",
        "CREATE INDEX idx_ab ON t(a, b);", // composite; a leads; NO single-col index on a
        "CREATE INDEX idx_x ON t(x);",     // single-col (regression control)
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=800_i64 {
        let a = if i <= 6 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 20)
        };
        let s = format!(
            "INSERT INTO t VALUES ({i}, {a}, {}, {});",
            i % 9,
            (i % 50) as i64 - 25
        );
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    let cmp = |s: &str| assert_eq!(fr(&f, s), sq(&r, s), "diverged: `{s}`");
    for s in [
        // Composite-leading a: seek the a-block (existing + absent).
        "SELECT COUNT(*) FROM t WHERE a = 5",
        "SELECT SUM(x) FROM t WHERE a = 5",
        "SELECT SUM(b) FROM t WHERE a = 5",
        "SELECT MIN(x), MAX(x) FROM t WHERE a = 5",
        "SELECT COUNT(*) FROM t WHERE a = 999",
        "SELECT SUM(x) FROM t WHERE a = 999",
        "SELECT COALESCE(SUM(x), -7) FROM t WHERE a = 999",
        "SELECT COUNT(*) FROM t WHERE a = 0",
        // Regression: single-column index still seeks.
        "SELECT COUNT(*) FROM t WHERE x = 5",
        "SELECT SUM(b) FROM t WHERE x = 5",
    ] {
        cmp(s);
    }
    assert!(
        has_op(&f, "SELECT COUNT(*) FROM t WHERE a = 5", "SeekGE"),
        "COUNT(*) WHERE a=? (composite-leading) must seek the a-block (SeekGE)"
    );
}
