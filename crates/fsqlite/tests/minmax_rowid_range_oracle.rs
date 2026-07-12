//! bd-minmax-rowid-range-seek: MIN(id) WHERE id >[=] c / MAX(id) WHERE id <[=] c over the rowid seeks
//! the table b-tree (one seek) instead of scanning. Byte-identical to C SQLite across all four
//! operators, reversed operand order, empty matches (-> NULL), COALESCE, and declines.
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
fn minmax_rowid_range_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);";
    f.execute(ddl).unwrap();
    r.execute_batch(ddl).unwrap();
    for i in 1..=400_i64 {
        // ids with gaps: 3,6,9,...
        let s = format!("INSERT INTO t VALUES ({}, {});", i * 3, i % 50);
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    let cmp = |s: &str| assert_eq!(fr(&f, s), sq(&r, s), "diverged: `{s}`");
    for s in [
        "SELECT MIN(id) FROM t WHERE id > 300",
        "SELECT MIN(id) FROM t WHERE id >= 300",
        "SELECT MAX(id) FROM t WHERE id < 300",
        "SELECT MAX(id) FROM t WHERE id <= 300",
        "SELECT MIN(id) FROM t WHERE id > 301", // between gaps
        "SELECT MAX(id) FROM t WHERE id < 301",
        "SELECT MIN(id) FROM t WHERE 300 < id", // reversed
        "SELECT MAX(id) FROM t WHERE 300 > id",
        "SELECT MIN(id) FROM t WHERE id > 100000", // empty -> NULL
        "SELECT MAX(id) FROM t WHERE id < 0",      // empty -> NULL
        "SELECT COALESCE(MIN(id), -1) FROM t WHERE id > 100000",
        "SELECT COALESCE(MAX(id), -1) FROM t WHERE id < 0",
        // declines: unnatural pairing; non-integer bound
        "SELECT MIN(id) FROM t WHERE id < 300",
        "SELECT MAX(id) FROM t WHERE id > 300",
        "SELECT MIN(id) FROM t WHERE id > 2.5",
    ] {
        cmp(s);
    }
    assert!(
        has_op(&f, "SELECT MIN(id) FROM t WHERE id > 300", "SeekGT"),
        "MIN(id) id>c must SeekGT"
    );
    assert!(
        has_op(&f, "SELECT MAX(id) FROM t WHERE id < 300", "SeekLT"),
        "MAX(id) id<c must SeekLT"
    );
}
