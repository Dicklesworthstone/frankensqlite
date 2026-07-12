//! bd-eq-seek-fallback-zero-match: `COUNT(*)/SUM(b) FROM t WHERE a = <int>` on an INTEGER-affinity
//! indexed column now finalizes a 0-match seek directly (no O(n) fallback scan). HARD GATE: byte-
//! identical to C SQLite (rusqlite) for existing keys (single & duplicate runs), NONEXISTENT keys
//! (COUNT=0 / SUM=NULL / MIN=NULL), on a MIXED-type INTEGER column (int/real/text present), and the
//! non-exact cases (TEXT column, real literal) which keep the fallback and must still match.
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
#[test]
fn eq_seek_exact_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT);",
        "CREATE INDEX idx_a ON t(a);",
        "CREATE INDEX idx_c ON t(c);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    // a in 0..49 (each ~10 rows -> duplicate runs); some NULLs; c text.
    for i in 1..=500_i64 {
        let a = if i <= 5 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 50)
        };
        let s = format!("INSERT INTO t VALUES ({i}, {a}, {}, 'k{}');", i % 7, i % 20);
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    // Mixed storage classes in the INTEGER column a (real + text stay off-integer).
    for (id, val) in [(9001, "7.5"), (9002, "'abc'")] {
        let s = format!("INSERT INTO t VALUES ({id}, {val}, 0, 'z');");
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    let cmp = |s: &str| assert_eq!(fr(&f, s), sq(&r, s), "diverged: `{s}`");
    for s in [
        // Existing keys (duplicate runs).
        "SELECT COUNT(*) FROM t WHERE a = 7",
        "SELECT SUM(b) FROM t WHERE a = 7",
        "SELECT MIN(a), MAX(a) FROM t WHERE a = 7",
        "SELECT COUNT(*) FROM t WHERE a = 0",
        // NONEXISTENT keys -> exact seek, no fallback scan (the lever).
        "SELECT COUNT(*) FROM t WHERE a = 999",
        "SELECT SUM(b) FROM t WHERE a = 999",
        "SELECT MIN(a) FROM t WHERE a = 999",
        "SELECT COUNT(*) FROM t WHERE a = -1",
        "SELECT COALESCE(SUM(b), -9) FROM t WHERE a = 999",
        // Non-exact: TEXT column (keeps fallback) - existing + nonexistent.
        "SELECT COUNT(*) FROM t WHERE c = 'k3'",
        "SELECT COUNT(*) FROM t WHERE c = 'nope'",
        // Non-exact: real literal on the INTEGER column (keeps fallback).
        "SELECT COUNT(*) FROM t WHERE a = 7.5",
        "SELECT COUNT(*) FROM t WHERE a = 7.0",
    ] {
        cmp(s);
    }
}
