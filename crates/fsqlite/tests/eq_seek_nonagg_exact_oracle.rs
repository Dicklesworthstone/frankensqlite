//! bd-eq-seek-fallback-zero-match (non-aggregate): `SELECT ... WHERE col=<int>` / EXISTS / LIMIT 1 on
//! an INTEGER-affinity indexed column now finalizes a 0-match seek directly (no O(n) fallback scan).
//! HARD GATE: byte-identical to C SQLite (rusqlite) for ABSENT keys (empty result / EXISTS 0) and
//! EXISTING keys, on a MIXED-type INTEGER column, plus the non-exact cases (TEXT column, real literal)
//! that keep the fallback.
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
fn eq_seek_nonagg_exact_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c TEXT);",
        "CREATE INDEX idx_a ON t(a);",
        "CREATE INDEX idx_c ON t(c);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=500_i64 {
        let a = if i <= 5 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 50)
        };
        let s = format!("INSERT INTO t VALUES ({i}, {a}, 'k{}');", i % 20);
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    for (id, val) in [(9001, "7.5"), (9002, "'abc'")] {
        let s = format!("INSERT INTO t VALUES ({id}, {val}, 'z');");
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    let cmp = |s: &str| assert_eq!(fr(&f, s), sq(&r, s), "diverged: `{s}`");
    for s in [
        // ABSENT keys -> exact seek, empty result (the lever).
        "SELECT id FROM t WHERE a = 999999",
        "SELECT 1 FROM t WHERE a = 999999 LIMIT 1",
        "SELECT EXISTS(SELECT 1 FROM t WHERE a = 999999)",
        "SELECT EXISTS(SELECT 1 FROM t WHERE a = -1)",
        // EXISTING keys (seek+match path, unchanged) -> EXISTS + deterministic ORDER BY.
        "SELECT EXISTS(SELECT 1 FROM t WHERE a = 7)",
        "SELECT id FROM t WHERE a = 7 ORDER BY id",
        "SELECT id FROM t WHERE a = 7 ORDER BY id LIMIT 1",
        "SELECT EXISTS(SELECT 1 FROM t WHERE a = 0)",
        // Non-exact: TEXT column (keeps fallback).
        "SELECT EXISTS(SELECT 1 FROM t WHERE c = 'nope')",
        "SELECT EXISTS(SELECT 1 FROM t WHERE c = 'k3')",
        // Non-exact: real literal on the INTEGER column (keeps fallback) — 7.5 exists.
        "SELECT EXISTS(SELECT 1 FROM t WHERE a = 7.5)",
        "SELECT id FROM t WHERE a = 7.5 ORDER BY id",
    ] {
        cmp(s);
    }
}
