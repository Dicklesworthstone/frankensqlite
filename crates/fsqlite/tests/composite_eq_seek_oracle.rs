//! bd-composite-eq-seek: `SELECT ... FROM t WHERE a=? AND b=?` on a composite (a,b[,c]) index now
//! seeks the full key (via the prefix+range seek, last term demoted to [c,c]) instead of full-scanning.
//! Byte-identical to C SQLite across multi-row matches (order), absent keys (empty), 3-term equality,
//! text/int columns, reversed conjunct order; regression: WHERE a=? AND b>? (real range) still works.
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
fn composite_eq_seek_matches_sqlite() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, w TEXT, x INTEGER, z INTEGER);",
        "CREATE INDEX idx_ab ON t(a, b);",
        "CREATE INDEX idx_aw ON t(a, w);",
        "CREATE INDEX idx_abz ON t(a, b, z);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=800_i64 {
        // multiple rows per (a,b): a in 0..9, b in 0..9 -> ~8 rows each
        let s = format!(
            "INSERT INTO t VALUES ({i}, {}, {}, 'w{}', {i}, {});",
            i % 10,
            i % 9,
            i % 9,
            i % 3
        );
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    let cmp = |s: &str| assert_eq!(fr(&f, s), sq(&r, s), "diverged: `{s}`");
    for s in [
        // Multi-row matches (natural order, no ORDER BY) + LIMIT.
        "SELECT id FROM t WHERE a = 5 AND b = 3",
        "SELECT id, x FROM t WHERE a = 5 AND b = 3 LIMIT 3",
        "SELECT id FROM t WHERE a = 5 AND b = 3 ORDER BY id",
        // Reversed conjunct order.
        "SELECT id FROM t WHERE b = 3 AND a = 5",
        // Absent -> empty.
        "SELECT id FROM t WHERE a = 5 AND b = 999",
        "SELECT id FROM t WHERE a = 999 AND b = 3",
        // Text second column.
        "SELECT id FROM t WHERE a = 5 AND w = 'w3'",
        "SELECT id FROM t WHERE a = 5 AND w = 'nope'",
        // 3-term full equality on (a,b,z).
        "SELECT id FROM t WHERE a = 5 AND b = 3 AND z = 1",
        // Regression: real range after the prefix still works.
        "SELECT id FROM t WHERE a = 5 AND b > 3 ORDER BY id",
        "SELECT id FROM t WHERE a = 5 AND b >= 3 AND b <= 6 ORDER BY id",
    ] {
        cmp(s);
    }
    // Opcode gate: composite eq seeks (SeekGE / SeekGT), not a plain scan.
    assert!(
        has_op(&f, "SELECT id FROM t WHERE a = 5 AND b = 3", "SeekGE"),
        "a=? AND b=? must seek the composite index (SeekGE)"
    );
    assert!(
        has_op(&f, "SELECT id FROM t WHERE a = 5 AND w = 'w3'", "SeekGE"),
        "a=? AND w=? (text) must seek the composite index"
    );
}
