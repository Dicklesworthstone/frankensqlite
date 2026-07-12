//! Verify: do `WHERE a=?` aggregate/existence shapes and range-bounded MIN/MAX use idx_a (Seek*) or
//! full-scan (Rewind on the table cursor)? Prints the distinguishing opcodes per query.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn ops(conn: &Connection, sql: &str) -> Vec<(String, i64)> {
    conn.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .filter_map(|row| {
            let v = row.values();
            match (v.get(1), v.get(2)) {
                (Some(SqliteValue::Text(op)), Some(SqliteValue::Integer(p1))) => {
                    Some((op.to_string(), *p1))
                }
                _ => None,
            }
        })
        .collect()
}

#[test]
#[ignore = "probe; run with --nocapture"]
fn where_a_plans() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);")
        .unwrap();
    conn.execute("CREATE INDEX idx_a ON t(a);").unwrap();
    for i in 1..=2000_i64 {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, {}, {});",
            (i * 7) % 500,
            i % 100
        ))
        .unwrap();
    }
    for sql in [
        "SELECT COUNT(*) FROM t WHERE a = 7",
        "SELECT SUM(b) FROM t WHERE a = 7",
        "SELECT 1 FROM t WHERE a = 7 LIMIT 1",
        "SELECT id FROM t WHERE a = 7 LIMIT 1",
        "SELECT MIN(a) FROM t WHERE a > 300",
        "SELECT MAX(a) FROM t WHERE a < 300",
    ] {
        let o = ops(&conn, sql);
        let seek = o.iter().any(|(op, _)| op.starts_with("Seek"));
        let opens_idx = o.iter().filter(|(op, _)| op == "OpenRead").count();
        let rewind = o.iter().any(|(op, _)| op == "Rewind");
        let names: Vec<&str> = o.iter().map(|(op, _)| op.as_str()).collect();
        eprintln!(
            "\n[{sql}]\n  seek={seek} rewind={rewind} openreads={opens_idx}\n  ops={names:?}"
        );
    }
}
