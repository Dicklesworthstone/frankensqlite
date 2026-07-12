//! Probe: `SELECT ... FROM t WHERE a = ? ORDER BY b [DESC] LIMIT k` (top-k-per-group) on a composite
//! `(a, b)` index — does frank STREAM off the index (no sorter) or open a SorterOpen? Byte-exactness +
//! the plan shape decide whether a "top-k per group, no sorter" lever exists.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn frank(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(sql)
        .unwrap()
        .iter()
        .map(|r| {
            r.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f:?}"),
                    SqliteValue::Text(s) => format!("'{s}'"),
                    SqliteValue::Blob(_) => "blob".to_owned(),
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect()
}

fn sqlite(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut cols = Vec::new();
        for i in 0..n {
            cols.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(_) => "blob".to_owned(),
            });
        }
        Ok(cols.join(","))
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

fn has_sorter(conn: &Connection, sql: &str) -> bool {
    conn.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .any(|r| matches!(r.values().get(1), Some(SqliteValue::Text(op)) if op.to_string() == "SorterOpen"))
}

#[test]
#[ignore = "probe; run with --nocapture"]
fn where_eq_orderby_limit_shape() {
    let f = Connection::open(":memory:").unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
        "CREATE INDEX idx_ab ON t(a, b);",
        "CREATE INDEX idx_ab_desc ON t(a, b DESC);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    for i in 1..=400_i64 {
        let a = i % 8;
        let b = (i.wrapping_mul(7)) % 50;
        let s = format!("INSERT INTO t VALUES ({i}, {a}, {b});");
        f.execute(&s).unwrap();
        r.execute_batch(&s).unwrap();
    }
    for sql in [
        "SELECT id, b FROM t WHERE a = 5 ORDER BY b DESC LIMIT 1",
        "SELECT id, b FROM t WHERE a = 5 ORDER BY b DESC LIMIT 3",
        "SELECT id, b FROM t WHERE a = 5 ORDER BY b ASC LIMIT 1",
        "SELECT id, b FROM t WHERE a = 5 ORDER BY b ASC LIMIT 3",
        "SELECT b FROM t WHERE a = 5 ORDER BY b DESC LIMIT 2",
    ] {
        let (fr, sq) = (frank(&f, sql), sqlite(&r, sql));
        eprintln!("\n[{sql}]");
        eprintln!("  match={}  frank_sorter={}", fr == sq, has_sorter(&f, sql));
        if fr != sq {
            eprintln!("  frank : {fr:?}");
            eprintln!("  sqlite: {sq:?}");
        }
    }
    eprintln!(
        "\n-> if match=true and frank_sorter=true, a 'top-k per group off the index, no sorter' lever exists."
    );
}
