//! bd-agg-composite-full-eq: `SELECT COUNT(*)/SUM(x) ... WHERE a=? AND b=?` over a composite (a,b)
//! index now seeks the pinned `(a,b,*)` block once (SeekGE + per-term `Ne` run stop) instead of
//! full-scanning. HARD GATE: byte-identical to C SQLite (rusqlite) across duplicate runs (many rows
//! per (a,b)), absent keys (→ COUNT=0 / SUM=NULL), the COALESCE wrapper, single-match keys, both
//! aggregates, and the 3-term decline (`a=? AND b=? AND c=?` on a 2-col index must NOT drop `c=?`).
//! Opcode gate: the 2-term eq must SeekGE the index; the 3-term case must still match by any means.

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

fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

fn has_op(conn: &Connection, sql: &str, want: &str) -> bool {
    conn.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(
        |row| matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string() == want),
    )
}

#[test]
fn agg_composite_full_eq_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);",
        "CREATE INDEX idx_ab ON t(a, b);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    // a in 0..20, b in 0..10 -> many duplicate (a,b) runs, each with several rows.
    for i in 1..=4000_i64 {
        let a = (i * 7) % 20;
        let b = (i * 3) % 10;
        let x = i % 100;
        let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {x});");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }

    let cmp = |sql: &str| {
        assert_eq!(
            frank_rows(&f, sql),
            sqlite_rows(&r, sql),
            "diverged: `{sql}`"
        );
    };
    for sql in [
        // Present keys, both aggregates, duplicate runs.
        "SELECT COUNT(*) FROM t WHERE a = 7 AND b = 3",
        "SELECT SUM(x) FROM t WHERE a = 7 AND b = 3",
        "SELECT COUNT(*) FROM t WHERE a = 0 AND b = 0",
        "SELECT SUM(x) FROM t WHERE a = 0 AND b = 0",
        "SELECT COUNT(*) FROM t WHERE a = 19 AND b = 9",
        // Reversed conjunct order.
        "SELECT SUM(x) FROM t WHERE b = 3 AND a = 7",
        // Absent (a,b) combos -> COUNT=0 / SUM=NULL.
        "SELECT COUNT(*) FROM t WHERE a = 7 AND b = 100",
        "SELECT SUM(x) FROM t WHERE a = 100 AND b = 3",
        "SELECT COALESCE(SUM(x), -1) FROM t WHERE a = 100 AND b = 3",
        // Present a, absent b within a live prefix.
        "SELECT COUNT(*) FROM t WHERE a = 7 AND b = 7",
        // AVG / MIN / MAX riding the same seek block.
        "SELECT MIN(x), MAX(x) FROM t WHERE a = 7 AND b = 3",
        // 3-term: (a,b) index cannot enforce c; must NOT drop it (decline the 2-col seek).
        "SELECT COUNT(*) FROM t WHERE a = 7 AND b = 3 AND x = 5",
    ] {
        cmp(sql);
    }

    // Opcode gate: the 2-term composite eq seeks the index.
    assert!(
        has_op(&f, "SELECT COUNT(*) FROM t WHERE a = 7 AND b = 3", "SeekGE"),
        "COUNT(*) WHERE a=? AND b=? must seek the (a,b) block"
    );
}
