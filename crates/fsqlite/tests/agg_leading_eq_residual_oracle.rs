//! bd-agg-leading-eq-residual: `SELECT COUNT(*)/SUM(...) FROM t WHERE a = <int> AND <residual>` seeks
//! the integer-exact equality-prefix block on an index and applies the FULL (placeholder-free) WHERE
//! as a residual filter per row, instead of full-scanning. HARD GATE: byte-identical to C SQLite across
//! residual equality/range/OR/multi-term predicates, a multi-column equality prefix, absent keys
//! (→ COUNT=0 / SUM=NULL), COALESCE, and both aggregates. The residual filter enforces the WHOLE
//! predicate, so nothing is dropped. Opcode gate: an all-literal WHERE SeekGEs the index; a WHERE with
//! a bound parameter DECLINES the seek (falls to a scan that binds it).

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(_) => "blob".to_owned(),
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
                rusqlite::types::Value::Blob(_) => "blob".to_owned(),
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
fn agg_leading_eq_residual_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER, y INTEGER, s TEXT);",
        "CREATE INDEX idx_ab ON t(a, b);",
        "CREATE INDEX idx_a ON t(a);",
        "CREATE INDEX idx_s ON t(s);", // BINARY collation (default)
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    for i in 1..=3000_i64 {
        let a = i % 12;
        let b = i % 7;
        let x = i % 10;
        let y = i % 3;
        let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {x}, {y}, 'k{}');", i % 6);
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
        // Leading eq + residual equality on a non-key column, both aggregates.
        "SELECT COUNT(*) FROM t WHERE a = 7 AND x = 5",
        "SELECT SUM(x) FROM t WHERE a = 7 AND x = 5",
        "SELECT SUM(b) FROM t WHERE a = 7 AND x = 5",
        // Residual range / inequality / OR.
        "SELECT COUNT(*) FROM t WHERE a = 7 AND x > 5",
        "SELECT COUNT(*) FROM t WHERE a = 3 AND x <> 5",
        "SELECT COUNT(*) FROM t WHERE a = 7 AND (x = 5 OR x = 6)",
        // Multiple residuals.
        "SELECT COUNT(*) FROM t WHERE a = 7 AND x = 5 AND y = 1",
        // Multi-column equality prefix (a, b) + residual on x.
        "SELECT COUNT(*) FROM t WHERE a = 7 AND b = 3 AND x = 5",
        "SELECT SUM(x) FROM t WHERE a = 7 AND b = 3 AND x > 2",
        // Absent prefix -> COUNT=0 / SUM=NULL, and COALESCE.
        "SELECT COUNT(*) FROM t WHERE a = 999 AND x = 5",
        "SELECT SUM(x) FROM t WHERE a = 999 AND x = 5",
        "SELECT COALESCE(SUM(x), -1) FROM t WHERE a = 999 AND x = 5",
        // MIN/MAX riding the same seek.
        "SELECT MIN(x), MAX(x) FROM t WHERE a = 7 AND x > 2",
        // TEXT prefix (BINARY-indexed) + residual — text literal vs TEXT column seeks exactly.
        "SELECT COUNT(*) FROM t WHERE s = 'k3' AND x = 5",
        "SELECT SUM(x) FROM t WHERE s = 'k3' AND x > 2",
        "SELECT COUNT(*) FROM t WHERE s = 'nope' AND x = 5",
    ] {
        cmp(sql);
    }

    // Opcode gate: an all-literal WHERE seeks the prefix (integer and text prefixes both).
    assert!(
        has_op(&f, "SELECT COUNT(*) FROM t WHERE a = 7 AND x = 5", "SeekGE"),
        "leading eq + literal residual must seek the prefix (SeekGE)"
    );
    assert!(
        has_op(&f, "SELECT COUNT(*) FROM t WHERE s = 'k3' AND x = 5", "SeekGE"),
        "TEXT leading eq + literal residual must seek the prefix (SeekGE)"
    );
    // A bound parameter anywhere in the WHERE declines the seek (re-emitting it per row could
    // mis-number the parameter), falling to a scan that binds it correctly.
    assert!(
        !has_op(&f, "SELECT COUNT(*) FROM t WHERE a = 7 AND x = ?", "SeekGE"),
        "a WHERE with a bound parameter must decline the residual-filter seek"
    );
}
