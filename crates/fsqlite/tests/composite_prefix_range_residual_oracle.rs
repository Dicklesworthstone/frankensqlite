//! Correctness probe (bd-zqkrp residual-safety): the composite equality-prefix + trailing-range seek
//! (`WHERE a = v AND b <range>` on `index(a,b)`) applies NO residual filter. If the detection matches a
//! WHERE that ALSO carries a predicate on a NON-key column (`... AND c = k`), that predicate would be
//! silently dropped and rows with the wrong `c` returned. This oracle asserts byte-identical results to
//! C SQLite for exactly that shape (and confirms the plain no-residual shape still seeks).

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

#[test]
fn composite_prefix_range_residual_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
        "CREATE INDEX idx_ab ON t(a, b);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    for i in 1..=2000_i64 {
        let a = i % 10;
        let b = i % 25;
        let c = i % 4;
        let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {c});");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }

    // Compare as SORTED SETS: no ORDER BY (so the composite prefix-range SEEK is actually taken — an
    // `ORDER BY id` it can't satisfy would decline the seek and hide the bug), sort to ignore the
    // seek's (b, rowid) emission order.
    let cmp = |sql: &str| {
        let mut a = frank_rows(&f, sql);
        let mut b = sqlite_rows(&r, sql);
        a.sort();
        b.sort();
        assert_eq!(a, b, "diverged: `{sql}`");
    };
    for sql in [
        // Plain prefix+range (no residual) — should seek and match.
        "SELECT id FROM t WHERE a = 5 AND b > 10",
        // Residual on a NON-key column: `c = 1` must NOT be dropped.
        "SELECT id FROM t WHERE a = 5 AND b > 10 AND c = 1",
        "SELECT id FROM t WHERE a = 3 AND b >= 5 AND c = 2",
        "SELECT id FROM t WHERE a = 7 AND b BETWEEN 2 AND 20 AND c = 0",
        "SELECT id FROM t WHERE a = 2 AND b < 15 AND c = 3",
        // Residual that references the range column again with a different op.
        "SELECT id FROM t WHERE a = 4 AND b > 3 AND c <> 1",
        // Also select c so a dropped `c` predicate shows as wrong rows even when ids happen to align.
        "SELECT id, c FROM t WHERE a = 5 AND b > 10 AND c = 1",
    ] {
        cmp(sql);
    }

    // The no-residual case must STILL take the composite prefix-range seek (guard must not
    // over-decline): its stop opcode is IdxGT. The residual case must NOT seek (declined to a scan).
    let has_op = |sql: &str, want: &str| {
        f.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(
            |row| matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string() == want),
        )
    };
    assert!(
        has_op("SELECT id FROM t WHERE a = 5 AND b > 10", "IdxGT"),
        "no-residual composite prefix+range must still seek (IdxGT)"
    );
    assert!(
        !has_op("SELECT id FROM t WHERE a = 5 AND b > 10 AND c = 1", "IdxGT"),
        "residual `c = 1` must decline the seek (fall to a scan that enforces c)"
    );
}
