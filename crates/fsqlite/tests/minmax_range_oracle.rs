//! bd-minmax-range-seek: `SELECT MIN(col) WHERE col >[=] c` / `MAX(col) WHERE col <[=] c` over an
//! INTEGER-affinity single-column-indexed column now seeks the bound (one seek) instead of scanning.
//! HARD GATE: byte-identical to C SQLite (rusqlite) across `>`/`>=`/`<`/`<=`, reversed operand order,
//! NULLs (excluded by the predicate), empty matches (→ NULL), the COALESCE wrapper, MIXED storage
//! classes in the INTEGER column (int/real/text — stresses the storage-class ordering the seek relies
//! on), and the decline cases (unnatural pairing, non-integer bound). Opcode gate: MIN col>c → SeekGT,
//! MAX col<c → SeekLT; unnatural pairings do not.

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
fn minmax_range_seek_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER);",
        "CREATE INDEX idx_a ON t(a);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    // Integers spread over a range, some NULLs (ids 1..=4).
    for i in 1..=500_i64 {
        let a = if i <= 4 {
            "NULL".to_owned()
        } else {
            format!("{}", ((i.wrapping_mul(7)) % 1000) - 400)
        };
        let stmt = format!("INSERT INTO t VALUES ({i}, {a});");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }
    // Mixed storage classes in the INTEGER column (real + text stay off-integer under INTEGER affinity).
    for (id, val) in [
        (9001, "7.5"),
        (9002, "250.25"),
        (9003, "'apple'"),
        (9004, "'zzz'"),
    ] {
        let stmt = format!("INSERT INTO t VALUES ({id}, {val});");
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
        // Natural pairings, all four operators.
        "SELECT MIN(a) FROM t WHERE a > 100",
        "SELECT MIN(a) FROM t WHERE a >= 100",
        "SELECT MAX(a) FROM t WHERE a < 100",
        "SELECT MAX(a) FROM t WHERE a <= 100",
        // Reversed operand order.
        "SELECT MIN(a) FROM t WHERE 100 < a",
        "SELECT MAX(a) FROM t WHERE 100 > a",
        // Empty matches -> NULL.
        "SELECT MIN(a) FROM t WHERE a > 100000",
        "SELECT MAX(a) FROM t WHERE a < -100000",
        // Wrapper.
        "SELECT COALESCE(MIN(a), -1) FROM t WHERE a > 100000",
        "SELECT COALESCE(MAX(a), -1) FROM t WHERE a < -100000",
        // Mixed storage classes exercised at the boundary (text > numeric; reals interleave ints).
        "SELECT MIN(a) FROM t WHERE a > 249",
        "SELECT MAX(a) FROM t WHERE a < 8",
        "SELECT MAX(a) FROM t WHERE a <= 600",
        // Declines (must still match via scan): unnatural pairing; non-integer bound.
        "SELECT MIN(a) FROM t WHERE a < 100",
        "SELECT MAX(a) FROM t WHERE a > 100",
        "SELECT MIN(a) FROM t WHERE a > 3.5",
    ] {
        cmp(sql);
    }

    // Opcode gate.
    assert!(
        has_op(&f, "SELECT MIN(a) FROM t WHERE a > 100", "SeekGT"),
        "MIN(a) WHERE a>c must seek the bound (SeekGT)"
    );
    assert!(
        has_op(&f, "SELECT MAX(a) FROM t WHERE a < 100", "SeekLT"),
        "MAX(a) WHERE a<c must seek the bound (SeekLT)"
    );
    assert!(
        !has_op(&f, "SELECT MIN(a) FROM t WHERE a < 100", "SeekLT")
            && !has_op(&f, "SELECT MIN(a) FROM t WHERE a < 100", "SeekLE"),
        "MIN(a) WHERE a<c (unnatural pairing) must decline the bound seek"
    );
}
