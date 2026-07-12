//! bd-agg-index-range: `SELECT COUNT(*)/SUM(x) FROM t WHERE a <range>` on a single-column index now
//! seeks the bound and walks the range instead of full-scanning — for BOTH `SUM` (via
//! `codegen_select_aggregate`) and `COUNT(*)` (which now yields out of `codegen_select_count_star`).
//! HARD GATE: byte-identical to C SQLite (rusqlite) across `>`/`>=`/`<`/`<=`/`BETWEEN`, reversed
//! operand order, NULLs (excluded by the predicate), empty ranges (→ COUNT=0 / SUM=NULL), the COALESCE
//! wrapper, mixed storage classes in the indexed column, and a residual-predicate DECLINE
//! (`a > c AND x = k` must not drop `x = k`). Opcode gate: a lower-bounded range SeekGEs the index.

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
fn agg_index_range_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, x INTEGER);",
        "CREATE INDEX idx_a ON t(a);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    // ids 1..=4 have NULL a (excluded by every range); the rest spread over [-400, 600).
    for i in 1..=500_i64 {
        let a = if i <= 4 {
            "NULL".to_owned()
        } else {
            format!("{}", ((i.wrapping_mul(7)) % 1000) - 400)
        };
        let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {});", i % 100);
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }
    // Mixed storage classes in the indexed column (real + text stay off-integer under INTEGER affinity).
    for (id, val) in [
        (9001, "7.5"),
        (9002, "250.25"),
        (9003, "'apple'"),
        (9004, "'zzz'"),
    ] {
        let stmt = format!("INSERT INTO t VALUES ({id}, {val}, 0);");
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
        // All four operators, both aggregates.
        "SELECT COUNT(*) FROM t WHERE a > 100",
        "SELECT SUM(x) FROM t WHERE a > 100",
        "SELECT COUNT(*) FROM t WHERE a >= 100",
        "SELECT COUNT(*) FROM t WHERE a < 100",
        "SELECT SUM(x) FROM t WHERE a <= 100",
        // BETWEEN (two-sided) and its desugaring.
        "SELECT COUNT(*) FROM t WHERE a BETWEEN 0 AND 200",
        "SELECT SUM(x) FROM t WHERE a > 0 AND a < 200",
        "SELECT COUNT(*) FROM t WHERE a >= -100 AND a <= 100",
        // Reversed operand order.
        "SELECT COUNT(*) FROM t WHERE 100 < a",
        "SELECT SUM(x) FROM t WHERE 100 > a",
        // Empty ranges -> COUNT=0 / SUM=NULL, and the COALESCE wrapper.
        "SELECT COUNT(*) FROM t WHERE a > 100000",
        "SELECT SUM(x) FROM t WHERE a < -100000",
        "SELECT COALESCE(SUM(x), -1) FROM t WHERE a > 100000",
        // Boundaries where reals/text interleave the integer range.
        "SELECT COUNT(*) FROM t WHERE a > 249",
        "SELECT COUNT(*) FROM t WHERE a <= 600",
        // Covering SUM of the *indexed* column (read straight from the index, no table lookup).
        "SELECT SUM(a) FROM t WHERE a > 100",
        "SELECT SUM(a) FROM t WHERE a BETWEEN 0 AND 200",
        // Range + residual (placeholder-free): seek the range as a superset, re-apply the whole WHERE.
        // MUST NOT drop the residual `x = 5` / `x > 3` / `x IN (...)`.
        "SELECT COUNT(*) FROM t WHERE a > 100 AND x = 5",
        "SELECT SUM(x) FROM t WHERE a BETWEEN 0 AND 200 AND x > 3",
        "SELECT COUNT(*) FROM t WHERE a > 100 AND x IN (2, 5)",
        "SELECT COUNT(*) FROM t WHERE a >= -100 AND a <= 100 AND x <> 5",
    ] {
        cmp(sql);
    }

    // Range + residual now SEEKS (superset) instead of scanning; the residual is enforced by the filter.
    assert!(
        has_op(
            &f,
            "SELECT COUNT(*) FROM t WHERE a > 100 AND x = 5",
            "SeekGE"
        ),
        "range + residual must seek the range (SeekGE) and filter the residual"
    );
    assert!(
        has_op(
            &f,
            "SELECT COUNT(*) FROM t WHERE a > 100 AND x = 5",
            "SeekRowid"
        ),
        "range + residual is non-covering (SeekRowid to read the residual column)"
    );

    // Opcode gate: a lower-bounded range seeks the index for both aggregates.
    assert!(
        has_op(&f, "SELECT COUNT(*) FROM t WHERE a > 100", "SeekGE"),
        "COUNT(*) WHERE a>c must seek the index"
    );
    assert!(
        has_op(&f, "SELECT SUM(x) FROM t WHERE a > 100", "SeekGE"),
        "SUM(x) WHERE a>c must seek the index"
    );
    // Covering gate: COUNT(*) and SUM(indexed col) read straight from the index (no SeekRowid);
    // SUM of a non-indexed column still needs the table lookup.
    assert!(
        !has_op(&f, "SELECT COUNT(*) FROM t WHERE a > 100", "SeekRowid"),
        "COUNT(*) range walk must be covering (no SeekRowid)"
    );
    assert!(
        !has_op(&f, "SELECT SUM(a) FROM t WHERE a > 100", "SeekRowid"),
        "SUM(indexed col) range walk must be covering (no SeekRowid)"
    );
    assert!(
        has_op(&f, "SELECT SUM(x) FROM t WHERE a > 100", "SeekRowid"),
        "SUM(non-indexed col) range walk must open the table (SeekRowid)"
    );
}
