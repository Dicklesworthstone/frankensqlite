//! bd-agg-composite-prefix-range: `SELECT COUNT(*)/SUM(...) FROM t WHERE a = v AND b <range>` on a
//! composite `index(a, b)` now seeks the `a = v` block bounded by the range on `b` instead of
//! full-scanning — for SUM (via `codegen_select_aggregate`) and COUNT(*) (which yields out of
//! `codegen_select_count_star`). HARD GATE: byte-identical to C SQLite (rusqlite) across all range
//! operators on `b`, both aggregates, NULL `b` (excluded), empty ranges (→ COUNT=0 / SUM=NULL),
//! COALESCE, mixed storage in `b`, and a residual-predicate DECLINE (`... AND c = k` must not be
//! dropped — the bd-zqkrp-residual-drop guard). Opcode gate: the seek emits SeekGE + IdxGT; COUNT(*)
//! and SUM(a) are covering (no SeekRowid), SUM(b) is not.

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
fn agg_composite_prefix_range_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
        "CREATE INDEX idx_ab ON t(a, b);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    for i in 1..=3000_i64 {
        let a = i % 12;
        // b spread over a range with some NULLs.
        let b = if i % 13 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", ((i.wrapping_mul(7)) % 200) - 50)
        };
        let c = i % 5;
        let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {c});");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }
    // Mixed storage in b under INTEGER affinity.
    for (id, aval, bval) in [(9001, 5, "7.5"), (9002, 5, "'apple'")] {
        let stmt = format!("INSERT INTO t VALUES ({id}, {aval}, {bval}, 0);");
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
        // All range operators on b within a = v, both aggregates.
        "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10",
        "SELECT SUM(c) FROM t WHERE a = 5 AND b > 10",
        "SELECT COUNT(*) FROM t WHERE a = 5 AND b >= 10",
        "SELECT COUNT(*) FROM t WHERE a = 5 AND b < 10",
        "SELECT SUM(c) FROM t WHERE a = 5 AND b <= 10",
        "SELECT COUNT(*) FROM t WHERE a = 5 AND b BETWEEN -20 AND 40",
        "SELECT SUM(c) FROM t WHERE a = 5 AND b > -20 AND b < 40",
        // Reversed operand order in the range.
        "SELECT COUNT(*) FROM t WHERE a = 5 AND 10 < b",
        // Absent prefix / empty range -> COUNT=0 / SUM=NULL, and COALESCE.
        "SELECT COUNT(*) FROM t WHERE a = 999 AND b > 0",
        "SELECT SUM(c) FROM t WHERE a = 5 AND b > 100000",
        "SELECT COALESCE(SUM(c), -1) FROM t WHERE a = 5 AND b > 100000",
        // Covering SUM of the pinned leading column, and of the range column (both are key columns).
        "SELECT SUM(a) FROM t WHERE a = 5 AND b > 10",
        "SELECT SUM(b) FROM t WHERE a = 5 AND b > 10",
        "SELECT MIN(b) FROM t WHERE a = 5 AND b > 10",
        "SELECT MAX(b) FROM t WHERE a = 5 AND b BETWEEN 0 AND 40",
        // Residual on a NON-key column: `c = 1` must NOT be dropped (declines to a correct scan).
        "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10 AND c = 1",
        "SELECT SUM(c) FROM t WHERE a = 3 AND b BETWEEN 0 AND 50 AND c = 2",
    ] {
        cmp(sql);
    }

    // Opcode gate: the composite prefix+range seek fires (SeekGE anchor + IdxGT prefix stop).
    assert!(
        has_op(
            &f,
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10",
            "SeekGE"
        ) && has_op(&f, "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10", "IdxGT"),
        "COUNT(*) WHERE a=v AND b>c must seek (SeekGE + IdxGT)"
    );
    assert!(
        has_op(&f, "SELECT SUM(c) FROM t WHERE a = 5 AND b > 10", "IdxGT"),
        "SUM WHERE a=v AND b>c must seek the composite block"
    );
    // Covering gate: COUNT(*)/SUM(leading col a) read straight from the index; SUM(b) needs the table.
    assert!(
        !has_op(
            &f,
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10",
            "SeekRowid"
        ),
        "COUNT(*) composite prefix+range must be covering (no SeekRowid)"
    );
    assert!(
        !has_op(
            &f,
            "SELECT SUM(a) FROM t WHERE a = 5 AND b > 10",
            "SeekRowid"
        ),
        "SUM(leading col) composite prefix+range must be covering (no SeekRowid)"
    );
    assert!(
        !has_op(
            &f,
            "SELECT SUM(b) FROM t WHERE a = 5 AND b > 10",
            "SeekRowid"
        ),
        "SUM(range col b) is a KEY column -> covering (no SeekRowid)"
    );
    // A SUM of a NON-key column still needs the table lookup.
    assert!(
        has_op(
            &f,
            "SELECT SUM(c) FROM t WHERE a = 5 AND b > 10",
            "SeekRowid"
        ),
        "SUM(non-key col c) must open the table (SeekRowid)"
    );
    // Residual declines the seek (no IdxGT), falling to a scan that enforces c.
    assert!(
        !has_op(
            &f,
            "SELECT COUNT(*) FROM t WHERE a = 5 AND b > 10 AND c = 1",
            "IdxGT"
        ),
        "residual `c = 1` must decline the composite prefix+range seek"
    );
}
