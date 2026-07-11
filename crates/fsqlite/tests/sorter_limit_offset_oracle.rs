//! bd-5310l: `ORDER BY ... LIMIT l OFFSET o` now bounds the top-N sorter to `l + o` retained rows
//! (was a full sort). HARD GATE: results must be byte-identical to C SQLite (rusqlite) across bounded
//! and above-cap (full-sort fallback) cases, deterministic and tie-heavy orderings, ASC/DESC, ints,
//! text, and NULLs. Plus an opcode gate: within the cap, `SorterOpen` carries `top_n_limit = l + o`.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
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
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => {
                    format!(
                        "X'{}'",
                        b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                    )
                }
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// The `top_n_limit` (p3) of the first `SorterOpen` opcode, or None if no sorter / not bounded.
fn sorter_top_n(conn: &Connection, sql: &str) -> Option<i64> {
    let rows = conn.query(&format!("EXPLAIN {sql}")).unwrap();
    for row in &rows {
        let vals = row.values();
        if let Some(SqliteValue::Text(op)) = vals.get(1) {
            if op.to_string() == "SorterOpen" {
                return match vals.get(4) {
                    Some(SqliteValue::Integer(p3)) => Some(*p3),
                    _ => Some(0),
                };
            }
        }
    }
    None
}

#[test]
fn ordered_limit_offset_bounded_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    let create = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, k INTEGER);";
    f.execute(create).unwrap();
    r.execute_batch(create).unwrap();
    // Tie-heavy v (only 50 distinct values across 2000 rows) so the bounded top-N tie handling is
    // exercised; ORDER BY v, id gives a deterministic total order (id is unique).
    for i in 1..=2000_i64 {
        let v = format!("v{:03}", (i.wrapping_mul(7)) % 50);
        let k = i % 13;
        let stmt = format!("INSERT INTO t VALUES ({i}, '{v}', {k});");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }
    for s in [
        "INSERT INTO t VALUES (5001, NULL, NULL);",
        "INSERT INTO t VALUES (5002, NULL, 7);",
    ] {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }

    let cmp = |sql: &str| {
        assert_eq!(
            frank_rows(&f, sql),
            sqlite_rows(&r, sql),
            "LIMIT/OFFSET sort diverged: `{sql}`"
        );
    };
    for sql in [
        // Bounded (l + o <= 1024): the new path.
        "SELECT id, v FROM t ORDER BY v, id LIMIT 10 OFFSET 100",
        "SELECT id, v FROM t ORDER BY v, id LIMIT 10 OFFSET 0",
        "SELECT id, v FROM t ORDER BY v, id LIMIT 1 OFFSET 500",
        "SELECT id, v FROM t ORDER BY v DESC, id DESC LIMIT 20 OFFSET 50",
        "SELECT id, k FROM t ORDER BY k, id LIMIT 15 OFFSET 200",
        "SELECT id, v, k FROM t ORDER BY v, k, id LIMIT 7 OFFSET 300",
        "SELECT id FROM t ORDER BY v, id LIMIT 5 OFFSET 1019", // l+o = 1024 (exactly the cap)
        // Above the cap / edges -> full-sort fallback, must still match.
        "SELECT id, v FROM t ORDER BY v, id LIMIT 5 OFFSET 1020", // l+o = 1025 (over cap)
        "SELECT id, v FROM t ORDER BY v, id LIMIT 10 OFFSET 5000",
        "SELECT id FROM t ORDER BY v, id LIMIT 0 OFFSET 5",
        "SELECT id, v FROM t ORDER BY v, id", // no limit
        // NULLs ordering + offset.
        "SELECT id, v FROM t ORDER BY v, id LIMIT 3 OFFSET 1",
        "SELECT id, v FROM t ORDER BY v DESC, id LIMIT 3 OFFSET 1",
    ] {
        cmp(sql);
    }

    // Opcode gate: within the cap the sorter is bounded to l + o; above the cap it is full (0).
    assert_eq!(
        sorter_top_n(&f, "SELECT id, v FROM t ORDER BY v, id LIMIT 10 OFFSET 100"),
        Some(110),
        "LIMIT 10 OFFSET 100 must bound the sorter to 110"
    );
    assert_eq!(
        sorter_top_n(
            &f,
            "SELECT id, v FROM t ORDER BY v, id LIMIT 10 OFFSET 5000"
        ),
        Some(0),
        "deep pagination (over cap) must fall back to a full (unbounded) sort"
    );
    assert_eq!(
        sorter_top_n(&f, "SELECT id, v FROM t ORDER BY v, id LIMIT 10"),
        Some(10),
        "bare LIMIT 10 stays bounded to 10 (unchanged)"
    );
}
