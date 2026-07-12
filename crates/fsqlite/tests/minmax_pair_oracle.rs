//! bd-minmax-pair-seek: `SELECT MIN(col), MAX(col)` (in either order) over one indexed column now
//! resolves to TWO index-end seeks instead of a full scan. HARD GATE: byte-identical to C SQLite
//! (rusqlite) across ASC and DESC indexes, leading `col`-NULLs (MIN skips), all-NULL and empty tables,
//! either SELECT order, and an unindexed control (declines to the scan, must still match). Opcode gate:
//! the indexed pair emits `Last` (an index-end seek); the unindexed control does not.

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
fn minmax_pair_seek_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, w INTEGER, u INTEGER);",
        "CREATE INDEX idx_v ON t(v);",           // ASC single-column
        "CREATE INDEX idx_w_desc ON t(w DESC);", // DESC single-column
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    // Leading v-NULLs (ids 1..=5) so MIN(v) skips them; u is NOT indexed (control).
    for i in 1..=500_i64 {
        let has_null = i <= 5;
        let v = if has_null {
            "NULL".to_owned()
        } else {
            format!("{}", ((i.wrapping_mul(7)) % 300) - 120)
        };
        let w = ((i.wrapping_mul(13)) % 400) - 150;
        let u = ((i.wrapping_mul(17)) % 260) - 100;
        let stmt = format!("INSERT INTO t VALUES ({i}, {v}, {w}, {u});");
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
        // ASC index, both SELECT orders (with a leading NULL region for MIN).
        "SELECT MIN(v), MAX(v) FROM t",
        "SELECT MAX(v), MIN(v) FROM t",
        // DESC index.
        "SELECT MIN(w), MAX(w) FROM t",
        "SELECT MAX(w), MIN(w) FROM t",
        // Unindexed control -> declines, must match.
        "SELECT MIN(u), MAX(u) FROM t",
        "SELECT MAX(u), MIN(u) FROM t",
    ] {
        cmp(sql);
    }

    // All-NULL column: MIN, MAX both NULL.
    for stmt in [
        "CREATE TABLE n (id INTEGER PRIMARY KEY, v INTEGER);",
        "CREATE INDEX idx_nv ON n(v);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    for i in 1..=40 {
        let stmt = format!("INSERT INTO n VALUES ({i}, NULL);");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }
    cmp("SELECT MIN(v), MAX(v) FROM n");

    // Empty table.
    for stmt in [
        "CREATE TABLE e (id INTEGER PRIMARY KEY, v INTEGER);",
        "CREATE INDEX idx_ev ON e(v);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    cmp("SELECT MIN(v), MAX(v) FROM e");
    cmp("SELECT MAX(v), MIN(v) FROM e");

    // Opcode gate: indexed pair seeks the index ends (Last); the unindexed control full-scans.
    assert!(
        has_op(&f, "SELECT MIN(v), MAX(v) FROM t", "Last"),
        "MIN(v), MAX(v) (indexed) must seek the index ends (Last)"
    );
    assert!(
        !has_op(&f, "SELECT MIN(u), MAX(u) FROM t", "Last"),
        "MIN(u), MAX(u) (unindexed) must full-scan (no index-end seek)"
    );
}
