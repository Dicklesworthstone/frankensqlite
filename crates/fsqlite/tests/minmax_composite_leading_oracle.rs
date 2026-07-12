//! bd-minmax-index-seek follow-on: `SELECT MAX(a)/MIN(a)` where `a` is the LEADING key term of a
//! composite `(a, b)` index (but has no single-column index) now seeks the index end (Column 0),
//! instead of full-scanning. HARD GATE: byte-identical to C SQLite (rusqlite) across leading `a`-NULLs
//! (MIN skips), all-NULL and empty tables, the COALESCE wrapper, and the decline cases (a NOCASE
//! leading term) which fall to the scan and must still match. Plus an opcode gate: the fast path emits
//! `Last` (MAX) off the composite index; the NOCASE leading term does not.

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

/// True when the EXPLAIN of `sql` contains the given opcode name.
fn has_op(conn: &Connection, sql: &str, want: &str) -> bool {
    conn.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(
        |row| matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string() == want),
    )
}

#[test]
fn minmax_composite_leading_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    let schema = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, nc TEXT COLLATE NOCASE, nb INTEGER, s INTEGER);",
        "CREATE INDEX idx_ab ON t(a, b);", // a leading (composite), no single-col index on a
        "CREATE INDEX idx_ncnb ON t(nc, nb);", // NOCASE leading -> MAX(nc) declines
        "CREATE INDEX idx_s ON t(s);",     // single-col control (existing path)
    ];
    for stmt in schema {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    // Leading a-NULLs (ids 1..=5) so MIN(a) skips them; pseudo-shuffled otherwise.
    for i in 1..=500_i64 {
        let has_null = i <= 5;
        let a = if has_null {
            "NULL".to_owned()
        } else {
            format!("{}", ((i.wrapping_mul(7)) % 300) - 120)
        };
        let bb = (i.wrapping_mul(13)) % 400;
        let nc = if i % 2 == 0 {
            format!("'K{:02}'", (i.wrapping_mul(11)) % 50)
        } else {
            format!("'k{:02}'", (i.wrapping_mul(11)) % 50)
        };
        let nb = (i.wrapping_mul(3)) % 200;
        let s = ((i.wrapping_mul(17)) % 260) - 100;
        let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {bb}, {nc}, {nb}, {s});");
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
        // Leading composite term a: seek the index end (Column 0).
        "SELECT MAX(a) FROM t",
        "SELECT MIN(a) FROM t",
        "SELECT COALESCE(MAX(a), -1) FROM t",
        "SELECT COALESCE(MIN(a), -1) FROM t",
        // NOCASE leading term nc -> declines to the scan, must still match.
        "SELECT MAX(nc) FROM t",
        "SELECT MIN(nc) FROM t",
        // Single-column indexed s -> existing path, must match.
        "SELECT MAX(s) FROM t",
        "SELECT MIN(s) FROM t",
    ] {
        cmp(sql);
    }

    // All-NULL leading column: MIN/MAX -> NULL.
    for stmt in [
        "CREATE TABLE n (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
        "CREATE INDEX idx_nab ON n(a, b);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    for i in 1..=40 {
        let stmt = format!("INSERT INTO n VALUES ({i}, NULL, {i});");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }
    cmp("SELECT MAX(a) FROM n");
    cmp("SELECT MIN(a) FROM n");

    // Empty table.
    for stmt in [
        "CREATE TABLE e (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
        "CREATE INDEX idx_eab ON e(a, b);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    cmp("SELECT MAX(a) FROM e");
    cmp("SELECT MIN(a) FROM e");

    // Opcode gate: MAX(a) seeks the composite index end (Last); the NOCASE leading term does not.
    assert!(
        has_op(&f, "SELECT MAX(a) FROM t", "Last"),
        "MAX(a) (composite leading term) must seek the index end (Last)"
    );
    assert!(
        !has_op(&f, "SELECT MAX(nc) FROM t", "Last"),
        "MAX(nc) (NOCASE leading term) must decline the index-end seek"
    );
}
