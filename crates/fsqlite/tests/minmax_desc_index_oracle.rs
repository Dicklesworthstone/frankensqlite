//! bd-minmax-index-seek follow-on: `SELECT MAX(v)/MIN(v)` on a DESC-indexed column (or a DESC-leading
//! composite index) now seeks the index end (MAX → first entry, MIN → last non-NULL walking back),
//! instead of full-scanning. A DESC index stores the extremum at the opposite end from ASC and puts
//! NULLs at the bottom. HARD GATE: byte-identical to C SQLite (rusqlite) across trailing `v`-NULLs
//! (MIN must skip them from the end), all-NULL and empty tables, the COALESCE wrapper, and a NOCASE
//! DESC index (declines to the scan, must still match). Opcode gate: MIN(v DESC) uses `Prev`; MAX(v
//! DESC) is a single seek (no scan `Next`); NOCASE declines.

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
fn minmax_desc_index_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    let schema = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, w INTEGER, u INTEGER, nc TEXT COLLATE NOCASE);",
        "CREATE INDEX idx_v_desc ON t(v DESC);", // DESC single-column
        "CREATE INDEX idx_w_desc ON t(w DESC, u);", // DESC-leading composite
        "CREATE INDEX idx_nc_desc ON t(nc DESC);", // NOCASE DESC -> declines
    ];
    for stmt in schema {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    // Some v-NULLs (ids 1..=6) so MIN(v) must skip the NULL region (at the DESC index bottom).
    for i in 1..=500_i64 {
        let has_null = i <= 6;
        let v = if has_null {
            "NULL".to_owned()
        } else {
            format!("{}", ((i.wrapping_mul(7)) % 300) - 120)
        };
        let w = ((i.wrapping_mul(13)) % 400) - 150;
        let u = i % 50;
        let nc = if i % 2 == 0 {
            format!("'K{:02}'", (i.wrapping_mul(11)) % 50)
        } else {
            format!("'k{:02}'", (i.wrapping_mul(11)) % 50)
        };
        let stmt = format!("INSERT INTO t VALUES ({i}, {v}, {w}, {u}, {nc});");
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
        // DESC single-column v (with trailing NULL region for MIN).
        "SELECT MAX(v) FROM t",
        "SELECT MIN(v) FROM t",
        "SELECT COALESCE(MAX(v), -1) FROM t",
        "SELECT COALESCE(MIN(v), -1) FROM t",
        // DESC-leading composite w.
        "SELECT MAX(w) FROM t",
        "SELECT MIN(w) FROM t",
        // NOCASE DESC nc -> declines to the scan, must still match.
        "SELECT MAX(nc) FROM t",
        "SELECT MIN(nc) FROM t",
    ] {
        cmp(sql);
    }

    // All-NULL DESC-indexed column: MIN/MAX -> NULL.
    for stmt in [
        "CREATE TABLE n (id INTEGER PRIMARY KEY, v INTEGER);",
        "CREATE INDEX idx_nv_desc ON n(v DESC);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    for i in 1..=40 {
        let stmt = format!("INSERT INTO n VALUES ({i}, NULL);");
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }
    cmp("SELECT MAX(v) FROM n");
    cmp("SELECT MIN(v) FROM n");

    // Empty DESC-indexed table.
    for stmt in [
        "CREATE TABLE e (id INTEGER PRIMARY KEY, v INTEGER);",
        "CREATE INDEX idx_ev_desc ON e(v DESC);",
    ] {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    cmp("SELECT MAX(v) FROM e");
    cmp("SELECT MIN(v) FROM e");

    // Opcode gate.
    assert!(
        has_op(&f, "SELECT MIN(v) FROM t", "Prev"),
        "MIN(v) on a DESC index must walk back from the end with Prev"
    );
    assert!(
        !has_op(&f, "SELECT MAX(v) FROM t", "Next"),
        "MAX(v) on a DESC index must be a single seek (no scan Next)"
    );
    assert!(
        has_op(&f, "SELECT MAX(nc) FROM t", "Next"),
        "MAX(nc) (NOCASE DESC) must decline to the full scan"
    );
    assert!(
        !has_op(&f, "SELECT MIN(nc) FROM t", "Prev"),
        "MIN(nc) (NOCASE DESC) must decline the DESC seek"
    );
}
