//! bd-count-distinct-index-walk: `SELECT COUNT(DISTINCT col) FROM t` over a single-column ASC BINARY
//! index walks the index counting non-NULL key-changes — no ephemeral dedup B-tree, no table read —
//! instead of a full scan that deduplicates every row.
//!
//! HARD GATE: byte-identical to C SQLite (rusqlite) for INTEGER and TEXT columns on both a single-index
//! and a composite-shadow schema (a composite `(col, …)` index declared before the single-column one
//! must not shadow it), across NULL-bearing data plus the edge cases empty / all-NULL / all-same /
//! all-distinct / single-row, and a NOCASE column (which declines to the scan, still byte-exact).
//! Opcode gate (shadow schema): the walk opens the single-column index and never opens the base table.

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

/// True if the plan opens an `OpenRead` whose P4 text equals `name`.
fn opens(conn: &Connection, sql: &str, name: &str) -> bool {
    conn.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .any(|row| {
            let vals = row.values();
            matches!(vals.get(1), Some(SqliteValue::Text(op)) if op.to_string() == "OpenRead")
                && vals
                    .iter()
                    .any(|v| matches!(v, SqliteValue::Text(t) if t.to_string() == name))
        })
}

fn setup(ddl: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in ddl {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    (f, r)
}

fn insert_both(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    f.execute(sql).unwrap();
    r.execute_batch(sql).unwrap();
}

fn cmp(f: &Connection, r: &rusqlite::Connection, sql: &str, label: &str) {
    assert_eq!(
        frank_rows(f, sql),
        sqlite_rows(r, sql),
        "[{label}] diverged: `{sql}`"
    );
}

/// Big-table battery: INTEGER `a` and TEXT `c`, both with NULLs. Opcode-gated to the index walk.
fn check_big(label: &str, ddl: &[&str]) {
    let (f, r) = setup(ddl);
    for i in 1..=3000_i64 {
        let a = if i % 13 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", (i * 7) % 40)
        };
        let c = if i % 17 == 0 {
            "NULL".to_owned()
        } else {
            format!("'k{}'", i % 25)
        };
        insert_both(
            &f,
            &r,
            &format!("INSERT INTO t VALUES ({i}, {a}, {c}, {});", i % 100),
        );
    }
    for (sql, idx) in [
        ("SELECT COUNT(DISTINCT a) FROM t", "idx_a"),
        ("SELECT COUNT(DISTINCT c) FROM t", "idx_c"),
    ] {
        cmp(&f, &r, sql, label);
        assert!(
            opens(&f, sql, idx),
            "[{label}] COUNT(DISTINCT) must walk {idx}: `{sql}`"
        );
        assert!(
            !opens(&f, sql, "t"),
            "[{label}] covering COUNT(DISTINCT) walk must not open the table: `{sql}`"
        );
    }
}

#[test]
fn count_distinct_index_walk_matches_sqlite() {
    // Single-column indexes: control.
    check_big(
        "single idx_a/idx_c",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c TEXT, b INTEGER);",
            "CREATE INDEX idx_a ON t(a);",
            "CREATE INDEX idx_c ON t(c);",
        ],
    );
    // Composite indexes declared FIRST, shadowing the single-column ones.
    check_big(
        "shadowed idx_a/idx_c",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c TEXT, b INTEGER);",
            "CREATE INDEX idx_ab ON t(a, b);",
            "CREATE INDEX idx_a ON t(a);",
            "CREATE INDEX idx_cb ON t(c, b);",
            "CREATE INDEX idx_c ON t(c);",
        ],
    );
}

#[test]
fn count_distinct_index_walk_edge_cases() {
    // Empty, all-NULL, all-same, all-distinct, single-row — the walk's boundary conditions.
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER);",
        "CREATE INDEX idx_a ON t(a);",
    ]);
    cmp(&f, &r, "SELECT COUNT(DISTINCT a) FROM t", "empty");
    insert_both(
        &f,
        &r,
        "INSERT INTO t VALUES (1, NULL), (2, NULL), (3, NULL);",
    );
    cmp(&f, &r, "SELECT COUNT(DISTINCT a) FROM t", "all-null");
    insert_both(&f, &r, "DELETE FROM t;");
    insert_both(
        &f,
        &r,
        "INSERT INTO t VALUES (1, 5), (2, 5), (3, 5), (4, 5);",
    );
    cmp(&f, &r, "SELECT COUNT(DISTINCT a) FROM t", "all-same");
    insert_both(&f, &r, "DELETE FROM t;");
    insert_both(&f, &r, "INSERT INTO t VALUES (1, 7);");
    cmp(&f, &r, "SELECT COUNT(DISTINCT a) FROM t", "single-row");
    insert_both(&f, &r, "DELETE FROM t;");
    for i in 1..=10_i64 {
        insert_both(&f, &r, &format!("INSERT INTO t VALUES ({i}, {i});"));
    }
    // Mix in leading NULLs before the distinct run.
    insert_both(&f, &r, "INSERT INTO t VALUES (100, NULL), (101, NULL);");
    cmp(
        &f,
        &r,
        "SELECT COUNT(DISTINCT a) FROM t",
        "all-distinct-plus-nulls",
    );
    assert!(
        opens(&f, "SELECT COUNT(DISTINCT a) FROM t", "idx_a"),
        "edge: COUNT(DISTINCT) must walk idx_a"
    );
}

#[test]
fn count_distinct_nocase_declines_but_matches() {
    // A NOCASE column: the BINARY index-walk plan must DECLINE (index order != DISTINCT grouping); the
    // full-scan path still produces the byte-exact case-folded distinct count.
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, c TEXT COLLATE NOCASE);",
        "CREATE INDEX idx_c ON t(c);",
    ]);
    for (i, v) in ["A", "a", "B", "b", "c", "C", "d"].iter().enumerate() {
        insert_both(&f, &r, &format!("INSERT INTO t VALUES ({}, '{v}');", i + 1));
    }
    cmp(&f, &r, "SELECT COUNT(DISTINCT c) FROM t", "nocase");
}
