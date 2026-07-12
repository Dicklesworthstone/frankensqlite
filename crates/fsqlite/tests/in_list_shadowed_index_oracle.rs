//! bd-2dgf5 follow-up: `COUNT(*)/SUM(x) WHERE a IN (<ints>)` on an INTEGER-affinity indexed column
//! must SEEK the index, not full-scan. Two fixes exercised:
//!   1. `simple_count_star` yields `COUNT(*) WHERE a IN (list)` to `codegen_select_aggregate` (the
//!      path with the IN-list seek); previously it stayed in `codegen_select_count_star`, which has no
//!      IN-list path and scanned.
//!   2. the index selection finds a single-column `idx_a` even when a composite `idx_ab (a,b)` is
//!      declared first and shadows it in `index_for_column`.
//! HARD GATE: byte-identical to C SQLite (rusqlite) on both a single-index and a composite-shadow
//! schema, across present/absent/duplicate list values, both aggregates, COALESCE, and (shadow schema)
//! rows whose `b IS NULL`. Opcode gate: COUNT(*) and SUM(x) both SeekGE the index on each schema.

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

/// Set up matching frank + sqlite connections with `ddl`, populate 3000 rows, run the byte-exact
/// battery, and gate that both aggregates seek.
fn check_schema(label: &str, ddl: &[&str]) {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in ddl {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    for i in 1..=3000_i64 {
        let a = (i * 7) % 40;
        // b includes NULLs — the single-column-index path must count these rows for their `a`.
        let b = if i % 11 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 10)
        };
        let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {});", i % 100);
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }

    let cmp = |sql: &str| {
        assert_eq!(
            frank_rows(&f, sql),
            sqlite_rows(&r, sql),
            "[{label}] diverged: `{sql}`"
        );
    };
    for sql in [
        "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11)",
        "SELECT SUM(x) FROM t WHERE a IN (3, 7, 11)",
        "SELECT COUNT(*) FROM t WHERE a IN (7)",
        "SELECT COUNT(*) FROM t WHERE a IN (7, 999, 1000)",
        "SELECT SUM(x) FROM t WHERE a IN (999, 1000)",
        "SELECT COALESCE(SUM(x), -1) FROM t WHERE a IN (999, 1000)",
        "SELECT COUNT(*) FROM t WHERE a IN (3, 3, 7)",
        "SELECT COUNT(*) FROM t WHERE a IN (0, 11, 22, 33)",
        // Covering SUM of the *indexed* column (read straight from the index, no table lookup).
        "SELECT SUM(a) FROM t WHERE a IN (3, 7, 11)",
    ] {
        cmp(sql);
    }

    // Opcode gate: both aggregates seek (COUNT(*) now yields to the aggregate seek path).
    assert!(
        has_op(&f, "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11)", "SeekGE"),
        "[{label}] COUNT(*) WHERE a IN (list) must seek the index"
    );
    assert!(
        has_op(&f, "SELECT SUM(x) FROM t WHERE a IN (3, 7, 11)", "SeekGE"),
        "[{label}] SUM(x) WHERE a IN (list) must seek the index"
    );
    // Covering gate: COUNT(*) and SUM(indexed col) accumulate straight off the index (no SeekRowid);
    // SUM of a non-indexed column still needs the table lookup.
    assert!(
        !has_op(
            &f,
            "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11)",
            "SeekRowid"
        ),
        "[{label}] COUNT(*) IN-list walk must be covering (no SeekRowid)"
    );
    assert!(
        !has_op(
            &f,
            "SELECT SUM(a) FROM t WHERE a IN (3, 7, 11)",
            "SeekRowid"
        ),
        "[{label}] SUM(indexed col) IN-list walk must be covering (no SeekRowid)"
    );
    assert!(
        has_op(
            &f,
            "SELECT SUM(x) FROM t WHERE a IN (3, 7, 11)",
            "SeekRowid"
        ),
        "[{label}] SUM(non-indexed col) IN-list walk must open the table (SeekRowid)"
    );
}

#[test]
fn in_list_seek_matches_sqlite() {
    // Single-column index: exercises the `simple_count_star` yield fix.
    check_schema(
        "single idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);",
            "CREATE INDEX idx_a ON t(a);",
        ],
    );
    // Composite index declared FIRST, shadowing idx_a: exercises the index-selection fix too.
    check_schema(
        "idx_ab shadows idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);",
            "CREATE INDEX idx_ab ON t(a, b);",
            "CREATE INDEX idx_a ON t(a);",
        ],
    );
}
