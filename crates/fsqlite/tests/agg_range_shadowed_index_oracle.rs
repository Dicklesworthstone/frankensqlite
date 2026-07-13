//! bd-agg-range-shadowed-index: `COUNT(*)/SUM(x) WHERE a <range>` (a pure single-column range, no
//! residual) on an INTEGER-affinity indexed column must SEEK/walk the index, not full-scan the table —
//! even when a COMPOSITE `(a, b)` index is declared BEFORE the single-column `idx_a` and shadows it.
//!
//! The bug: `aggregate_index_range_seek_target`'s residual-free branch selected the index via
//! `index_for_column`, which returns the first leading-column match (the composite `idx_ab`), then
//! `filter(key_term_count == 1)?` early-returned None — so the whole aggregate fell back to a table
//! Rewind scan whenever a composite `(a, …)` index shadowed `idx_a`. The single-index-only schema was
//! unaffected (its lone index IS single-column), which is why this hid behind the composite. Fixed by
//! searching ALL indexes for a usable single-column ascending index (the same shadowing fix the IN-list
//! seek already carries in `index_integer_in_list_target`).
//!
//! HARD GATE: byte-identical to C SQLite (rusqlite) across upper-only / lower-only / bounded ranges,
//! `<`/`<=`/`>`/`>=`/BETWEEN, empty ranges, and NULL-bearing rows, on BOTH the single-index and the
//! composite-shadow schema. Opcode gate (shadow schema): every range aggregate opens an INDEX cursor
//! rather than table-scanning.

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

/// True if the plan opens an index cursor (an `OpenRead` whose P4 text is one of `idx_names`) — i.e.
/// it seeks/walks the index rather than table-scanning.
fn opens_index(conn: &Connection, sql: &str, idx_names: &[&str]) -> bool {
    conn.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .any(|row| {
            let vals = row.values();
            let is_open = matches!(vals.get(1), Some(SqliteValue::Text(op)) if op.to_string() == "OpenRead");
            is_open
                && vals.iter().any(|v| {
                    matches!(v, SqliteValue::Text(t) if idx_names.iter().any(|n| t.to_string() == *n))
                })
        })
}

/// True if the plan opens the base table `t` (a full-scan tell for a covering aggregate).
fn opens_table(conn: &Connection, sql: &str, table: &str) -> bool {
    conn.query(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .any(|row| {
            let vals = row.values();
            let is_open =
                matches!(vals.get(1), Some(SqliteValue::Text(op)) if op.to_string() == "OpenRead");
            is_open
                && vals
                    .iter()
                    .any(|v| matches!(v, SqliteValue::Text(t) if t.to_string() == table))
        })
}

const RANGES: &[&str] = &[
    "SELECT COUNT(*) FROM t WHERE a < 5",
    "SELECT COUNT(*) FROM t WHERE a <= 5",
    "SELECT COUNT(*) FROM t WHERE a > 30",
    "SELECT COUNT(*) FROM t WHERE a >= 30",
    "SELECT COUNT(*) FROM t WHERE a >= 5 AND a <= 9",
    "SELECT COUNT(*) FROM t WHERE a > 5 AND a < 9",
    "SELECT COUNT(*) FROM t WHERE a BETWEEN 5 AND 9",
    "SELECT SUM(x) FROM t WHERE a < 5",
    "SELECT SUM(x) FROM t WHERE a >= 5 AND a <= 9",
    "SELECT SUM(a) FROM t WHERE a < 5",
    "SELECT COUNT(*) FROM t WHERE a > 1000",
    "SELECT COALESCE(SUM(x), -1) FROM t WHERE a > 1000",
    "SELECT COUNT(*) FROM t WHERE a < 0",
];

fn check_schema(label: &str, ddl: &[&str], idx_names: &[&str]) {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in ddl {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    for i in 1..=3000_i64 {
        let a = (i * 7) % 40;
        let b = if i % 11 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 10)
        };
        let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {b}, {});", i % 100);
        f.execute(&stmt).unwrap();
        r.execute_batch(&stmt).unwrap();
    }

    for sql in RANGES {
        assert_eq!(
            frank_rows(&f, sql),
            sqlite_rows(&r, sql),
            "[{label}] diverged: `{sql}`"
        );
        // The whole point: a pure range aggregate must open an index, never table-scan.
        assert!(
            opens_index(&f, sql, idx_names),
            "[{label}] range aggregate must open an index (not table-scan): `{sql}`"
        );
    }

    // Covering COUNT(*) / SUM(indexed col) walk the index only — no table open.
    for sql in [
        "SELECT COUNT(*) FROM t WHERE a >= 5 AND a <= 9",
        "SELECT SUM(a) FROM t WHERE a < 5",
    ] {
        assert!(
            !opens_table(&f, sql, "t"),
            "[{label}] covering range aggregate must not open the table: `{sql}`"
        );
    }
    // SUM of a non-indexed column still needs the table lookup.
    assert!(
        opens_table(&f, "SELECT SUM(x) FROM t WHERE a < 5", "t"),
        "[{label}] SUM(non-indexed col) range must open the table"
    );
}

#[test]
fn agg_range_shadowed_index_matches_sqlite() {
    // Single-column index only: control (already seeked before the fix).
    check_schema(
        "single idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);",
            "CREATE INDEX idx_a ON t(a);",
        ],
        &["idx_a"],
    );
    // Composite index declared FIRST, shadowing idx_a: the regressing case. The single-column idx_a
    // must still be found and seeked.
    check_schema(
        "idx_ab shadows idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);",
            "CREATE INDEX idx_ab ON t(a, b);",
            "CREATE INDEX idx_a ON t(a);",
        ],
        &["idx_a"],
    );
}
