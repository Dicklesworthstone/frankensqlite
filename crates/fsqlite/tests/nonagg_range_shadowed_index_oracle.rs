//! bd-agg-range-shadowed-index (non-aggregate mirror): `SELECT <cols> FROM t WHERE a <range>` must
//! seek/walk the single-column index `idx_a`, not full-scan the table — even when a COMPOSITE `(a, b)`
//! index is declared BEFORE `idx_a` and shadows it in `index_for_column`.
//!
//! Same bug as the aggregate range seek, but in `codegen_select`'s two single-column range planners:
//! the ascending `index_range` (line ~1905, fires for no-ORDER-BY and a deterministic `ORDER BY a, id`)
//! and the descending `index_range_desc` (line ~2087, keyset `ORDER BY a DESC, id DESC`). Both selected
//! the index via `index_for_column(&col).filter(key_term_count == 1)?`, which early-returned None when
//! a composite `(a, …)` index shadowed `idx_a`, so the scan fell back to a full table Rewind. Fixed by
//! searching ALL indexes for a usable single-column ascending index (same idiom as the IN-list seek).
//!
//! HARD GATE: byte-identical to C SQLite (rusqlite) on both a single-index and a composite-shadow
//! schema, for ordered ranges (`ORDER BY a, id` ASC and `ORDER BY a DESC, id DESC`) and — compared as
//! sorted sets — bare no-ORDER-BY ranges, covering and non-covering, with NULL-bearing rows. Opcode
//! gate (shadow schema): every range scan opens `idx_a`, never the shadowing `idx_ab` alone/table scan.

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

/// True if the plan opens an `OpenRead` whose P4 text is one of `idx_names`.
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

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

// Range queries with an index-native deterministic order — these must match byte-exact WITH order.
const ORDERED: &[&str] = &[
    "SELECT id, a FROM t WHERE a < 5 ORDER BY a, id",
    "SELECT id, b FROM t WHERE a >= 5 AND a <= 9 ORDER BY a, id",
    "SELECT id, a, b, x FROM t WHERE a > 30 ORDER BY a, id",
    "SELECT id FROM t WHERE a BETWEEN 5 AND 9 ORDER BY a, id",
    "SELECT id, a FROM t WHERE a <= 0 ORDER BY a, id",
    // Descending keyset (index_range_desc).
    "SELECT id, a FROM t WHERE a < 5 ORDER BY a DESC, id DESC",
    "SELECT id, b FROM t WHERE a >= 5 AND a <= 9 ORDER BY a DESC, id DESC",
];

// Bare no-ORDER-BY ranges — order is unspecified, so compare as sorted sets. Still opcode-gated to seek.
const UNORDERED: &[&str] = &[
    "SELECT id FROM t WHERE a < 5",
    "SELECT id, b FROM t WHERE a >= 5 AND a <= 9",
    "SELECT id, a, x FROM t WHERE a > 30",
    "SELECT id FROM t WHERE a BETWEEN 5 AND 9",
];

fn check_schema(label: &str, ddl: &[&str]) {
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

    for sql in ORDERED {
        assert_eq!(
            frank_rows(&f, sql),
            sqlite_rows(&r, sql),
            "[{label}] ordered diverged: `{sql}`"
        );
        assert!(
            opens_index(&f, sql, &["idx_a"]),
            "[{label}] range scan must open idx_a: `{sql}`"
        );
    }
    for sql in UNORDERED {
        assert_eq!(
            sorted(frank_rows(&f, sql)),
            sorted(sqlite_rows(&r, sql)),
            "[{label}] unordered set diverged: `{sql}`"
        );
        assert!(
            opens_index(&f, sql, &["idx_a"]),
            "[{label}] bare range scan must open idx_a (not table-scan): `{sql}`"
        );
    }
}

#[test]
fn nonagg_range_shadowed_index_matches_sqlite() {
    // Single-column index only: control (already seeked before the fix).
    check_schema(
        "single idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);",
            "CREATE INDEX idx_a ON t(a);",
        ],
    );
    // Composite index declared FIRST, shadowing idx_a: the regressing case.
    check_schema(
        "idx_ab shadows idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);",
            "CREATE INDEX idx_ab ON t(a, b);",
            "CREATE INDEX idx_a ON t(a);",
        ],
    );
}
