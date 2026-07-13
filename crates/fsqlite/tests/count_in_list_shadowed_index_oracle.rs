//! bd-agg-range-shadowed-index (COUNT-IN mirror): `COUNT(*) WHERE c IN (<list>)` on a single-column
//! indexed column must seek the index per value (once the list reaches the once-materialized threshold,
//! 8), not full-scan the table — even when a COMPOSITE `(c, d)` index is declared BEFORE `idx_c` and
//! shadows it.
//!
//! `extract_count_indexed_in_target` selected the index via `index_for_column(&col).filter/decline on
//! key_term_count != 1` (here an explicit early `return None`), which the composite `idx_cd` triggered —
//! so a text/non-integer `COUNT(*) IN` materialized the list but still Rewind-scanned the whole table
//! for membership instead of SeekGE-ing `idx_c` per value. (Integer IN-lists route through the already
//! shadow-fixed `index_integer_in_list_target`, so this bit non-integer columns.) Fixed by searching all
//! indexes for a usable single-column ascending index.
//!
//! HARD GATE: byte-identical to C SQLite (rusqlite) on both a single-index and a composite-shadow
//! schema, across present / absent / mixed / duplicate list values, sub-threshold lists (which stay a
//! filtered scan), NULL-bearing rows, and a NULL list literal. Opcode gate (shadow schema): the
//! threshold-length seeks SeekGE `idx_c` and never opens the base table (covering COUNT(*)).

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

/// True if the plan opens an `OpenRead` whose P4 text equals `name` (an index name or the table name).
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

// >= 8 values (the once-materialized threshold) so the per-value seek engages.
const SEEKING: &[&str] = &[
    "SELECT COUNT(*) FROM w WHERE c IN ('k1','k2','k3','k4','k5','k6','k7','k8','k9')",
    "SELECT COUNT(*) FROM w WHERE c IN ('z0','z1','z2','z3','z4','z5','z6','z7','z8')",
    "SELECT COUNT(*) FROM w WHERE c IN ('k1','z1','k5','z2','k9','z3','k13','z4','k20')",
    "SELECT COUNT(*) FROM w WHERE c IN ('k1','k1','k2','k2','k3','k3','k4','k4','k5')",
];

// Correctness-only shapes (no seek gate): sub-threshold list (stays a filtered scan) and a NULL literal.
const CORRECTNESS_ONLY: &[&str] = &[
    "SELECT COUNT(*) FROM w WHERE c IN ('k1','k2')",
    "SELECT COUNT(*) FROM w WHERE c IN ('k1','k2','k3','k4','k5','k6','k7','k8',NULL)",
];

fn check_schema(label: &str, ddl: &[&str], seek_gated: bool) {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in ddl {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    for i in 1..=3000_i64 {
        // Some rows have NULL c — IN never matches NULL, must be excluded exactly as C SQLite does.
        let c = if i % 13 == 0 {
            "NULL".to_owned()
        } else {
            format!("'k{}'", i % 25)
        };
        let stmt = format!("INSERT INTO w VALUES ({i}, {c}, {});", i % 7);
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
    for sql in SEEKING.iter().chain(CORRECTNESS_ONLY) {
        cmp(sql);
    }

    if seek_gated {
        for sql in SEEKING {
            assert!(
                has_op(&f, sql, "SeekGE") && opens(&f, sql, "idx_c"),
                "[{label}] threshold COUNT(*) IN-list must SeekGE idx_c: `{sql}`"
            );
            assert!(
                !opens(&f, sql, "w"),
                "[{label}] covering COUNT(*) IN-list must not open the table: `{sql}`"
            );
        }
    }
}

#[test]
fn count_in_list_shadowed_index_matches_sqlite() {
    // Single-column TEXT index only: control (already seeked before the fix).
    check_schema(
        "single idx_c",
        &[
            "CREATE TABLE w (id INTEGER PRIMARY KEY, c TEXT, d INTEGER);",
            "CREATE INDEX idx_c ON w(c);",
        ],
        true,
    );
    // Composite index declared FIRST, shadowing idx_c: the regressing case.
    check_schema(
        "idx_cd shadows idx_c",
        &[
            "CREATE TABLE w (id INTEGER PRIMARY KEY, c TEXT, d INTEGER);",
            "CREATE INDEX idx_cd ON w(c, d);",
            "CREATE INDEX idx_c ON w(c);",
        ],
        true,
    );
}
