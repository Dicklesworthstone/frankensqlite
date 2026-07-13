//! bd-agg-in-list-residual: `COUNT(*)/SUM(x) WHERE a IN (<ints>) AND <residual>` on an
//! INTEGER-affinity indexed column must SEEK the index's IN-list runs and re-apply the remaining
//! predicates as a residual filter, NOT full-scan. This ungates the routing that was disabled in
//! `1606d1fb` pending the COUNT(*) routing fix: `codegen_select_count_star` now yields these shapes to
//! `codegen_select_aggregate`, whose IN-list seek path (`index_integer_in_list_residual_target`) drives
//! `AggStep` from the seeked runs after `emit_where_filter` rejects rows failing the residual.
//!
//! HARD GATE: byte-identical to C SQLite (rusqlite) across residuals on a non-indexed column, on the
//! composite index's second column (with NULLs), comparison / BETWEEN / IN residuals, and residuals
//! that match nothing — on both a single-column and a composite-shadow schema, for COUNT(*), SUM, and
//! COALESCE(SUM). Opcode gate: every residual shape still SeekGE's the index (no Rewind full-scan).
//!
//! Companion to `in_list_shadowed_index_oracle.rs` (the residual-FREE IN-list seek); this file is the
//! `has_residual == true` arm of the same lever.

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
/// battery over IN-list + residual shapes, and gate that each still seeks (never Rewind full-scans).
fn check_schema(label: &str, ddl: &[&str]) {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in ddl {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    for i in 1..=3000_i64 {
        let a = (i * 7) % 40;
        // b includes NULLs — a residual on `b` must treat NULL rows exactly as C SQLite does.
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

    // Residual on a NON-indexed column, comparison / BETWEEN / IN / equality / inequality residuals,
    // residuals that reference the composite index's second column (with NULLs), and residuals that
    // match nothing. Order of conjuncts is varied so residual extraction is exercised on both sides.
    let queries = [
        "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11) AND x > 50",
        "SELECT COUNT(*) FROM t WHERE x > 50 AND a IN (3, 7, 11)",
        "SELECT SUM(x) FROM t WHERE a IN (3, 7, 11) AND x > 50",
        "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11) AND x BETWEEN 10 AND 60",
        "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11) AND x IN (5, 15, 25, 35)",
        "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11) AND b = 3",
        "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11) AND b IS NULL",
        "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11) AND b IS NOT NULL",
        "SELECT SUM(x) FROM t WHERE a IN (0, 7, 14, 21) AND b <> 5",
        "SELECT COUNT(*) FROM t WHERE a IN (3, 3, 7) AND x <= 20",
        // Residual filters the whole result out (present list values, impossible residual).
        "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11) AND x > 1000000",
        "SELECT COALESCE(SUM(x), -1) FROM t WHERE a IN (3, 7, 11) AND x > 1000000",
        // Absent + duplicate list values combined with a residual.
        "SELECT COUNT(*) FROM t WHERE a IN (7, 999, 1000) AND x >= 0",
        "SELECT SUM(x) FROM t WHERE a IN (999, 1000) AND x > 0",
    ];
    for sql in queries {
        cmp(sql);
    }

    // Opcode gate: a residual must not defeat the seek — every shape still SeekGE's the index and
    // never falls back to a Rewind full table scan.
    for sql in [
        "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11) AND x > 50",
        "SELECT SUM(x) FROM t WHERE a IN (3, 7, 11) AND x > 50",
        "SELECT COUNT(*) FROM t WHERE a IN (3, 7, 11) AND b IS NULL",
    ] {
        assert!(
            has_op(&f, sql, "SeekGE"),
            "[{label}] IN-list + residual must seek the index: `{sql}`"
        );
        assert!(
            !has_op(&f, sql, "Rewind"),
            "[{label}] IN-list + residual must not full-scan (Rewind): `{sql}`"
        );
    }
}

#[test]
fn in_list_residual_seek_matches_sqlite() {
    // Single-column index: the residual is applied over rows seeked by `idx_a`.
    check_schema(
        "single idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);",
            "CREATE INDEX idx_a ON t(a);",
        ],
    );
    // Composite index declared FIRST, shadowing idx_a: residuals on `b` (the second column) and on
    // the non-indexed `x` must both re-apply correctly after the IN-list seek.
    check_schema(
        "idx_ab shadows idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, x INTEGER);",
            "CREATE INDEX idx_ab ON t(a, b);",
            "CREATE INDEX idx_a ON t(a);",
        ],
    );
}
