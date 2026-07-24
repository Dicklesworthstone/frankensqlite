//! bd-nax2y: `SELECT <cols> FROM t WHERE <second_col> = <const>` where the constrained column is the
//! SECOND term of a two-column index whose LEADING term is unconstrained (a MySQL-style SKIP SCAN, no
//! single-column index on the target column). The emitter enumerates distinct leading values and
//! per value seeks/walks the `(leading, const)` run. Byte-identical to C SQLite (compared as a sorted
//! set — no ORDER BY, so emission order is implementation-defined). Covers low- and high-cardinality
//! leading columns (walk vs seek paths), NULLs in both columns, covering and non-covering output,
//! duplicate `(leading, const)` runs, and the empty result. A single-column index on the target
//! column, or a leading-column constraint, must decline the skip scan.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(_) => "blob".to_owned(),
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
                rusqlite::types::Value::Blob(_) => "blob".to_owned(),
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

fn both(ddl: &[&str], inserts: &[String]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in ddl {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    for stmt in inserts {
        f.execute(stmt).unwrap();
        r.execute_batch(stmt).unwrap();
    }
    (f, r)
}

fn cmp(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let mut a = frank_rows(f, sql);
    let mut b = sqlite_rows(r, sql);
    a.sort();
    b.sort();
    assert_eq!(a, b, "diverged: `{sql}`");
}

#[test]
fn skip_scan_low_cardinality_leading_matches_sqlite() {
    // Few distinct leading `x` values (12), many rows each -> the seek path dominates. NULLs in both.
    let mut ins = Vec::new();
    for i in 1..=4000_i64 {
        let x = if i % 53 == 0 {
            "NULL".to_owned()
        } else {
            (i % 12).to_string()
        };
        let a = if i % 37 == 0 {
            "NULL".to_owned()
        } else {
            (i % 8).to_string()
        };
        ins.push(format!("INSERT INTO t VALUES ({i}, {x}, {a}, {});", i % 4));
    }
    let (f, r) = both(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, a INTEGER, c INTEGER);",
            "CREATE INDEX idx_xa ON t(x, a);",
        ],
        &ins,
    );
    // The skip scan must fire: it emits a SeekGT to advance between distinct leading values (a full
    // scan never does), and never opens/rewinds the base table for a covering query.
    assert!(
        has_op(&f, "SELECT x, a FROM t WHERE a = 5", "SeekGT"),
        "skip scan should serve WHERE a = 5 over idx_xa(x, a) (SeekGT advance present)"
    );
    for a in [0, 3, 5, 7, 999] {
        cmp(&f, &r, &format!("SELECT x, a FROM t WHERE a = {a}")); // covering
        cmp(&f, &r, &format!("SELECT id, c FROM t WHERE a = {a}")); // non-covering
        cmp(
            &f,
            &r,
            &format!("SELECT COUNT(id) FROM t WHERE a = {a} GROUP BY x"),
        ); // sanity via a different shape
    }
}

#[test]
fn skip_scan_high_cardinality_leading_matches_sqlite() {
    // Near-unique leading `x` (every row a distinct x) -> the adaptive walk path dominates (never
    // worse than a full covering scan). The results must still be byte-exact.
    let mut ins = Vec::new();
    for i in 1..=3000_i64 {
        // x is unique per row; a repeats over a small domain.
        ins.push(format!(
            "INSERT INTO t VALUES ({i}, {i}, {}, {});",
            i % 6,
            i % 3
        ));
    }
    let (f, r) = both(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, a INTEGER, c INTEGER);",
            "CREATE INDEX idx_xa ON t(x, a);",
        ],
        &ins,
    );
    assert!(has_op(&f, "SELECT id FROM t WHERE a = 2", "SeekGT"));
    for a in [0, 2, 4, 5, 100] {
        cmp(&f, &r, &format!("SELECT x, a FROM t WHERE a = {a}"));
        cmp(&f, &r, &format!("SELECT id FROM t WHERE a = {a}"));
    }
}

#[test]
fn skip_scan_text_and_dup_runs_match_sqlite() {
    // TEXT second column + long duplicate (x, const) runs (many rows share the exact (x, s) tuple).
    let mut ins = Vec::new();
    for i in 1..=2500_i64 {
        let x = i % 20;
        let s = if i % 41 == 0 {
            "NULL".to_owned()
        } else {
            format!("'s{}'", i % 5)
        };
        ins.push(format!("INSERT INTO t VALUES ({i}, {x}, {s}, {});", i % 4));
    }
    let (f, r) = both(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, s TEXT, c INTEGER);",
            "CREATE INDEX idx_xs ON t(x, s);",
        ],
        &ins,
    );
    assert!(has_op(&f, "SELECT x, s FROM t WHERE s = 's2'", "SeekGT"));
    for s in ["s0", "s2", "s4", "nope"] {
        cmp(&f, &r, &format!("SELECT x, s FROM t WHERE s = '{s}'"));
        cmp(&f, &r, &format!("SELECT id, c FROM t WHERE s = '{s}'"));
    }
}

#[test]
fn skip_scan_edge_cases_match_sqlite() {
    // Empty table, all-same-leading, all-NULL-target, single row.
    for (label, ddl, rows) in [
        (
            "empty",
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, a INTEGER);",
            vec![],
        ),
        (
            "all-same-x",
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, a INTEGER);",
            (1..=100)
                .map(|i| format!("INSERT INTO t VALUES ({i}, 7, {});", i % 5))
                .collect::<Vec<_>>(),
        ),
        (
            "all-null-a",
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, a INTEGER);",
            (1..=100)
                .map(|i| format!("INSERT INTO t VALUES ({i}, {}, NULL);", i % 10))
                .collect(),
        ),
        (
            "single",
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, a INTEGER);",
            vec!["INSERT INTO t VALUES (1, 3, 9);".to_owned()],
        ),
    ] {
        let (f, r) = both(&[ddl, "CREATE INDEX idx_xa ON t(x, a);"], &rows);
        for a in [3, 9, 0] {
            cmp(&f, &r, &format!("SELECT x, a FROM t WHERE a = {a}"));
        }
        let _ = label;
    }
}

#[test]
fn skip_scan_three_column_index_matches_sqlite() {
    // `WHERE a = <const>` where `a` is the SECOND term of a THREE-column index `idx(x, a, c)`: `x` is
    // the unconstrained leading term, `c` is an unconstrained TRAILING term. The seek must fill `c` and
    // the rowid with the MIN sentinel so it lands on the first `(x, a, *)`, and the run emits every
    // matching row across all `c`. Low- and high-cardinality leading `x`, NULLs in `x`, `a`, and `c`.
    let mut ins = Vec::new();
    for i in 1..=4000_i64 {
        let x = if i % 53 == 0 {
            "NULL".to_owned()
        } else {
            (i % 15).to_string()
        };
        let a = if i % 37 == 0 {
            "NULL".to_owned()
        } else {
            (i % 8).to_string()
        };
        let c = if i % 29 == 0 {
            "NULL".to_owned()
        } else {
            (i % 6).to_string()
        };
        ins.push(format!("INSERT INTO t VALUES ({i}, {x}, {a}, {c});"));
    }
    let (f, r) = both(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, a INTEGER, c INTEGER);",
            "CREATE INDEX idx_xac ON t(x, a, c);",
        ],
        &ins,
    );
    // Skip scan fires over the 3-col index (SeekGT advance between distinct leading values), and a
    // covering query (x, a, c all in the index) never opens the base table.
    assert!(
        has_op(&f, "SELECT x, a, c FROM t WHERE a = 5", "SeekGT"),
        "skip scan should serve WHERE a = 5 over idx_xac(x, a, c)"
    );
    for a in [0, 3, 5, 7, 999] {
        cmp(&f, &r, &format!("SELECT x, a, c FROM t WHERE a = {a}")); // covering (all 3 key cols)
        cmp(&f, &r, &format!("SELECT id FROM t WHERE a = {a}")); // non-covering
        cmp(&f, &r, &format!("SELECT x, c FROM t WHERE a = {a}")); // covering subset
    }
}

#[test]
fn skip_scan_declines_when_avoidable() {
    let (f, r) = both(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, a INTEGER);",
            "CREATE INDEX idx_xa ON t(x, a);",
            "CREATE INDEX idx_a ON t(a);", // single-column index on the target -> equality seek wins
        ],
        &(1..=500)
            .map(|i| format!("INSERT INTO t VALUES ({i}, {}, {});", i % 10, i % 7))
            .collect::<Vec<_>>(),
    );
    // A single-column index on `a` exists: the equality seek serves it; the skip scan must decline.
    assert!(
        !has_op(&f, "SELECT x, a FROM t WHERE a = 3", "SeekGT"),
        "skip scan must decline when a single-column index on the target column exists"
    );
    cmp(&f, &r, "SELECT x, a FROM t WHERE a = 3");
    // A constraint on the LEADING term is a normal composite equality/prefix seek, not a skip scan.
    cmp(&f, &r, "SELECT x, a FROM t WHERE x = 4");
    cmp(&f, &r, "SELECT x, a FROM t WHERE x = 4 AND a = 3");
}
