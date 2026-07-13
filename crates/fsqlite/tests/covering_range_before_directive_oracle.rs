//! bd-covering-range-before-directive: `SELECT <covering cols> FROM t WHERE col <range>` (no ORDER BY)
//! seeks the single-column index's matching slice instead of honoring the planner's FullTableScan
//! directive and reading every row. Covering only (output = rowid / index columns, no table read), so
//! the index-range walk is strictly cheaper than the full scan and cannot pessimize a non-selective
//! range. Rows come back index-ordered (spec-legal without ORDER BY), so the oracle compares result
//! SETS (sorted), and asserts the covering plan opens the index but never the base table.

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
    let mut rows: Vec<Vec<String>> = conn
        .query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    rows.sort();
    rows
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    let mut rows: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                    rusqlite::types::Value::Null => "NULL".to_owned(),
                    rusqlite::types::Value::Integer(x) => x.to_string(),
                    rusqlite::types::Value::Real(f) => format!("{f:?}"),
                    rusqlite::types::Value::Text(s) => format!("'{s}'"),
                    rusqlite::types::Value::Blob(b) => {
                        format!(
                            "X'{}'",
                            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                        )
                    }
                });
            }
            Ok(out)
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    rows.sort();
    rows
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

fn check(label: &str, ddl: &[&str]) {
    let (f, r) = setup(ddl);
    for i in 1..=400_i64 {
        // a: 0..39, with a NULL every 13th row; x is not indexed.
        let a = if i % 13 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 40)
        };
        insert_both(
            &f,
            &r,
            &format!("INSERT INTO t VALUES ({i}, {a}, {});", i * 3),
        );
    }
    // Covering ranges (output = rowid / indexed column): must seek idx_a, never open table t.
    let covering = [
        "SELECT id FROM t WHERE a < 5",
        "SELECT id FROM t WHERE a <= 5",
        "SELECT id FROM t WHERE a > 35",
        "SELECT id FROM t WHERE a >= 35",
        "SELECT id FROM t WHERE a BETWEEN 10 AND 12",
        "SELECT a FROM t WHERE a < 3",
        "SELECT a, id FROM t WHERE a > 37",
        "SELECT id FROM t WHERE a < 999", // non-selective: still covering, still <= full scan
    ];
    for sql in covering {
        cmp(&f, &r, sql, label);
        assert!(
            opens(&f, sql, "idx_a"),
            "[{label}] covering range must seek idx_a: `{sql}`"
        );
        assert!(
            !opens(&f, sql, "t"),
            "[{label}] covering range must not open table t: `{sql}`"
        );
    }
    // Non-covering (x not in idx_a): correctness only — must NOT be forced onto the seek's per-row
    // table lookups. We assert byte-set correctness; the plan is left to the planner (full scan).
    for sql in [
        "SELECT x FROM t WHERE a < 5",
        "SELECT id, x FROM t WHERE a BETWEEN 10 AND 12",
    ] {
        cmp(&f, &r, sql, label);
    }
}

#[test]
fn covering_range_before_directive_matches_sqlite() {
    check(
        "single idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, x INTEGER);",
            "CREATE INDEX idx_a ON t(a);",
        ],
    );
    // Composite (a,b) declared FIRST must not shadow the single-column idx_a.
    check(
        "shadowed idx_a",
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, x INTEGER);",
            "CREATE INDEX idx_ax ON t(a, x);",
            "CREATE INDEX idx_a ON t(a);",
        ],
    );
}

#[test]
fn covering_range_edge_cases() {
    let (f, r) = setup(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER);",
        "CREATE INDEX idx_a ON t(a);",
    ]);
    let q = "SELECT id FROM t WHERE a < 5";
    cmp(&f, &r, q, "empty");
    insert_both(&f, &r, "INSERT INTO t VALUES (1, NULL), (2, NULL);");
    cmp(&f, &r, q, "all-null-excluded"); // NULL < 5 is not true → 0 rows
    insert_both(
        &f,
        &r,
        "INSERT INTO t VALUES (3, 1), (4, 4), (5, 5), (6, 9);",
    );
    cmp(&f, &r, q, "mixed");
    cmp(&f, &r, "SELECT id FROM t WHERE a >= 5", "ge-with-nulls");
    cmp(
        &f,
        &r,
        "SELECT id FROM t WHERE a BETWEEN 1 AND 4",
        "between",
    );
    assert!(
        opens(&f, q, "idx_a") && !opens(&f, q, "t"),
        "edge: covering seek gate"
    );
}
