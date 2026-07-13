//! bd-rowid-range-before-directive: `SELECT ... FROM t WHERE <rowid> <range>` seeks the table
//! b-tree's `[lower,upper]` rowid slice instead of honoring the planner's FullTableScan directive and
//! reading every row (`PlannerSelectAccessKind` has no rowid-range variant, so the directive would
//! otherwise short-circuit the rowid_range heuristic). The scan walks in rowid order — the SAME order
//! the full scan emits — so it is byte-identical (order included), needs no covering gate, and reads a
//! subset. Compared byte-exact to rusqlite; a lower-bound query is opcode-gated to a `Seek*`.

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

/// True if the plan emits any `Seek*` opcode (proves the rowid range seek positioned on a bound).
fn has_seek(conn: &Connection, sql: &str) -> bool {
    conn.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(|row| {
        matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string().starts_with("Seek"))
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

#[test]
fn rowid_range_before_directive_matches_sqlite() {
    let (f, r) = setup(&["CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, x TEXT);"]);
    for i in 1..=400_i64 {
        let a = if i % 11 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 50)
        };
        insert_both(
            &f,
            &r,
            &format!("INSERT INTO t VALUES ({i}, {a}, 'v{}');", i % 30),
        );
    }
    // Rowid ranges read the table slice, so non-covering columns (a, x, *) are fine.
    let cases = [
        "SELECT id FROM t WHERE id < 100",
        "SELECT id FROM t WHERE id <= 100",
        "SELECT id FROM t WHERE id > 300",
        "SELECT id FROM t WHERE id >= 300",
        "SELECT id, a, x FROM t WHERE id BETWEEN 120 AND 180",
        "SELECT * FROM t WHERE id <= 40",
        "SELECT a FROM t WHERE id > 380",
        "SELECT id FROM t WHERE id < 999999", // non-selective: still correct
        "SELECT id FROM t WHERE id > 100 AND id < 110",
    ];
    for sql in cases {
        cmp(&f, &r, sql, "rowid-range");
    }
    // Lower-bound query must position with a Seek (proves the seek fired, not a full scan).
    assert!(
        has_seek(&f, "SELECT id FROM t WHERE id > 300"),
        "lower-bound rowid range must Seek"
    );
    assert!(
        has_seek(&f, "SELECT id, a FROM t WHERE id BETWEEN 120 AND 180"),
        "between must Seek"
    );
}

#[test]
fn rowid_range_edge_cases() {
    let (f, r) = setup(&["CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);"]);
    let q = "SELECT id FROM t WHERE id > 5";
    cmp(&f, &r, q, "empty");
    for i in 1..=10_i64 {
        insert_both(&f, &r, &format!("INSERT INTO t VALUES ({i}, {});", i * 2));
    }
    cmp(&f, &r, q, "small");
    cmp(&f, &r, "SELECT id FROM t WHERE id >= 5", "ge-boundary");
    cmp(&f, &r, "SELECT id FROM t WHERE id <= 1", "single-low");
    cmp(&f, &r, "SELECT id FROM t WHERE id > 10", "empty-high");
    cmp(
        &f,
        &r,
        "SELECT id FROM t WHERE id BETWEEN 3 AND 7",
        "between",
    );
    assert!(
        has_seek(&f, "SELECT id FROM t WHERE id >= 5"),
        "edge: lower-bound must Seek"
    );
}
