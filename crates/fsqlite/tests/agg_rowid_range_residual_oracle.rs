//! bd-agg-rowid-range-residual: `SELECT COUNT(*)/SUM(...) FROM t WHERE <rowid> <range> AND <residual>`
//! seeks the rowid range on the table b-tree (a SUPERSET of the matching rows) and re-applies the whole
//! (placeholder-free) WHERE as a residual filter per row, instead of full-scanning. Works for SUM (via
//! codegen_select_aggregate) and COUNT(*) (which yields out of codegen_select_count_star when the range
//! carries a residual). HARD GATE: byte-identical to C SQLite across residual eq/range/IN/`<>`, one- and
//! two-sided rowid ranges, absent ranges (→ COUNT=0 / SUM=NULL), COALESCE, and both aggregates. Opcode
//! gate: the range seeks the table (Seek*); the residual is enforced by the filter, not dropped.

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

#[test]
fn agg_rowid_range_residual_matches_sqlite() {
    let f = Connection::open(":memory:").expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER);";
    f.execute(ddl).unwrap();
    r.execute_batch(ddl).unwrap();
    for i in 1..=1000_i64 {
        let stmt = format!("INSERT INTO t VALUES ({i}, {}, {});", i % 10, i % 3);
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
        // Rowid range + residual, both aggregates.
        "SELECT COUNT(*) FROM t WHERE id > 500 AND x = 5",
        "SELECT SUM(x) FROM t WHERE id > 500 AND x = 5",
        "SELECT COUNT(*) FROM t WHERE id BETWEEN 100 AND 400 AND x > 3",
        "SELECT SUM(x) FROM t WHERE id >= 100 AND id <= 300 AND x <> 5",
        "SELECT COUNT(*) FROM t WHERE id < 200 AND x IN (2, 5, 8)",
        "SELECT COUNT(*) FROM t WHERE id > 100 AND id < 900 AND y = 1",
        // Multiple residuals.
        "SELECT COUNT(*) FROM t WHERE id > 100 AND x = 5 AND y = 2",
        // Absent range -> COUNT=0 / SUM=NULL, and COALESCE.
        "SELECT COUNT(*) FROM t WHERE id > 100000 AND x = 5",
        "SELECT SUM(x) FROM t WHERE id < 0 AND x = 5",
        "SELECT COALESCE(SUM(x), -1) FROM t WHERE id < 0 AND x = 5",
        // MIN/MAX riding the same seek.
        "SELECT MIN(x), MAX(x) FROM t WHERE id BETWEEN 100 AND 400 AND x > 1",
        // Residual-free rowid range still works (regression).
        "SELECT COUNT(*) FROM t WHERE id > 500",
        "SELECT SUM(x) FROM t WHERE id BETWEEN 100 AND 400",
    ] {
        cmp(sql);
    }

    // Opcode gate: the rowid range seeks the table (SeekGT for `id > c`), residual enforced by filter.
    assert!(
        has_op(
            &f,
            "SELECT COUNT(*) FROM t WHERE id > 500 AND x = 5",
            "SeekGT"
        ),
        "COUNT(*) rowid range + residual must seek the range (SeekGT)"
    );
    assert!(
        has_op(
            &f,
            "SELECT SUM(x) FROM t WHERE id > 500 AND x = 5",
            "SeekGT"
        ),
        "SUM rowid range + residual must seek the range (SeekGT)"
    );
    // A bound parameter in the RESIDUAL is fine (literal rowid bound -> probe emits no placeholders, so
    // the residual filter numbers `?` exactly as the scan path). The seek fires.
    assert!(
        has_op(
            &f,
            "SELECT COUNT(*) FROM t WHERE id > 500 AND x = ?",
            "SeekGT"
        ),
        "rowid range (literal bound) + parameterized residual must still seek the range"
    );
    // A parameterized rowid BOUND declines (the probe must be a literal).
    assert!(
        !has_op(
            &f,
            "SELECT COUNT(*) FROM t WHERE id > ? AND x = 5",
            "SeekGT"
        ),
        "a parameterized rowid bound must decline (probe must be a literal)"
    );
}
