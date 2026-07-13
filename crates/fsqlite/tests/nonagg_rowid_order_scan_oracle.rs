//! bd-nonagg-rowid-order-scan: `SELECT ... FROM t [WHERE <filter>] ORDER BY <rowid> ASC [LIMIT k]`
//! needs NO sorter — a table scan (Rewind+Next) is already in ascending rowid order (rowid is unique,
//! no ties). Previously `resolve_order_by_index_plan` returned None for the rowid, so all rows were
//! sorted. This routes to the plain scan (filter + LIMIT/OFFSET, stops early under LIMIT). Compared IN
//! OUTPUT ORDER against C SQLite; ASC cases assert NO sorter opcode; DESC / non-rowid ORDER BY still use
//! the sorter (correctness preserved).
use fsqlite::Connection;
use fsqlite_types::SqliteValue;
fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(), SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"), SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
// ORDERED: preserve row order (the lever's correctness is about output order).
fn frank_ord(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    c.query(sql).unwrap_or_else(|e| panic!("frank `{sql}`: {e}")).iter().map(|row| row.values().iter().map(render).collect()).collect()
}
fn sqlite_ord(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap(); let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n { out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
            rusqlite::types::Value::Null => "NULL".to_owned(), rusqlite::types::Value::Integer(x) => x.to_string(),
            rusqlite::types::Value::Real(f) => format!("{f:?}"), rusqlite::types::Value::Text(s) => format!("'{s}'"),
            rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
        }); } Ok(out)
    }).unwrap().map(Result::unwrap).collect()
}
fn has_sorter(c: &Connection, sql: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with("Sorter")))
}
fn setup(ddl: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap(); let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in ddl { f.execute(s).unwrap(); r.execute_batch(s).unwrap(); } (f, r)
}
fn ins(f: &Connection, r: &rusqlite::Connection, s: &str) { f.execute(s).unwrap(); r.execute_batch(s).unwrap(); }
fn cmp_ord(f: &Connection, r: &rusqlite::Connection, sql: &str, l: &str) { assert_eq!(frank_ord(f, sql), sqlite_ord(r, sql), "[{l}] order diverged: `{sql}`"); }
#[test]
fn nonagg_rowid_order_scan_matches_sqlite() {
    let (f, r) = setup(&["CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);", "CREATE INDEX idx_c ON t(c);"]);
    for i in 1..=600_i64 {
        let a = if i % 17 == 0 { "NULL".to_owned() } else { format!("{}", i % 20) };
        let c = if i % 19 == 0 { "NULL".to_owned() } else { format!("{}", i % 12) };
        ins(&f, &r, &format!("INSERT INTO t VALUES ({i}, {a}, {c}, 'v{}');", i % 7));
    }
    // WHERE-less `ORDER BY <rowid> ASC`: served by the plain scan, NO sorter, verified in output order.
    let scans = [
        "SELECT * FROM t ORDER BY id",
        "SELECT * FROM t ORDER BY id LIMIT 10",
        "SELECT id, x FROM t ORDER BY id ASC LIMIT 5 OFFSET 3",
        "SELECT rowid, x FROM t ORDER BY rowid LIMIT 3",             // explicit rowid keyword
        "SELECT * FROM t ORDER BY id ASC",
        "SELECT id FROM t ORDER BY id LIMIT 1",                      // top-1
    ];
    for sql in scans {
        cmp_ord(&f, &r, sql, "rowid-order-scan");
        assert!(!has_sorter(&f, sql), "WHERE-less ORDER BY rowid ASC must NOT open a sorter: `{sql}`");
    }
    // NOT bypassed by this cut (still correct via the sorter): any WHERE, DESC, or non-rowid ORDER BY.
    let sorted = [
        "SELECT * FROM t WHERE x = 'v3' ORDER BY id",     // has WHERE -> declines (follow-up)
        "SELECT c FROM t WHERE a > 10 ORDER BY id LIMIT 20",
        "SELECT * FROM t ORDER BY id DESC LIMIT 10",      // DESC (follow-up)
        "SELECT * FROM t ORDER BY x LIMIT 10",            // non-rowid ORDER BY
        "SELECT * FROM t WHERE a < 5 ORDER BY id DESC",
    ];
    for sql in sorted {
        cmp_ord(&f, &r, sql, "still-sorts");
        assert!(has_sorter(&f, sql), "WHERE / DESC / non-rowid ORDER BY should still use the sorter: `{sql}`");
    }
}
