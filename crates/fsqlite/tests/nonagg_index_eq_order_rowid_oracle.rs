//! bd-nonagg-index-eq-order-rowid: `SELECT ... WHERE <single-col-indexed> = <val> ORDER BY <rowid> ASC
//! [LIMIT k]` is served by the index-EQUALITY seek (which returns the eq value's rows in rowid-ascending
//! order — it positions at `(val, i64::MIN)` and walks forward) WITHOUT a sorter and WITHOUT full-scanning
//! every row. A composite index would order by its trailing column, and a DESC index reverses the walk,
//! so both decline. Compared IN OUTPUT ORDER against C SQLite.
use fsqlite::Connection;
use fsqlite_types::SqliteValue;
fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(), SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"), SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
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
fn opcode_present(c: &Connection, sql: &str, prefix: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with(prefix)))
}
fn has_seek(c: &Connection, sql: &str) -> bool { opcode_present(c, sql, "Seek") }
fn has_sorter(c: &Connection, sql: &str) -> bool { opcode_present(c, sql, "Sorter") }
fn setup(ddl: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").unwrap(); let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in ddl { f.execute(s).unwrap(); r.execute_batch(s).unwrap(); } (f, r)
}
fn ins(f: &Connection, r: &rusqlite::Connection, s: &str) { f.execute(s).unwrap(); r.execute_batch(s).unwrap(); }
fn cmp_ord(f: &Connection, r: &rusqlite::Connection, sql: &str, l: &str) { assert_eq!(frank_ord(f, sql), sqlite_ord(r, sql), "[{l}] order diverged: `{sql}`"); }
#[test]
fn nonagg_index_eq_order_rowid_matches_sqlite() {
    let (f, r) = setup(&["CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);", "CREATE INDEX idx_c ON t(c);"]);
    for i in 1..=600_i64 {
        let a = if i % 17 == 0 { "NULL".to_owned() } else { format!("{}", i % 20) };
        let c = if i % 19 == 0 { "NULL".to_owned() } else { format!("{}", i % 12) };
        ins(&f, &r, &format!("INSERT INTO t VALUES ({i}, {a}, {c}, 'v{}');", i % 7));
    }
    // Single-col-indexed eq + ORDER BY rowid ASC: index seek, NO sorter, verified in output order.
    let seeks = [
        "SELECT * FROM t WHERE c = 5 ORDER BY id",
        "SELECT * FROM t WHERE c = 3 ORDER BY id LIMIT 3",
        "SELECT id, c FROM t WHERE c = 7 ORDER BY id",
        "SELECT c FROM t WHERE c = 2 ORDER BY id LIMIT 5",       // covering output
        "SELECT * FROM t WHERE c = 0 ORDER BY id ASC",
        "SELECT id FROM t WHERE c = 4 ORDER BY rowid",           // explicit rowid keyword
        "SELECT * FROM t WHERE c = 11 ORDER BY id LIMIT 100",    // limit > match count
    ];
    for sql in seeks {
        cmp_ord(&f, &r, sql, "idx-eq-order");
        assert!(has_seek(&f, sql), "index-eq + ORDER BY rowid ASC must seek: `{sql}`");
        assert!(!has_sorter(&f, sql), "index-eq + ORDER BY rowid ASC must NOT sort: `{sql}`");
    }
    // Declines (still correct): DESC (walk is ascending-only), ORDER BY the eq column, ORDER BY non-rowid.
    for sql in [
        "SELECT * FROM t WHERE c = 5 ORDER BY id DESC",
        "SELECT * FROM t WHERE c = 5 ORDER BY c",
        "SELECT * FROM t WHERE c = 5 ORDER BY x",
    ] {
        cmp_ord(&f, &r, sql, "idx-eq-declines");
    }
    // Bare eq (no ORDER BY) still seeks (unchanged).
    assert!(has_seek(&f, "SELECT * FROM t WHERE c = 5"), "bare index eq must still seek");

    // Composite index: `WHERE cc = <v> ORDER BY id` must NOT use the eq seek (its trailing key column
    // `dd` would order the rows, not rowid) — it declines and stays correct.
    let (f2, r2) = setup(&["CREATE TABLE u (id INTEGER PRIMARY KEY, cc INTEGER, dd INTEGER, x TEXT);", "CREATE INDEX idx_ccdd ON u(cc, dd);"]);
    for i in 1..=400_i64 {
        // dd deliberately anti-correlated with id so a (cc,dd)-ordered walk would diverge from id order.
        ins(&f2, &r2, &format!("INSERT INTO u VALUES ({i}, {}, {}, 'w{}');", i % 8, (400 - i) % 50, i % 3));
    }
    for sql in ["SELECT * FROM u WHERE cc = 3 ORDER BY id", "SELECT id, dd FROM u WHERE cc = 1 ORDER BY id LIMIT 7"] {
        cmp_ord(&f2, &r2, sql, "composite-declines");
    }
}
