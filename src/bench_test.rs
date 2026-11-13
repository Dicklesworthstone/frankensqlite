use fsqlite_core::Connection;

fn main() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conn.execute("EXPLAIN SELECT * FROM t WHERE id = 42").unwrap();
}
