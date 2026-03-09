use fsqlite::Connection;

fn main() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("PRAGMA journal_mode = WAL;").unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);")
        .unwrap();
    conn.execute("BEGIN;").unwrap();
    for i in 0..10000 {
        conn.execute(&format!(
            "INSERT INTO bench (id, name, score) VALUES ({}, 'name_{}', {})",
            i,
            i,
            i * 10
        ))
        .unwrap();
    }
    conn.execute("COMMIT;").unwrap();
}
