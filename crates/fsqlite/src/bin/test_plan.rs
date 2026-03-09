use fsqlite::Connection;

fn main() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER);")
        .unwrap();
    let rows = conn
        .query("EXPLAIN SELECT * FROM bench WHERE id = 5;")
        .unwrap();
    for row in rows {
        let values: Vec<String> = row.values().iter().map(|v| format!("{:?}", v)).collect();
        println!("{}", values.join(" | "));
    }
}
