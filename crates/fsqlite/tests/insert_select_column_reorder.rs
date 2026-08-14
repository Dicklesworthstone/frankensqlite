use fsqlite::Connection;

#[test]
fn insert_test() {
    let conn = Connection::open(":memory:").unwrap();
    conn.execute("CREATE TABLE src (a INTEGER, b TEXT);").unwrap();
    conn.execute("CREATE TABLE dst (x TEXT, y INTEGER);").unwrap();
    conn.execute("INSERT INTO src VALUES (10, 'ten');").unwrap();
    
    let rows = conn.query("SELECT a, b FROM src;").unwrap();
    println!("src row 0: {:?}", rows[0].values());

    conn.execute("INSERT INTO dst (y, x) SELECT a, b FROM src;").unwrap();
    
    let rows2 = conn.query("SELECT x, y FROM dst;").unwrap();
    println!("dst row 0: {:?}", rows2[0].values());
}
