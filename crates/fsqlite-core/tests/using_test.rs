use fsqlite_core::connection::Connection;

#[test]
fn test_using() {
    let conn = Connection::open(":memory:").unwrap();
    let setup = "
        CREATE TABLE t1(id INTEGER, name TEXT, val INTEGER);
        CREATE TABLE t2(id INTEGER, name TEXT, extra TEXT);
        INSERT INTO t1 VALUES(1,'Alice',10),(2,'Bob',20),(3,'Carol',30);
        INSERT INTO t2 VALUES(1,'Alice','x'),(2,'Bob','y'),(4,'Dave','z');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        conn.execute(s).unwrap();
    }
    match conn.query("SELECT id, name FROM t1 LEFT JOIN t2 USING(id) ORDER BY id") {
        Ok(rows) => println!("Success! {} rows", rows.len()),
        Err(e) => println!("Error: {:?}", e),
    }
}
