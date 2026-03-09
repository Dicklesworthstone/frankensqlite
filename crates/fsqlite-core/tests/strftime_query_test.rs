use fsqlite_core::connection::Connection;

#[test]
fn test_strftime_query() {
    let conn = Connection::open(":memory:").unwrap();
    let setup = "
        CREATE TABLE sales(id INTEGER PRIMARY KEY, amount REAL, sale_date TEXT);
        INSERT INTO sales VALUES(1,100.0,'2024-01-15'),(2,200.0,'2024-01-20');
    ";
    for s in setup.split(';').filter(|s| !s.trim().is_empty()) {
        conn.execute(s).unwrap();
    }

    println!("--- GROUP BY ALIAS ---");
    let rows = conn
        .query(
            "SELECT strftime('%Y-%m', sale_date) AS month, SUM(amount) FROM sales GROUP BY month",
        )
        .unwrap();
    for row in rows {
        println!("{:?}", row.values());
    }
}
