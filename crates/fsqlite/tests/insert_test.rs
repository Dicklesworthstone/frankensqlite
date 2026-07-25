use fsqlite::Connection;
use fsqlite_types::SqliteValue;

#[test]
fn test_insert_test() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE src (a INTEGER, b TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE dst (x TEXT, y INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO src VALUES (10, 'ten');")
            .await
            .unwrap();

        let rows = conn.query("SELECT a, b FROM src;").await.unwrap();
        println!("src row 0: {:?}", rows[0].values());

        conn.execute("INSERT INTO dst (y, x) SELECT a, b FROM src;")
            .await
            .unwrap();

        let rows2 = conn.query("SELECT x, y FROM dst;").await.unwrap();
        println!("dst row 0: {:?}", rows2[0].values());
        assert_eq!(
            rows2[0].values(),
            &[SqliteValue::Text("ten".into()), SqliteValue::Integer(10)]
        );
    });
}
