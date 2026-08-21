// Keeper (bd-errmsg-parity-batch2): CREATE INDEX / CREATE TRIGGER on a missing
// target table schema-qualifies the "no such table" error (default "main."),
// unlike SELECT/DROP/ALTER which stay unqualified. Oracle: sqlite3 3.46.1 +
// rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn err_of(sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute(sql).await.expect_err("must be rejected").to_string()
}

#[test]
fn create_ddl_no_such_table_is_schema_qualified() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(
            err_of("CREATE INDEX i ON nope(a)").await,
            "no such table: main.nope",
        );
        assert_eq!(
            err_of("CREATE TRIGGER tr AFTER INSERT ON nope BEGIN SELECT 1; END").await,
            "no such table: main.nope",
        );
        assert_eq!(
            err_of("CREATE TRIGGER tr INSTEAD OF INSERT ON nope BEGIN SELECT 1; END").await,
            "no such table: main.nope",
        );
        // Control: SELECT / DROP / ALTER stay UNqualified.
        assert_eq!(err_of("SELECT * FROM nope").await, "no such table: nope");
        assert_eq!(err_of("DROP TABLE nope").await, "no such table: nope");
        assert_eq!(err_of("ALTER TABLE nope RENAME TO x").await, "no such table: nope");
    });
}
