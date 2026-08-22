// Keeper (bd-errmsg-parity-batch3): ATTACH with an already-in-use schema name
// (main, temp, or a duplicate attached name) reports stock's "database <name>
// is already in use" (name as-written), not frank's old "internal error: cannot
// attach with reserved schema name: <name>". Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn attach_err(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql)
        .await
        .expect_err("ATTACH of an in-use name must be rejected")
        .to_string()
}

#[test]
fn attach_in_use_schema_reports_already_in_use() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(
            attach_err(&[], "ATTACH ':memory:' AS main").await,
            "database main is already in use",
        );
        assert_eq!(
            attach_err(&[], "ATTACH ':memory:' AS temp").await,
            "database temp is already in use",
        );
        // Case is preserved in the message.
        assert_eq!(
            attach_err(&[], "ATTACH ':memory:' AS MAIN").await,
            "database MAIN is already in use",
        );
        // A duplicate attached name (already-correct path) reports the same.
        assert_eq!(
            attach_err(&["ATTACH ':memory:' AS aux"], "ATTACH ':memory:' AS aux").await,
            "database aux is already in use",
        );
        // A fresh attached name succeeds.
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("ATTACH ':memory:' AS aux2")
            .await
            .expect("fresh ATTACH must succeed");
    });
}
