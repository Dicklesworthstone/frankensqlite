// Keeper (bd-errmsg-parity-batch4): CREATE TABLE with an unknown COLLATE name is
// rejected with stock's "no such collation sequence: <name>"; builtin collations
// (BINARY/NOCASE/RTRIM, case-insensitive) are accepted. Oracle: sqlite3 3.46.1 +
// rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn err_of(ddl: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute(ddl)
        .await
        .expect_err("unknown collation must be rejected")
        .to_string()
}

async fn ok(ddl: &str) {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute(ddl)
        .await
        .expect("valid collation DDL must succeed");
}

#[test]
fn create_table_unknown_collation_rejected() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(
            err_of("CREATE TABLE t(a TEXT COLLATE nope)").await,
            "no such collation sequence: nope",
        );
        // Builtin collations (any case) and no-collation are accepted.
        ok("CREATE TABLE t(a TEXT COLLATE NOCASE)").await;
        ok("CREATE TABLE t(a TEXT COLLATE binary)").await;
        ok("CREATE TABLE t(a TEXT COLLATE RTRIM)").await;
        ok("CREATE TABLE t(a TEXT)").await;
    });
}
