// Keeper (bd-errmsg-parity-batch #8): a column cannot have both a generated
// (AS) expression and a DEFAULT. Stock SQLite's message is order-dependent:
// DEFAULT after the AS expr -> "cannot use DEFAULT on a generated column";
// DEFAULT before it -> "error in generated column \"<name>\"".
// Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn ddl_err(ddl: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute(ddl)
        .await
        .expect_err("DEFAULT on a generated column must be rejected")
        .to_string()
}

async fn ddl_ok(ddl: &str) {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute(ddl).await.expect("valid DDL must succeed");
}

#[test]
fn generated_column_with_default_is_rejected() {
    asupersync::test_utils::run_test(|| async {
        // DEFAULT after the generated expression (STORED / VIRTUAL / bare).
        assert_eq!(
            ddl_err("CREATE TABLE t(a, b AS (a+1) STORED DEFAULT 5)").await,
            "cannot use DEFAULT on a generated column",
        );
        assert_eq!(
            ddl_err("CREATE TABLE t(a, b AS (a+1) VIRTUAL DEFAULT 5)").await,
            "cannot use DEFAULT on a generated column",
        );
        assert_eq!(
            ddl_err("CREATE TABLE t(a, b AS (a+1) DEFAULT 5)").await,
            "cannot use DEFAULT on a generated column",
        );
        // DEFAULT before the generated expression -> the generic wrapper.
        assert_eq!(
            ddl_err("CREATE TABLE t(a, b DEFAULT 5 AS (a+1))").await,
            "error in generated column \"b\"",
        );
        // Valid: generated column alone, or DEFAULT alone.
        ddl_ok("CREATE TABLE t(a, b AS (a+1))").await;
        ddl_ok("CREATE TABLE t(a, b AS (a+1) STORED)").await;
        ddl_ok("CREATE TABLE t(a, b DEFAULT 5)").await;
    });
}
