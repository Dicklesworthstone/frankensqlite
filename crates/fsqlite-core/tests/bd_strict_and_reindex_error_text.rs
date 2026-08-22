// Keeper (bd-errmsg-parity-batch3): STRICT-table type-declaration errors and
// REINDEX-of-unknown-object report stock's verbatim SQLITE_ERROR text, not
// frank's old "internal error: ..." (SQLITE_INTERNAL). Oracle: sqlite3 3.46.1 +
// rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn err_of(sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute(sql)
        .await
        .expect_err("must be rejected")
        .to_string()
}

#[test]
fn strict_and_reindex_error_text_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(
            err_of("CREATE TABLE t(a) STRICT").await,
            "missing datatype for t.a",
        );
        assert_eq!(
            err_of("CREATE TABLE t(a FOO) STRICT").await,
            "unknown datatype for t.a: \"FOO\"",
        );
        // A recognised STRICT type is fine.
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("CREATE TABLE ok(a INTEGER, b TEXT, c ANY) STRICT")
            .await
            .expect("valid STRICT");

        assert_eq!(
            err_of("REINDEX no_such_object").await,
            "unable to identify the object to be reindexed",
        );
    });
}
