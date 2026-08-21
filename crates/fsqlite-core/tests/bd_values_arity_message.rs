// Keeper (bd-errmsg-parity-batch2): ragged VALUES rows report stock's verbatim
// "all VALUES must have the same number of terms" under SQLITE_ERROR, not
// frank's old "SQL error at offset N: ..." (mis-classified ParseError) prefix.
// Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn err_of(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql).await.expect_err("ragged VALUES must be rejected").to_string()
}

#[test]
fn ragged_values_reports_verbatim_terms_message() {
    asupersync::test_utils::run_test(|| async {
        // VALUES as a table source in a subquery.
        assert_eq!(
            err_of(&[], "SELECT 1 FROM (VALUES (1), (1, 2))").await,
            "all VALUES must have the same number of terms",
        );
        // Top-level VALUES.
        assert_eq!(
            err_of(&[], "VALUES (1), (1, 2)").await,
            "all VALUES must have the same number of terms",
        );
        // Multi-row INSERT with ragged VALUES.
        assert_eq!(
            err_of(&["CREATE TABLE t(a, b)"], "INSERT INTO t VALUES (1, 2), (3)").await,
            "all VALUES must have the same number of terms",
        );
    });
}
