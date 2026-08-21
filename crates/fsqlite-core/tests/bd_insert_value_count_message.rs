// Keeper (bd-errmsg-parity-batch #1): an INSERT VALUES column/value count
// mismatch reports stock's verbatim SQLITE_ERROR text, NOT frank's old
// "not implemented: ..." (NotImplemented) wrapper. Two stock forms:
//   no target list   -> "table t has N columns but M values were supplied"
//   with target list -> "M values for N columns"
// Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn insert_err(setup: &str, insert: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute(setup).await.unwrap();
    c.execute(insert)
        .await
        .expect_err("column/value count mismatch must be rejected")
        .to_string()
}

#[test]
fn insert_value_count_mismatch_reports_stock_text() {
    asupersync::test_utils::run_test(|| async {
        // No target column list -> "table t has N columns but M values were supplied".
        assert_eq!(
            insert_err("CREATE TABLE t(a, b)", "INSERT INTO t VALUES(1)").await,
            "table t has 2 columns but 1 values were supplied",
        );
        assert_eq!(
            insert_err("CREATE TABLE t(a, b)", "INSERT INTO t VALUES(1, 2, 3)").await,
            "table t has 2 columns but 3 values were supplied",
        );
        // With an explicit target list -> "M values for N columns".
        assert_eq!(
            insert_err("CREATE TABLE t(a, b)", "INSERT INTO t(a) VALUES(1, 2)").await,
            "2 values for 1 columns",
        );
        assert_eq!(
            insert_err("CREATE TABLE t(a, b)", "INSERT INTO t(a, b) VALUES(1)").await,
            "1 values for 2 columns",
        );
    });
}
