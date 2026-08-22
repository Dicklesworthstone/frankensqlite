// Keeper (bd-errmsg-parity-batch #7): an INSERT ... SELECT column/value count
// mismatch reports stock's verbatim SQLITE_ERROR text (same two forms as the
// VALUES path), NOT frank's old Internal "INSERT ... SELECT column count
// mismatch: source row N has X values, SELECT produced Y".
//   no target list   -> "table t has N columns but M values were supplied"
//   with target list -> "M values for N columns"
// Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn insert_err(setup: &[&str], insert: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(insert)
        .await
        .expect_err("INSERT..SELECT count mismatch must be rejected")
        .to_string()
}

#[test]
fn insert_select_value_count_mismatch_reports_stock_text() {
    asupersync::test_utils::run_test(|| async {
        // FROM-less SELECT, no target list.
        assert_eq!(
            insert_err(&["CREATE TABLE t(a, b)"], "INSERT INTO t SELECT 1").await,
            "table t has 2 columns but 1 values were supplied",
        );
        assert_eq!(
            insert_err(&["CREATE TABLE t(a, b)"], "INSERT INTO t SELECT 1, 2, 3").await,
            "table t has 2 columns but 3 values were supplied",
        );
        // Explicit target list.
        assert_eq!(
            insert_err(&["CREATE TABLE t(a, b)"], "INSERT INTO t(a) SELECT 1, 2").await,
            "2 values for 1 columns",
        );
        assert_eq!(
            insert_err(&["CREATE TABLE t(a, b)"], "INSERT INTO t(a, b) SELECT 1").await,
            "1 values for 2 columns",
        );
        // SELECT ... FROM a source table (rows actually flow through the emitter).
        assert_eq!(
            insert_err(
                &[
                    "CREATE TABLE t(a, b)",
                    "CREATE TABLE s(x)",
                    "INSERT INTO s VALUES(1)"
                ],
                "INSERT INTO t SELECT x FROM s",
            )
            .await,
            "table t has 2 columns but 1 values were supplied",
        );
    });
}
