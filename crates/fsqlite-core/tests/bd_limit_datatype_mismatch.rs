// Keeper (bd-errmsg-parity-batch3): a non-integer LIMIT on a table scan (VDBE
// MustBeInt path) reports stock's bare "datatype mismatch", not the verbose
// "type mismatch: expected integer, got text". Oracle: sqlite3 3.46.1 +
// rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn err_of(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql).await.expect_err("non-integer LIMIT must be rejected").to_string()
}

#[test]
fn non_integer_limit_reports_datatype_mismatch() {
    asupersync::test_utils::run_test(|| async {
        let setup: &[&str] = &["CREATE TABLE t(a)", "INSERT INTO t VALUES(1),(2),(3)"];
        assert_eq!(err_of(setup, "SELECT * FROM t LIMIT 'x'").await, "datatype mismatch");
        assert_eq!(err_of(setup, "SELECT * FROM t LIMIT x'00'").await, "datatype mismatch");
        assert_eq!(err_of(setup, "SELECT * FROM t LIMIT 1.5").await, "datatype mismatch");

        // Coercion parity: an integer-valued text LIMIT still works.
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("CREATE TABLE t(a)").await.unwrap();
        c.execute("INSERT INTO t VALUES(1),(2),(3)").await.unwrap();
        let rows = c.query_with_params("SELECT * FROM t LIMIT '2'", &[]).await.unwrap();
        assert_eq!(rows.len(), 2, "LIMIT '2' should coerce to 2 rows");
    });
}
