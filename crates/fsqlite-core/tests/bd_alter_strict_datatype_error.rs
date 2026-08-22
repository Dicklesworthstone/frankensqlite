// Keeper (bd-errmsg-parity-batch3): ALTER TABLE ADD COLUMN on a STRICT table
// with a missing/unknown datatype reports stock's "error in table <t> after add
// column: <inner>" wrapper (inner = the CREATE-time datatype message), not
// frank's old "internal error: STRICT table t column c ...". Oracle: sqlite3
// 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;

async fn alter_err(add_col: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute("CREATE TABLE t(a INTEGER) STRICT").await.unwrap();
    c.execute(&format!("ALTER TABLE t ADD COLUMN {add_col}"))
        .await
        .expect_err("STRICT ADD COLUMN datatype error must be rejected")
        .to_string()
}

#[test]
fn alter_strict_add_column_datatype_error() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(
            alter_err("c FOO").await,
            "error in table t after add column: unknown datatype for t.c: \"FOO\"",
        );
        assert_eq!(
            alter_err("c").await,
            "error in table t after add column: missing datatype for t.c",
        );
        // A valid STRICT type succeeds.
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("CREATE TABLE t(a INTEGER) STRICT").await.unwrap();
        c.execute("ALTER TABLE t ADD COLUMN c TEXT")
            .await
            .expect("valid STRICT ADD COLUMN");
    });
}
