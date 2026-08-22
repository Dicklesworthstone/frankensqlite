// Keeper: inserting a non-integral value into an INTEGER PRIMARY KEY (rowid)
// reports stock SQLite's bare "datatype mismatch" (SQLITE_MISMATCH), not a
// verbose "type mismatch: expected INTEGER PRIMARY KEY rowid, got text".
// Coercion itself is unchanged and matches stock: text-digit '5' and integral
// real 2.0 are accepted; 'x' and fractional 1.5 are rejected.
// Oracle: sqlite3 3.46.1 + rusqlite 3.53.
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn insert_err(value_sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b)")
        .await
        .unwrap();
    c.execute(&format!("INSERT INTO t VALUES({value_sql}, 'y')"))
        .await
        .expect_err("non-integral rowid must be rejected")
        .to_string()
}

async fn insert_rowid(value_sql: &str) -> i64 {
    let c = Connection::open(":memory:").await.unwrap();
    c.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b)")
        .await
        .unwrap();
    c.execute(&format!("INSERT INTO t VALUES({value_sql}, 'y')"))
        .await
        .unwrap();
    let rows = c.query_with_params("SELECT a FROM t", &[]).await.unwrap();
    match rows.first().map(|r| r.values()[0].clone()) {
        Some(SqliteValue::Integer(n)) => n,
        other => panic!("expected integer rowid, got {other:?}"),
    }
}

#[test]
fn rowid_non_integral_reports_datatype_mismatch() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(insert_err("'x'").await, "datatype mismatch");
        assert_eq!(insert_err("1.5").await, "datatype mismatch");
        // Coercion parity: these are accepted just like stock.
        assert_eq!(insert_rowid("'5'").await, 5);
        assert_eq!(insert_rowid("2.0").await, 2);
        assert_eq!(insert_rowid("5").await, 5);
    });
}
