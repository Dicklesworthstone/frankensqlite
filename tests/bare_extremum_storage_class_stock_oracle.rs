//! Stock-parity ties and semantic unique extrema preserve storage classes and bare values.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

#[test]
fn bare_extremum_numeric_ties_preserve_stock_storage_classes() {
    asupersync::test_utils::run_test(|| check_extrema(false));
}

#[test]
fn bare_extremum_unique_rows_preserve_stock_storage_classes() {
    asupersync::test_utils::run_test(|| check_extrema(true));
}

async fn check_extrema(unique: bool) {
    let frank = Connection::open(":memory:").await.expect("open");
    let stock = rusqlite::Connection::open_in_memory().expect("stock");
    let setup = "CREATE TABLE t(v, b, seq); INSERT INTO t VALUES (1, x'01', 1), (1.0, x'02', 2)";
    frank.execute_batch(setup).await.expect("setup");
    stock.execute_batch(setup).expect("stock setup");
    if unique {
        let update = "UPDATE t SET v=2.0 WHERE seq=2";
        frank.execute(update).await.expect("unique extremum");
        stock.execute_batch(update).expect("stock unique extremum");
    }
    for sql in [
        "SELECT min(v), b FROM (SELECT v,b FROM t ORDER BY seq DESC)",
        "SELECT max(v), b FROM (SELECT v,b FROM t ORDER BY seq DESC)",
        "WITH s AS (SELECT v,b FROM t ORDER BY seq DESC LIMIT 2) SELECT min(v), b FROM s",
        "SELECT max(v), b FROM (SELECT v,b FROM t ORDER BY seq ASC) HAVING max(v)>0",
    ] {
        let actual: Vec<Vec<SqliteValue>> = frank
            .query(sql)
            .await
            .expect("query")
            .iter()
            .map(|row| row.values().to_vec())
            .collect();
        let mut stmt = stock.prepare(sql).expect("stock prepare");
        let expected: Vec<Vec<SqliteValue>> = stmt
            .query_map([], |row| {
                (0..2)
                    .map(|i| {
                        Ok(match row.get::<_, rusqlite::types::Value>(i)? {
                            rusqlite::types::Value::Null => SqliteValue::Null,
                            rusqlite::types::Value::Integer(v) => SqliteValue::Integer(v),
                            rusqlite::types::Value::Real(v) => SqliteValue::Float(v),
                            rusqlite::types::Value::Text(v) => SqliteValue::Text(v.into()),
                            rusqlite::types::Value::Blob(v) => SqliteValue::Blob(v.into()),
                        })
                    })
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .expect("stock query")
            .collect::<rusqlite::Result<_>>()
            .expect("stock rows");
        let classes = |rows: &[Vec<SqliteValue>]| {
            rows.iter()
                .map(|row| {
                    row.iter()
                        .map(SqliteValue::storage_class)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            classes(&actual),
            classes(&expected),
            "storage classes: {sql}"
        );
        assert_eq!(
            actual, expected,
            "values, including exact blob bytes: {sql}"
        );
    }
}
