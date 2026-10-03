//! Flattening must retain SQLite's aggregate tie and duplicate-name semantics.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

async fn compare(setup: &[&str], query: &str) {
    let frank = Connection::open(":memory:").await.unwrap();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        frank.execute(sql).await.unwrap();
        stock.execute_batch(sql).unwrap();
    }
    let actual = frank
        .query(query)
        .await
        .unwrap()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    let mut statement = stock.prepare(query).unwrap();
    let expected = statement
        .query_map([], |row| {
            Ok(vec![match row.get::<_, rusqlite::types::Value>(0)? {
                rusqlite::types::Value::Null => SqliteValue::Null,
                rusqlite::types::Value::Integer(value) => SqliteValue::Integer(value),
                rusqlite::types::Value::Real(value) => SqliteValue::Float(value),
                rusqlite::types::Value::Text(value) => SqliteValue::Text(value.into()),
                rusqlite::types::Value::Blob(value) => SqliteValue::Blob(value.into()),
            }])
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let classes = |rows: &[Vec<SqliteValue>]| {
        rows.iter()
            .map(|row| row.iter().map(SqliteValue::storage_class).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        classes(&actual),
        classes(&expected),
        "storage classes differ: {query}"
    );
    assert_eq!(actual, expected, "query differs from stock SQLite: {query}");
}

#[test]
fn aggregate_ties_preserve_ordered_value_type() {
    asupersync::test_utils::run_test(|| async {
        compare(
            &[
                "CREATE TABLE t(x, seq INTEGER)",
                "INSERT INTO t VALUES (1, 1), (1.0, 2)",
            ],
            "SELECT min(x) FROM (SELECT x FROM t ORDER BY seq DESC)",
        )
        .await;
    });
}

#[test]
fn aggregate_ties_preserve_ordered_collation_value() {
    asupersync::test_utils::run_test(|| async {
        compare(
            &[
                "CREATE TABLE t(x TEXT COLLATE NOCASE, seq INTEGER)",
                "INSERT INTO t VALUES ('Abc', 1), ('abc', 2)",
            ],
            "SELECT min(x) FROM (SELECT x FROM t ORDER BY seq DESC)",
        )
        .await;
    });
}

#[test]
fn star_column_precedes_duplicate_expression_alias() {
    asupersync::test_utils::run_test(|| async {
        compare(
            &[
                "CREATE TABLE t(a INTEGER, b INTEGER)",
                "INSERT INTO t VALUES (1, 2)",
            ],
            "SELECT s.b FROM (SELECT *, a + 10 AS b FROM t) AS s",
        )
        .await;
    });
}

#[test]
fn count_still_rejects_unknown_inner_order_column() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        for sql in ["CREATE TABLE t(a INTEGER)", "INSERT INTO t VALUES (1)"] {
            frank.execute(sql).await.unwrap();
            stock.execute_batch(sql).unwrap();
        }
        let query = "SELECT count(*) FROM (SELECT a FROM t ORDER BY missing)";
        assert!(stock.prepare(query).is_err());
        assert!(frank.query(query).await.is_err());
    });
}
