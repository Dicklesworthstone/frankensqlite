//! CREATE TABLE AS SELECT must preserve quoted table names and existing rows.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

async fn compare_integer_rows(frank: &Connection, stock: &rusqlite::Connection, sql: &str) {
    let actual = frank
        .query(sql)
        .await
        .unwrap_or_else(|error| panic!("FrankenSQLite query `{sql}`: {error:?}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    let mut statement = stock.prepare(sql).unwrap();
    let expected = statement
        .query_map([], |row| Ok(vec![SqliteValue::Integer(row.get::<_, i64>(0)?)]))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(actual, expected, "rows differ from stock SQLite for `{sql}`");
}

#[test]
fn ctas_quoted_names_preserve_existing_table_and_match_stock() {
    asupersync::test_utils::run_test(|| async {
        for explicit_transaction in [false, true] {
            let frank = Connection::open(":memory:").await.unwrap();
            let stock = rusqlite::Connection::open_in_memory().unwrap();
            for sql in ["CREATE TABLE victim(x)", "INSERT INTO victim VALUES (1)"] {
                frank.execute(sql).await.unwrap();
                stock.execute_batch(sql).unwrap();
            }
            if explicit_transaction {
                frank.execute("BEGIN").await.unwrap();
                stock.execute_batch("BEGIN").unwrap();
            }
            for (create, read_copy) in [
                (
                    r#"CREATE TABLE "copy""name" AS SELECT 42 AS x"#,
                    r#"SELECT x FROM "copy""name" ORDER BY x"#,
                ),
                (
                    r#"CREATE TABLE "victim"" VALUES (?1) --" AS SELECT 99 AS x"#,
                    r#"SELECT x FROM "victim"" VALUES (?1) --" ORDER BY x"#,
                ),
            ] {
                frank.execute(create).await.unwrap();
                stock.execute_batch(create).unwrap();
                compare_integer_rows(&frank, &stock, read_copy).await;
                compare_integer_rows(&frank, &stock, "SELECT x FROM victim ORDER BY x").await;
            }
            if explicit_transaction {
                frank.execute("ROLLBACK").await.unwrap();
                stock.execute_batch("ROLLBACK").unwrap();
                compare_integer_rows(&frank, &stock, "SELECT x FROM victim ORDER BY x").await;
                compare_integer_rows(
                    &frank,
                    &stock,
                    "SELECT count(*) FROM sqlite_master WHERE name != 'victim'",
                )
                .await;
            }
        }
    });
}
