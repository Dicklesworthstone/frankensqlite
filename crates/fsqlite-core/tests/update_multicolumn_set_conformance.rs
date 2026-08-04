use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn format_fsqlite_rows(rows: Vec<Row>) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.values().iter().map(format_fsqlite_value).collect())
        .collect()
}

fn format_fsqlite_value(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(number) => number.to_string(),
        SqliteValue::Float(number) => format!("{number}"),
        SqliteValue::Text(text) => format!("'{text}'"),
        SqliteValue::Blob(bytes) => format_blob(bytes),
    }
}

fn format_rusqlite_value(value: rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(number) => number.to_string(),
        rusqlite::types::Value::Real(number) => format!("{number}"),
        rusqlite::types::Value::Text(text) => format!("'{text}'"),
        rusqlite::types::Value::Blob(bytes) => format_blob(&bytes),
    }
}

fn format_blob(bytes: &[u8]) -> String {
    format!(
        "X'{}'",
        bytes
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>()
    )
}

fn rusqlite_rows(conn: &rusqlite::Connection, sql: &str) -> TestResult<Vec<Vec<String>>> {
    let mut stmt = conn.prepare(sql)?;
    let column_count = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut values = Vec::with_capacity(column_count);
            for index in 0..column_count {
                let value = row.get::<_, rusqlite::types::Value>(index)?;
                values.push(format_rusqlite_value(value));
            }
            Ok(values)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

async fn assert_query_matches_rusqlite(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    sql: &str,
) -> TestResult {
    assert_eq!(
        format_fsqlite_rows(fconn.query(sql).await?),
        rusqlite_rows(rconn, sql)?,
        "{sql}"
    );
    Ok(())
}

#[test]
fn update_multicolumn_set_matches_rusqlite() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let fconn = Connection::open(":memory:").await?;
            let rconn = rusqlite::Connection::open_in_memory()?;

            for sql in [
                "CREATE TABLE items(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, label TEXT);",
                "CREATE INDEX idx_items_ab ON items(a, b);",
                "INSERT INTO items VALUES
            (1, 10, 100, 'alpha'),
            (2, 20, 200, 'beta'),
            (3, NULL, 300, 'gamma');",
            ] {
                fconn.execute(sql).await?;
                rconn.execute_batch(sql)?;
            }

            for sql in [
                "UPDATE items
         SET (a, b) = (b, a)
         WHERE id = 1
         RETURNING id, a, b, label",
                "UPDATE items
         SET (a, b, label) = (a + 1, b + 2, label || '-x')
         WHERE id IN (2, 3)
         RETURNING id, typeof(a), a, b, label",
                "UPDATE items
         SET (a, b) = (NULL, 5)
         WHERE id = 3
         RETURNING id, typeof(a), a, b, label",
            ] {
                assert_query_matches_rusqlite(&fconn, &rconn, sql).await?;
            }

            assert_query_matches_rusqlite(
                &fconn,
                &rconn,
                "SELECT id, typeof(a), a, b, label FROM items ORDER BY id",
            )
            .await?;

            Ok(())
        }
        .await;
    });
    outcome
}
