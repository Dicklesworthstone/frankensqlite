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

async fn fsqlite_prepared_rows(conn: &Connection, sql: &str) -> TestResult<Vec<Vec<String>>> {
    let stmt = conn.prepare(sql).await?;
    Ok(format_fsqlite_rows(stmt.query().await?))
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

#[test]
fn prepared_pragma_getters_and_setters_match_rusqlite() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let fconn = Connection::open(":memory:").await?;
            let rconn = rusqlite::Connection::open_in_memory()?;

            for sql in ["PRAGMA user_version = 37", "PRAGMA cache_size = -4000"] {
                fconn.prepare(sql).await?.execute().await?;
                rconn.execute_batch(sql)?;
            }

            for sql in ["PRAGMA user_version", "PRAGMA cache_size"] {
                assert_eq!(
                    fsqlite_prepared_rows(&fconn, sql).await?,
                    rusqlite_rows(&rconn, sql)?
                );
            }

            let user_version_row = fconn
                .prepare("PRAGMA user_version")
                .await?
                .query_row()
                .await?;
            assert_eq!(user_version_row.values(), &[SqliteValue::Integer(37)]);
            assert_eq!(
                fconn
                    .prepare("PRAGMA user_version")
                    .await?
                    .execute()
                    .await?,
                1
            );

            Ok(())
        }
        .await;
    });
    outcome
}
