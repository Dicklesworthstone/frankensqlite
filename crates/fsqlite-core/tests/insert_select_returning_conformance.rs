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
fn insert_select_returning_matches_rusqlite() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
    let fconn = Connection::open(":memory:").await?;
    let rconn = rusqlite::Connection::open_in_memory()?;

    for sql in [
        "CREATE TABLE src(id INTEGER PRIMARY KEY, name TEXT, qty INTEGER);",
        "CREATE TABLE dst(id INTEGER PRIMARY KEY, label TEXT UNIQUE, qty INTEGER DEFAULT 5);",
        "INSERT INTO src VALUES (1, 'alpha', 3), (2, 'beta', NULL), (3, 'gamma', 7);",
        "INSERT INTO dst(id, label, qty) VALUES (100, 'alpha', 11);",
    ] {
        fconn.execute(sql).await?;
        rconn.execute_batch(sql)?;
    }

    for sql in [
        "INSERT INTO dst(id, label, qty)
         SELECT id + 10, upper(name), coalesce(qty, 0) * 2
         FROM src
         ORDER BY id
         RETURNING id, label, qty, typeof(qty)",
        "INSERT OR IGNORE INTO dst(label, qty)
         SELECT name, qty
         FROM src
         ORDER BY id
         RETURNING label, qty",
        "INSERT INTO dst(label, qty)
         SELECT label || '-copy', qty
         FROM dst
         WHERE id IN (11, 13)
         ORDER BY id
         RETURNING id, label, qty",
        "INSERT INTO dst(label, qty)
         SELECT name, qty
         FROM src
         WHERE 0
         RETURNING *",
    ] {
        assert_query_matches_rusqlite(&fconn, &rconn, sql).await?;
    }

    assert_query_matches_rusqlite(&fconn, &rconn, "SELECT id, label, qty FROM dst ORDER BY id")
        .await?;

    Ok(())
        }
        .await;
    });
    outcome
}
