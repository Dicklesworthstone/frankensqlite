//! SQL-level JSON1 path and mutator conformance against C SQLite.
//!
//! This complements the JSON extension unit tests by executing the registered
//! SQL functions and table-valued functions through `fsqlite-core`.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn format_franken_value(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(number) => number.to_string(),
        SqliteValue::Float(number) => format!("{number}"),
        SqliteValue::Text(text) => format!("'{text}'"),
        SqliteValue::Blob(bytes) => format!(
            "X'{}'",
            bytes
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        ),
    }
}

fn format_rusqlite_value(value: rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(number) => number.to_string(),
        rusqlite::types::Value::Real(number) => format!("{number}"),
        rusqlite::types::Value::Text(text) => format!("'{text}'"),
        rusqlite::types::Value::Blob(bytes) => format!(
            "X'{}'",
            bytes
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        ),
    }
}

async fn franken_rows(
    conn: &Connection,
    sql: &str,
) -> std::result::Result<Vec<Vec<String>>, String> {
    conn.query(sql)
        .await
        .map_err(|error| format!("{error}"))
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(format_franken_value).collect())
                .collect()
        })
}

fn rusqlite_rows(
    conn: &rusqlite::Connection,
    sql: &str,
) -> std::result::Result<Vec<Vec<String>>, String> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|error| format!("prepare: {error}"))?;
    let column_count = stmt.column_count();
    stmt.query_map([], |row| {
        let mut values = Vec::with_capacity(column_count);
        for index in 0..column_count {
            let value = row.get::<_, rusqlite::types::Value>(index)?;
            values.push(format_rusqlite_value(value));
        }
        Ok(values)
    })
    .map_err(|error| format!("query: {error}"))?
    .collect::<std::result::Result<Vec<_>, _>>()
    .map_err(|error| format!("row: {error}"))
}

fn assert_query_matches(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let franken = franken_rows(fconn, sql);
    let csqlite = rusqlite_rows(rconn, sql);
    assert_eq!(
        franken, csqlite,
        "JSON1 conformance mismatch for query:\n{sql}\nfranken: {franken:?}\ncsqlite: {csqlite:?}"
    );
}

fn setup_pair() -> (Connection, rusqlite::Connection) {
    (
        Connection::open(":memory:").expect("open FrankenSQLite"),
        rusqlite::Connection::open_in_memory().expect("open rusqlite"),
    )
}

#[test]
fn json_extract_path_forms_match_rusqlite() {
    let (fconn, rconn) = setup_pair();
    let setup = r#"CREATE TABLE docs(id INTEGER PRIMARY KEY, doc TEXT);
INSERT INTO docs VALUES (1, '{"name":"Ada","age":37}');"#;
    fconn
        .execute_batch(setup)
        .expect("setup FrankenSQLite docs");
    rconn.execute_batch(setup).expect("setup rusqlite docs");

    let cases = [
        r#"SELECT json_extract('{"a.b":{"c":[10,20,30]},"plain":4}', '$."a.b".c[#-1]')"#,
        r#"SELECT json_extract('{"a":[1,2],"b":{"c":3}}', '$.a[0]', '$.b.c', '$.missing')"#,
        r#"SELECT json_type('{"a":[1,null,"x"]}', '$.a[1]'), json_type('{"a":[1,null,"x"]}', '$.a[2]')"#,
        r#"SELECT json_array_length('{"a":[1,2,3],"b":4}', '$.a'), json_array_length('{"a":[1,2,3],"b":4}', '$.b')"#,
        r"SELECT doc -> '$.name', doc ->> '$.age' FROM docs WHERE id = 1",
    ];
    for sql in cases {
        assert_query_matches(&fconn, &rconn, sql);
    }
}

#[test]
fn json_mutator_path_semantics_match_rusqlite() {
    let (fconn, rconn) = setup_pair();
    let cases = [
        r#"SELECT json_set('{"a":[1,2]}', '$.a[#]', 3)"#,
        r#"SELECT json_set('{"a":[{"b":1},{"b":2}]}', '$.a[#-1].b', 9)"#,
        r#"SELECT json_insert('{"a":1}', '$.a', 99, '$.b', 2)"#,
        r#"SELECT json_replace('{"a":1,"b":2}', '$.a', 7, '$.missing', 8)"#,
        r#"SELECT json_remove('{"a":[1,2,3],"b":4}', '$.a[#-2]', '$.b')"#,
        r#"SELECT json_remove('{"a":1}', '$')"#,
        r#"SELECT json_patch('{"a":{"b":1,"c":2},"d":3}', '{"a":{"b":9,"c":null},"e":4}')"#,
    ];
    for sql in cases {
        assert_query_matches(&fconn, &rconn, sql);
    }
}

#[test]
fn json_table_valued_path_filters_match_rusqlite() {
    let (fconn, rconn) = setup_pair();
    let cases = [
        r#"SELECT key, value, type, atom, fullkey, path FROM json_each('{"items":[{"sku":"a","qty":2},{"sku":"b","qty":5}]}', '$.items') ORDER BY key"#,
        r#"SELECT key, value, type, atom, fullkey, path FROM json_each('{"items":[{"sku":"a","qty":2},{"sku":"b","qty":5}]}', '$.items[1]') ORDER BY key"#,
        r#"SELECT fullkey, type, atom FROM json_tree('{"items":[{"sku":"a","qty":2},{"sku":"b","qty":5}]}', '$.items[0]') WHERE atom IS NOT NULL ORDER BY fullkey"#,
    ];
    for sql in cases {
        assert_query_matches(&fconn, &rconn, sql);
    }
}
