#![recursion_limit = "512"]

//! bd-z0qqu: SQLite resolves a name inside an ORDER BY expression to a FROM
//! column first and then to a result-column alias, so
//! `SELECT a.c AS k FROM a JOIN b ... ORDER BY lower(k)` sorts by `lower(a.c)`.
//! fsqlite resolved result aliases only for a bare ORDER BY name: an alias
//! inside an expression failed with "internal error: column not found: k" or
//! "no such column: k" (joins, and grouped single-table queries). In-memory
//! joins also sorted a bare name two sources share (`ORDER BY id` over
//! `a.id, b.id`) by the first result column instead of reporting
//! "ambiguous column name: id".
//!
//! Each query is compared with rusqlite (rows, or failure), ad hoc and
//! prepared, in memory and file-backed.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{b:?}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE a(id INTEGER PRIMARY KEY, t TEXT, n INTEGER, c TEXT COLLATE NOCASE)",
    "INSERT INTO a VALUES (1,'1',1,'b'),(2,'2',2,'A'),(3,'01',3,'a'),(4,NULL,NULL,'B')",
    "CREATE TABLE b(id INTEGER PRIMARY KEY, an INTEGER, nm TEXT)",
    "INSERT INTO b VALUES (10,1,'n10'),(11,2,'N11'),(12,3,'n12'),(13,2,'n13')",
    "CREATE INDEX b_an ON b(an)",
];

const QUERIES: &[&str] = &[
    // Single table.
    "SELECT c AS k FROM a ORDER BY lower(k), id",
    "SELECT c AS k, count(*) FROM a GROUP BY c ORDER BY lower(k)",
    "SELECT c AS k, count(*) AS cnt FROM a GROUP BY c ORDER BY cnt * -1, lower(k)",
    "SELECT id AS t FROM a ORDER BY t + 0",
    "SELECT n AS t FROM a ORDER BY t || ''",
    // Joins.
    "SELECT a.c AS k, b.id FROM a JOIN b ON b.an = a.id ORDER BY lower(k), b.id",
    "SELECT a.id, b.id, a.c AS k FROM a LEFT JOIN b ON b.an = a.id ORDER BY lower(k), a.id, b.id LIMIT 3",
    "SELECT a.id, b.id, a.c AS k FROM a LEFT JOIN b ON b.an = a.id \
     ORDER BY lower(k), a.id, b.id LIMIT 3 OFFSET 2",
    "SELECT a.n AS t, b.id FROM a JOIN b ON b.an = a.id ORDER BY t || '', b.id",
    "SELECT a.c AS k, count(*) AS cnt FROM a JOIN b ON b.an = a.id GROUP BY a.c \
     ORDER BY cnt * -1, lower(k)",
    "SELECT b.nm AS nm FROM a JOIN b ON b.an = a.id ORDER BY nm || '', b.id",
    "SELECT a.c AS k FROM a LEFT JOIN b ON b.an = a.id \
     ORDER BY substr(k, 1, 1) COLLATE BINARY, a.id, b.id",
    "SELECT upper(a.c) AS k, b.id FROM a JOIN b ON b.an = a.id ORDER BY substr(k, 1, 1), a.id, b.id",
    "SELECT a.id AS k, b.id FROM a JOIN b ON b.an = a.id ORDER BY k + 0 DESC, b.id",
    // A bare alias, and a bare name only one source has, still resolve.
    "SELECT a.c AS k, b.id FROM a JOIN b ON b.an = a.id ORDER BY k, b.id",
    "SELECT a.id, b.nm FROM a JOIN b ON b.an = a.id ORDER BY nm, a.id",
    // A bare name two sources share is ambiguous.
    "SELECT a.id, b.id FROM a JOIN b ON b.an = a.id ORDER BY id",
    "SELECT a.id, b.id FROM a LEFT JOIN b ON b.an = a.id ORDER BY id DESC",
    "SELECT a.id AS id, b.id FROM a JOIN b ON b.an = a.id ORDER BY id, b.id",
];

fn rows_r(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let ncol = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            Ok((0..ncol)
                .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                .collect())
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<Vec<String>>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(rows)
}

fn same_outcome(
    franken: Result<Vec<fsqlite_core::connection::Row>, fsqlite_error::FrankenError>,
    stock: &Result<Vec<Vec<String>>, String>,
    what: &str,
) {
    match (franken, stock) {
        (Ok(rows), Ok(expected)) => {
            let rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect();
            assert_eq!(&rows, expected, "{what}");
        }
        (Err(error), Err(expected)) => assert!(
            error.to_string().contains("ambiguous column name"),
            "{what}: franken `{error}`, stock `{expected}`"
        ),
        (franken, stock) => panic!("{what}: franken {franken:?}, stock {stock:?}"),
    }
}

#[test]
fn order_by_expressions_resolve_result_aliases_after_from_columns() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("bd_z0qqu.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in QUERIES {
                let stock = rows_r(&r, sql);
                same_outcome(f.query(sql).await, &stock, &format!("query `{sql}`"));
                let prepared = match f.prepare(sql).await {
                    Ok(stmt) => stmt.query().await,
                    Err(error) => Err(error),
                };
                same_outcome(prepared, &stock, &format!("prepared `{sql}`"));
            }
        });
    }
}
