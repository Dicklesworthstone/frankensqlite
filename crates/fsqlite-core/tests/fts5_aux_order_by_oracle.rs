#![recursion_limit = "512"]

//! bd-reality-check-1022-llk6g.18: an FTS5 auxiliary function in ORDER BY must
//! work like stock even when the term is not also a result column. v0.4.12
//! failed `SELECT rowid FROM docs WHERE docs MATCH 'q' ORDER BY bm25(docs)`
//! (the most common ranked-search shape) with "internal error: column not
//! found: docs": the hidden table-name argument was validated as a column.
//!
//! Oracle: bundled stock SQLite on the same corpus, for an in-memory table
//! fsqlite built and for a stock-written file fsqlite reopens (lazy segments).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const CORPUS: &[(&str, &str)] = &[
    ("alpha beta", "gamma"),
    ("alpha", "alpha alpha delta"),
    ("zeta", "alpha epsilon epsilon epsilon"),
    ("alpha alpha alpha", "omega"),
    ("theta", "beta"),
];

const QUERIES: &[&str] = &[
    "SELECT rowid FROM docs WHERE docs MATCH 'alpha' ORDER BY bm25(docs)",
    "SELECT rowid FROM docs WHERE docs MATCH 'alpha' ORDER BY bm25(docs) DESC",
    "SELECT rowid FROM docs WHERE docs MATCH 'alpha' ORDER BY bm25(docs, 10.0, 1.0)",
    "SELECT rowid FROM docs WHERE docs MATCH 'alpha' ORDER BY bm25(docs, 1.0, 10.0)",
    "SELECT title FROM docs WHERE docs MATCH 'alpha' ORDER BY bm25(docs), rowid",
    "SELECT rowid, bm25(docs) < 0 FROM docs WHERE docs MATCH 'alpha' ORDER BY bm25(docs)",
    "SELECT rowid FROM docs WHERE docs MATCH 'alpha' ORDER BY -bm25(docs)",
    "SELECT rowid FROM docs WHERE docs MATCH 'alpha' ORDER BY abs(bm25(docs)) DESC",
    "SELECT rowid FROM docs WHERE docs MATCH 'alpha' ORDER BY bm25(docs) LIMIT 2",
    "SELECT rowid FROM docs WHERE docs MATCH 'alpha OR beta' ORDER BY bm25(docs), rowid",
    "SELECT rowid FROM docs WHERE docs MATCH 'alpha' AND rowid > 1 ORDER BY bm25(docs)",
    "SELECT rowid FROM docs WHERE docs MATCH 'alpha' \
     ORDER BY highlight(docs, 0, '[', ']'), rowid",
];

fn frank_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f:.9e}"),
                    SqliteValue::Text(text) => text.to_string(),
                    SqliteValue::Blob(bytes) => format!("{bytes:?}"),
                })
                .collect()
        })
        .collect()
}

fn stock_rows(stock: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = stock.prepare(sql).expect("stock prepare");
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            (0..width)
                .map(|index| {
                    Ok(match row.get_ref(index)? {
                        rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                        rusqlite::types::ValueRef::Integer(n) => n.to_string(),
                        rusqlite::types::ValueRef::Real(f) => format!("{f:.9e}"),
                        rusqlite::types::ValueRef::Text(text) => {
                            String::from_utf8_lossy(text).into_owned()
                        }
                        rusqlite::types::ValueRef::Blob(bytes) => format!("{bytes:?}"),
                    })
                })
                .collect()
        })
        .expect("stock query")
        .collect::<rusqlite::Result<_>>()
        .expect("stock rows")
}

fn seed_stock(stock: &rusqlite::Connection) {
    stock
        .execute_batch("CREATE VIRTUAL TABLE docs USING fts5(title, body);")
        .expect("stock create");
    for (title, body) in CORPUS {
        stock
            .execute(
                "INSERT INTO docs(title, body) VALUES (?1, ?2)",
                rusqlite::params![title, body],
            )
            .expect("stock insert");
    }
}

async fn assert_queries_match(label: &str, conn: &Connection, stock: &rusqlite::Connection) {
    for sql in QUERIES {
        let expected = stock_rows(stock, sql);
        let rows = conn
            .query(sql)
            .await
            .unwrap_or_else(|error| panic!("{label}: fsqlite failed `{sql}`: {error}"));
        assert_eq!(frank_rows(&rows), expected, "{label}: `{sql}`");
    }
}

#[test]
fn fts5_aux_function_order_by_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let stock = rusqlite::Connection::open_in_memory().expect("stock open");
        seed_stock(&stock);

        // A table fsqlite builds itself.
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute("CREATE VIRTUAL TABLE docs USING fts5(title, body);")
            .await
            .expect("create");
        for (title, body) in CORPUS {
            conn.execute(&format!(
                "INSERT INTO docs(title, body) VALUES ('{title}', '{body}')"
            ))
            .await
            .expect("insert");
        }
        assert_queries_match("in-memory", &conn, &stock).await;
        conn.close().await.expect("close");

        // A stock-written file, whose segments fsqlite binds lazily.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fts5_order_by.db");
        {
            let writer = rusqlite::Connection::open(&path).expect("stock file");
            seed_stock(&writer);
        }
        let conn = Connection::open(path.to_str().expect("utf8 path"))
            .await
            .expect("open file");
        assert_queries_match("stock-file", &conn, &stock).await;
        conn.close().await.expect("close file");
    });
}
