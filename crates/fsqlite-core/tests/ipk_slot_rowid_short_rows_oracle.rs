#![recursion_limit = "512"]

//! Rows that store their own rowid in the INTEGER PRIMARY KEY slot, made
//! short by ALTER TABLE ADD COLUMN.
//!
//! SQLite writes NULL in the slot, but older FrankenSQLite (0.3.x) wrote the
//! rowid there. SQLite reads the slot as present whatever it holds. After an
//! ADD COLUMN these rows are shorter than the table, and FrankenSQLite read
//! them as its legacy layout that omits the slot: every column after the
//! alias shifted by one (an mcp_agent_mail mailbox lost all 42k existing
//! messages this way).
//!
//! The fixture writes the old layout with stock SQLite: a plain `id INTEGER`
//! column holding the rowid, retyped to INTEGER PRIMARY KEY through
//! writable_schema. Reads and writes are compared with rusqlite.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const CRAFT: &str = "
    CREATE TABLE m(id INTEGER, project_id INTEGER NOT NULL, topic TEXT, subject TEXT NOT NULL, n INTEGER);
    INSERT INTO m(rowid, id, project_id, topic, subject, n) VALUES
      (1, 1, 10, NULL, 's1', 5), (2, 2, 20, 'tp', 's2', 6), (3, 3, 3, 'x', 's3', NULL),
      (7, 7, 30, 'y', 's7', 8), (8, NULL, 40, 'z', 's8', 9);
    CREATE INDEX m_p ON m(project_id);
    CREATE INDEX m_topic ON m(topic);
    PRAGMA writable_schema = ON;
    UPDATE sqlite_schema
       SET sql = 'CREATE TABLE m(id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL, topic TEXT, subject TEXT NOT NULL, n INTEGER)'
     WHERE name = 'm';
    PRAGMA writable_schema = OFF;
";

const QUERIES: &[&str] = &[
    "SELECT * FROM m ORDER BY id",
    "SELECT id, project_id, subject FROM m WHERE project_id = 20",
    "SELECT id, subject, extra FROM m WHERE topic = 'x'",
    "SELECT subject, n FROM m WHERE id = 3",
    "SELECT count(*), sum(project_id), sum(n), group_concat(subject) FROM m",
    "SELECT length(subject), substr(subject, 1, 1), octet_length(topic) FROM m ORDER BY id",
    "SELECT a.id, b.id FROM m a JOIN m b ON b.project_id = a.id ORDER BY 1, 2",
];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{}", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{}", b.len()),
    }
}

fn rusqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..ncol)
            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect())
    })
    .expect("rusqlite query")
    .map(|r| r.unwrap())
    .collect()
}

async fn franken_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

fn craft(path: &std::path::Path, alters: &str) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(CRAFT).unwrap();
    drop(conn);
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(alters).unwrap();
}

const WRITES: &[&str] = &[
    "UPDATE m SET subject = subject || '!' WHERE id = 2",
    "UPDATE m SET extra = 'e' WHERE project_id = 30",
    "DELETE FROM m WHERE id = 3",
    "INSERT INTO m(project_id, topic, subject) VALUES (50, 'w', 's9')",
];

#[test]
fn short_rows_with_rowid_in_ipk_slot_match_sqlite() {
    for alters in [
        "ALTER TABLE m ADD COLUMN extra TEXT;",
        "ALTER TABLE m ADD COLUMN extra TEXT; ALTER TABLE m ADD COLUMN tail INTEGER DEFAULT 4;",
    ] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let franken_path = dir.path().join("franken.db");
            let stock_path = dir.path().join("stock.db");
            craft(&franken_path, alters);
            craft(&stock_path, alters);

            let stock = rusqlite::Connection::open(&stock_path).unwrap();
            let conn = Connection::open(franken_path.to_str().unwrap())
                .await
                .unwrap();
            for sql in QUERIES {
                assert_eq!(
                    franken_rows(&conn, sql).await,
                    rusqlite_rows(&stock, sql),
                    "mismatch on `{sql}` after `{alters}`"
                );
            }
            for sql in WRITES {
                conn.execute(sql).await.unwrap();
                stock.execute(sql, []).unwrap();
            }
            for sql in QUERIES {
                assert_eq!(
                    franken_rows(&conn, sql).await,
                    rusqlite_rows(&stock, sql),
                    "mismatch on `{sql}` after writes and `{alters}`"
                );
            }
            conn.close().await.unwrap();

            let written = rusqlite::Connection::open(&franken_path).unwrap();
            for sql in QUERIES {
                assert_eq!(
                    rusqlite_rows(&written, sql),
                    rusqlite_rows(&stock, sql),
                    "stock reads the FrankenSQLite-written file differently on `{sql}`"
                );
            }
            assert_eq!(
                rusqlite_rows(&written, "PRAGMA integrity_check"),
                vec![vec!["'ok'".to_owned()]]
            );
        });
    }
}
