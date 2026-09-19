//! Reproduce the explicit-main correlated lookup used by foreign_key_check.
//! Ordinary SQL rows only; this is not financial-data or live-provider proof.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

struct ProfileGuard;

impl Drop for ProfileGuard {
    fn drop(&mut self) {
        set_hot_path_profile_enabled(false);
    }
}

fn stock_rows(stock: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    let mut stmt = stock.prepare(sql).expect("stock prepare");
    let columns = stmt.column_count();
    stmt.query_map([], |row| {
        (0..columns)
            .map(|column| {
                Ok(match row.get_ref(column)? {
                    rusqlite::types::ValueRef::Null => SqliteValue::Null,
                    rusqlite::types::ValueRef::Integer(value) => SqliteValue::Integer(value),
                    rusqlite::types::ValueRef::Real(value) => SqliteValue::Float(value),
                    rusqlite::types::ValueRef::Text(value) => SqliteValue::Text(
                        std::str::from_utf8(value).expect("UTF-8 SQL text").into(),
                    ),
                    rusqlite::types::ValueRef::Blob(value) => SqliteValue::Blob(value.into()),
                })
            })
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .expect("stock query")
    .collect::<Result<Vec<_>, _>>()
    .expect("stock rows")
}

#[test]
fn main_foreign_key_check_seeks_and_still_reports_orphans() {
    asupersync::test_utils::run_test(|| async {
        for suffix in ["", " WITHOUT ROWID"] {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("foreign-keys.db");
            let stock = rusqlite::Connection::open(&path).expect("stock producer");
            stock.execute_batch(&format!(
                "PRAGMA foreign_keys=OFF;
                 CREATE TABLE parent(owner TEXT NOT NULL, ordinal INTEGER NOT NULL,
                     PRIMARY KEY(owner,ordinal)){suffix};
                 CREATE TABLE child(id INTEGER PRIMARY KEY, owner TEXT, ordinal INTEGER,
                     FOREIGN KEY(owner,ordinal) REFERENCES parent(owner,ordinal));
                 WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<255)
                     INSERT INTO parent SELECT 'group',i FROM n;
                 INSERT INTO child SELECT ordinal+1,owner,ordinal FROM parent;
                 INSERT INTO child VALUES(257,'group',256),(258,NULL,256),(259,'group',NULL);"
            )).expect("stock schema and rows");
            let expected = stock_rows(&stock, "PRAGMA foreign_key_check");
            assert_eq!(expected.len(), 1, "exactly one deliberately orphaned row");
            stock.close().expect("close producer");
            let conn = Connection::open(path.to_string_lossy()).await.expect("engine open");
            set_hot_path_profile_enabled(true);
            let guard = ProfileGuard;
            let before = hot_path_profile_snapshot().vdbe.opcodes_executed_total;
            let rows = conn.query("PRAGMA foreign_key_check").await.expect("engine FK check");
            let ops = hot_path_profile_snapshot().vdbe.opcodes_executed_total - before;
            drop(guard);
            let actual = rows.iter().map(|row| row.values().to_vec()).collect::<Vec<_>>();
            assert_eq!(actual, expected, "FK violations {suffix:?}");
            eprintln!("main_fk suffix={suffix:?} children=259 parents=256 opcodes={ops}");
            assert!(ops < 256 * 200, "qualified parent lookup scanned repeatedly: {ops}");
            conn.close().await.expect("engine close");
        }
    });
}
