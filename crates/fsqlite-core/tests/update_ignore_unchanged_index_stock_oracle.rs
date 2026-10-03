//! A rejected UPDATE must not duplicate an unchanged persistent index entry.

use fsqlite_core::connection::Connection;

const SETUP: [&str; 4] = [
    "CREATE TABLE u(id INTEGER PRIMARY KEY, b UNIQUE, c)",
    "CREATE INDEX u_c ON u(c)",
    "INSERT INTO u VALUES(1,10,1),(2,20,2)",
    "UPDATE OR IGNORE u SET b=10 WHERE id=2",
];

fn inspect(stock: &rusqlite::Connection) -> (Vec<String>, Vec<(i64, i64, i64)>) {
    let integrity = stock
        .prepare("PRAGMA integrity_check")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let indexed_rows = stock
        .prepare("SELECT id,b,c FROM u INDEXED BY u_c WHERE c=2 ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    (integrity, indexed_rows)
}

#[test]
fn ignored_update_preserves_unchanged_index_on_stock_reopen() {
    asupersync::test_utils::run_test(|| async {
        // Preserve the database on both success and failure for inspection.
        let retained = tempfile::tempdir().unwrap().keep();
        let database = retained.join("update-ignore.db");
        let frank = Connection::open(database.to_str().unwrap()).await.unwrap();
        let reference = rusqlite::Connection::open_in_memory().unwrap();
        for sql in SETUP {
            frank.execute(sql).await.unwrap();
            reference.execute_batch(sql).unwrap();
        }
        frank.close_without_checkpoint().await.unwrap();
        // Stock SQLite must be the first reader after closing FrankenSQLite.
        let reopened = rusqlite::Connection::open(&database).unwrap();
        let actual = inspect(&reopened);
        let expected = inspect(&reference);
        assert_eq!(expected, (vec!["ok".to_owned()], vec![(2, 20, 2)]));
        assert_eq!(actual, expected, "retained database: {}", database.display());
    });
}
