//! bd-ecibg:
//! 1. `PreparedStatement::query()` / `query_with_params()` / `query_with_params_for_each()` /
//!    `query_row()` on non-RETURNING DML (INSERT/UPDATE/DELETE) must step the write and return
//!    empty rows (or `QueryReturnedNoRows` for `query_row`), matching stock SQLite / rusqlite.
//! 2. `Connection::execute()`, `Connection::execute_with_params()`, and
//!    `PreparedStatement::execute()` return `sqlite3_changes()` (the previous DML's count) for
//!    DDL and control statements (CREATE/ALTER/DROP/BEGIN/COMMIT/zero-row PRAGMA), matching
//!    rusqlite 3.54.1.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;
use tempfile::tempdir;

fn read_table_stock(conn: &rusqlite::Connection) -> Vec<(i64, String)> {
    let mut stmt = conn
        .prepare("SELECT id, v FROM items ORDER BY id")
        .expect("stock prepare");
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("stock query")
        .map(|r| r.expect("stock row"))
        .collect()
}

async fn read_table_fsqlite(conn: &Connection) -> Vec<(i64, String)> {
    conn.query("SELECT id, v FROM items ORDER BY id")
        .await
        .expect("fsql query")
        .into_iter()
        .map(|row| match row.values() {
            [SqliteValue::Integer(id), SqliteValue::Text(v)] => (*id, v.to_string()),
            other => panic!("unexpected row: {other:?}"),
        })
        .collect()
}

#[test]
fn prepared_dml_query_and_ddl_execute_changes_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let _dir = if file_backed {
                Some(tempdir().expect("tempdir"))
            } else {
                None
            };
            let (stock, fsql) = if let Some(dir) = &_dir {
                let stock_path = dir.path().join("stock.db");
                let fsql_path = dir.path().join("fsql.db");
                (
                    rusqlite::Connection::open(&stock_path).expect("open stock file"),
                    Connection::open(fsql_path.to_str().expect("utf8 path"))
                        .await
                        .expect("open fsql file"),
                )
            } else {
                (
                    rusqlite::Connection::open_in_memory().expect("open stock mem"),
                    Connection::open(":memory:").await.expect("open fsql mem"),
                )
            };

            let create_sql = "CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT)";
            assert_eq!(
                fsql.execute(create_sql).await.expect("fsql create"),
                stock.execute(create_sql, []).expect("stock create"),
                "initial CREATE count (file_backed={file_backed})"
            );

            // Part 1: PreparedStatement::query() / query_with_params() / for_each() / query_row()
            // on INSERT, UPDATE, DELETE without RETURNING.
            {
                let ins_sql = "INSERT INTO items VALUES (?1, ?2)";
                let mut stock_ins = stock.prepare(ins_sql).expect("stock prepare ins");
                let mut srows = stock_ins.query(rusqlite::params![1, "a"]).expect("stock ins query");
                assert!(srows.next().expect("stock next").is_none());
                drop(srows);

                let fsql_ins = fsql.prepare(ins_sql).await.expect("fsql prepare ins");
                let frows = fsql_ins
                    .query_with_params(&[SqliteValue::Integer(1), SqliteValue::Text("a".into())])
                    .await
                    .expect("fsql prepared INSERT via query_with_params must step");
                assert!(frows.is_empty());

                // Unparameterized query() path.
                let ins2_sql = "INSERT INTO items VALUES (2, 'b')";
                let mut stock_ins2 = stock.prepare(ins2_sql).expect("stock prepare ins2");
                assert!(stock_ins2.query([]).expect("stock query").next().unwrap().is_none());
                let fsql_ins2 = fsql.prepare(ins2_sql).await.expect("fsql prepare ins2");
                assert!(fsql_ins2.query().await.expect("fsql ins2 query").is_empty());

                // query_with_params_for_each() path on INSERT.
                let mut stock_ins3 = stock.prepare(ins_sql).expect("stock prepare ins3");
                assert!(
                    stock_ins3
                        .query(rusqlite::params![3, "c"])
                        .expect("stock query")
                        .next()
                        .unwrap()
                        .is_none()
                );
                let mut visited = 0usize;
                fsql_ins
                    .query_with_params_for_each(
                        &[SqliteValue::Integer(3), SqliteValue::Text("c".into())],
                        |_| {
                            visited += 1;
                            Ok(())
                        },
                    )
                    .await
                    .expect("fsql prepared INSERT via for_each must step");
                assert_eq!(visited, 0);

                // query_row() on non-RETURNING DML steps write and returns QueryReturnedNoRows.
                let mut stock_ins4 = stock.prepare(ins_sql).expect("stock prepare ins4");
                let stock_qr = stock_ins4.query_row(rusqlite::params![4, "d"], |_| Ok(()));
                assert!(matches!(stock_qr, Err(rusqlite::Error::QueryReturnedNoRows)));
                let fsql_qr = fsql_ins
                    .query_row_with_params(&[SqliteValue::Integer(4), SqliteValue::Text("d".into())])
                    .await;
                assert!(
                    matches!(fsql_qr, Err(FrankenError::QueryReturnedNoRows)),
                    "expected QueryReturnedNoRows, got {fsql_qr:?}"
                );

                // UPDATE via prepared query_with_params.
                let upd_sql = "UPDATE items SET v = ?1 WHERE id = ?2";
                let mut stock_upd = stock.prepare(upd_sql).expect("stock prepare upd");
                assert!(
                    stock_upd
                        .query(rusqlite::params!["b2", 2])
                        .expect("stock upd query")
                        .next()
                        .unwrap()
                        .is_none()
                );
                let fsql_upd = fsql.prepare(upd_sql).await.expect("fsql prepare upd");
                assert!(
                    fsql_upd
                        .query_with_params(&[SqliteValue::Text("b2".into()), SqliteValue::Integer(2)])
                        .await
                        .expect("fsql prepared UPDATE via query_with_params must step")
                        .is_empty()
                );

                // DELETE via prepared query_with_params.
                let del_sql = "DELETE FROM items WHERE id = ?1";
                let mut stock_del = stock.prepare(del_sql).expect("stock prepare del");
                assert!(
                    stock_del
                        .query(rusqlite::params![4])
                        .expect("stock del query")
                        .next()
                        .unwrap()
                        .is_none()
                );
                let fsql_del = fsql.prepare(del_sql).await.expect("fsql prepare del");
                assert!(
                    fsql_del
                        .query_with_params(&[SqliteValue::Integer(4)])
                        .await
                        .expect("fsql prepared DELETE via query_with_params must step")
                        .is_empty()
                );

                // Verify table contents match stock byte-for-byte.
                assert_eq!(
                    read_table_fsqlite(&fsql).await,
                    read_table_stock(&stock),
                    "table state after prepared DML query() (file_backed={file_backed})"
                );
                assert_eq!(
                    read_table_fsqlite(&fsql).await,
                    vec![
                        (1, "a".to_owned()),
                        (2, "b2".to_owned()),
                        (3, "c".to_owned()),
                    ]
                );
            }

            // Part 2: execute() / execute_with_params() / PreparedStatement::execute()
            // return sqlite3_changes() across DDL and control statements.
            let seed_dml = "UPDATE items SET v = v || '!'";
            let stock_changes = stock.execute(seed_dml, []).expect("stock update 3");
            let fsql_changes = fsql.execute(seed_dml).await.expect("fsql update 3");
            assert_eq!(stock_changes, 3);
            assert_eq!(fsql_changes, 3);

            let ddl_and_control = [
                "CREATE TABLE extra(x INTEGER)",
                "CREATE INDEX idx_extra_x ON extra(x)",
                "CREATE VIEW v_extra AS SELECT x FROM extra",
                "ALTER TABLE extra ADD COLUMN y TEXT",
                "DROP VIEW v_extra",
                "DROP INDEX idx_extra_x",
                "DROP TABLE extra",
                "BEGIN",
                "COMMIT",
                "PRAGMA cache_size = 2000",
            ];

            for sql in ddl_and_control {
                let expected = stock.execute(sql, []).expect("stock ddl execute");
                assert_eq!(expected, 3, "stock changes() must stay 3 on `{sql}`");
                let actual = fsql.execute(sql).await.expect("fsql ddl execute");
                assert_eq!(
                    actual, expected,
                    "Connection::execute count mismatch on `{sql}` (file_backed={file_backed})"
                );
            }

            // Also verify execute_with_params() and PreparedStatement::execute() on DDL.
            for sql in [
                "CREATE TABLE extra2(x INTEGER)",
                "ALTER TABLE extra2 ADD COLUMN z INTEGER",
                "DROP TABLE extra2",
            ] {
                let expected = stock.execute(sql, []).expect("stock ddl2");
                assert_eq!(expected, 3);
                let actual = fsql
                    .execute_with_params(sql, &[])
                    .await
                    .expect("fsql execute_with_params ddl");
                assert_eq!(
                    actual, expected,
                    "Connection::execute_with_params count mismatch on `{sql}` (file_backed={file_backed})"
                );
            }

            for sql in [
                "CREATE TABLE extra3(x INTEGER)",
                "DROP TABLE extra3",
            ] {
                let expected = {
                    let mut stmt = stock.prepare(sql).expect("stock prepare ddl3");
                    stmt.execute([]).expect("stock stmt execute ddl3")
                };
                assert_eq!(expected, 3);
                let actual = {
                    let stmt = fsql.prepare(sql).await.expect("fsql prepare ddl3");
                    stmt.execute().await.expect("fsql stmt execute ddl3")
                };
                assert_eq!(
                    actual, expected,
                    "PreparedStatement::execute count mismatch on `{sql}` (file_backed={file_backed})"
                );
            }

            fsql.close().await.expect("close fsql");
        }
    });
}
