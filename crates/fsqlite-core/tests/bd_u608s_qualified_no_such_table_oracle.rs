//! bd-u608s: "no such table" names the table as written, with its schema
//! qualifier, as stock SQLite 3.54.1 does.
//!
//! `INSERT INTO main.x`, `UPDATE main.x`, `DELETE FROM main.x` and
//! `DROP TABLE main.x` dropped the qualifier, and so did `temp.`, an attached
//! schema and the user's spelling (`MAIN.`). So did the missing target of a
//! persistent trigger's INSERT / UPDATE / DELETE, which stock names under
//! `main.` because a trigger's body is fixed to the trigger's schema.
//!
//! Every message is compared with bundled SQLite 3.54.1 on `:memory:` and on
//! a file, direct and prepared.
//!
//! Negatives that an "always prefix main." fix fails:
//! - the unqualified statements stay unqualified;
//! - so does a TEMP trigger's body, which stock does not fix to a schema;
//! - the statements that already agreed (SELECT, CREATE INDEX) stay as they
//!   are.

use fsqlite_core::connection::Connection;

// One trigger per table: stock compiles every trigger of a table when a
// statement on it is prepared, whatever its WHEN, so a second trigger with a
// missing table would decide the message.
const SETUP: &[&str] = &[
    "CREATE TABLE t(a)",
    "CREATE TABLE t1(a)",
    "CREATE TABLE t2(a)",
    "CREATE TABLE t3(a)",
    "CREATE TABLE t4(a)",
    "ATTACH ':memory:' AS aux",
    "CREATE TRIGGER tr_ins AFTER INSERT ON t1 BEGIN INSERT INTO nosuch1 VALUES(1); END",
    "CREATE TRIGGER tr_upd AFTER INSERT ON t2 BEGIN UPDATE nosuch2 SET a = 1; END",
    "CREATE TRIGGER tr_del AFTER INSERT ON t3 BEGIN DELETE FROM nosuch3; END",
    "CREATE TEMP TRIGGER tr_tmp AFTER INSERT ON t4 BEGIN INSERT INTO nosuch4 VALUES(1); END",
];

/// Each must fail in both engines, with stock's message.
const FAILING: &[&str] = &[
    "SELECT * FROM nosuch",
    "SELECT * FROM main.nosuch",
    "INSERT INTO nosuch VALUES(1)",
    "INSERT INTO main.nosuch VALUES(1)",
    "INSERT INTO MAIN.nosuch VALUES(1)",
    "INSERT INTO temp.nosuch VALUES(1)",
    "INSERT INTO aux.nosuch VALUES(1)",
    "INSERT INTO main.nosuch SELECT 1",
    "UPDATE nosuch SET a = 1 WHERE a = 0",
    "UPDATE main.nosuch SET a = 1 WHERE a = 0",
    "DELETE FROM nosuch WHERE a = 0",
    "DELETE FROM main.nosuch WHERE a = 0",
    "DROP TABLE nosuch",
    "DROP TABLE main.nosuch",
    "DROP TABLE temp.nosuch",
    "DROP TABLE aux.nosuch",
    "CREATE INDEX i1 ON nosuch(a)",
    "INSERT INTO t1 VALUES(1)",
    "INSERT INTO t2 VALUES(2)",
    "INSERT INTO t3 VALUES(3)",
    "INSERT INTO t4 VALUES(4)",
];

fn stock_message(conn: &rusqlite::Connection, sql: &str) -> String {
    match conn.execute_batch(sql) {
        Ok(()) => panic!("stock SQLite accepted `{sql}`, which it must refuse"),
        Err(rusqlite::Error::SqliteFailure(_, Some(message))) => message,
        Err(other) => panic!("stock SQLite refused `{sql}` without a message: {other:?}"),
    }
}

#[test]
fn missing_tables_are_named_as_written_like_stock() {
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in SETUP {
        stock.execute_batch(sql).unwrap();
    }
    let expected: Vec<String> = FAILING.iter().map(|sql| stock_message(&stock, sql)).collect();
    // Not vacuous: stock qualifies some and not others.
    assert_eq!(expected[2], "no such table: nosuch");
    assert_eq!(expected[3], "no such table: main.nosuch");
    assert_eq!(expected[4], "no such table: MAIN.nosuch");
    assert_eq!(expected[17], "no such table: main.nosuch1");
    assert_eq!(expected[20], "no such table: nosuch4");
    asupersync::test_utils::run_test(|| async move {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path().join("u608s.db").to_str().unwrap().to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for sql in SETUP {
                    conn.execute(sql).await.unwrap();
                }
                let mut wrong = Vec::new();
                for (sql, want) in FAILING.iter().zip(&expected) {
                    // `prepare()` takes SELECT, INSERT, UPDATE, DELETE and PRAGMA
                    // only, so DROP and CREATE run directly in both passes.
                    let ddl = sql.starts_with("DROP") || sql.starts_with("CREATE");
                    let got = if prepared && !ddl {
                        match conn.prepare(sql).await {
                            Err(error) => Err(error),
                            Ok(statement) if sql.starts_with("SELECT") => {
                                statement.query().await.map(|_| ())
                            }
                            Ok(statement) => statement.execute().await.map(|_| ()),
                        }
                    } else {
                        conn.execute(sql).await.map(|_| ())
                    };
                    match got {
                        Ok(()) => wrong.push(format!("`{sql}`: accepted, stock says {want:?}")),
                        Err(error) if error.to_string() != *want => wrong.push(format!(
                            "`{sql}`: {:?}, stock says {want:?}",
                            error.to_string()
                        )),
                        Err(_) => {}
                    }
                }
                conn.close().await.unwrap();
                assert!(
                    wrong.is_empty(),
                    "file_backed={file_backed} prepared={prepared}:\n{}",
                    wrong.join("\n")
                );
            }
        }
    });
}
