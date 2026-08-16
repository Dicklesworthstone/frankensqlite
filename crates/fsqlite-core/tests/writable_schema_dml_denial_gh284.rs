//! GH #284 part 2 (bd-gh-pragma-writable-schema): direct DML on `sqlite_master`
//! with `writable_schema` OFF must be rejected with the correct SQLITE_ERROR
//! ("table sqlite_master may not be modified"), like stock SQLite — not the
//! internal "no such table: sqlite_master" FrankenSQLite used to raise.

use fsqlite_core::connection::Connection;

async fn dml_error(conn: &Connection, sql: &str) -> String {
    conn.execute(sql)
        .await
        .expect_err(&format!("`{sql}` must be rejected"))
        .to_string()
}

#[test]
fn schema_table_dml_denied_with_correct_error() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(a)").await.unwrap();

        // writable_schema defaults OFF, so every direct DML on the schema table
        // must report "may not be modified", not the generic "no such table".
        for sql in [
            "UPDATE sqlite_master SET name='x' WHERE name='t'",
            "DELETE FROM sqlite_master WHERE name='t'",
            "INSERT INTO sqlite_master(type,name,tbl_name,rootpage,sql) \
             VALUES('table','x','x',2,'CREATE TABLE x(a)')",
            // The sqlite_schema alias is denied the same way.
            "UPDATE sqlite_schema SET name='x' WHERE name='t'",
        ] {
            let err = dml_error(&conn, sql).await;
            assert!(
                err.contains("may not be modified"),
                "[{sql}] expected 'may not be modified', got: {err}"
            );
            assert!(
                !err.contains("no such table"),
                "[{sql}] must not report 'no such table', got: {err}"
            );
        }
    });
}
