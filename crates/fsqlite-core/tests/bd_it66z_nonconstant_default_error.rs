// Keeper for bd-alter-create-nonconstant-default-internal-vs-err-it66z: ALTER
// TABLE ADD COLUMN / column-DEFAULT rejections must match SQLite's messages
// VERBATIM under SQLITE_ERROR — never wrapped as an Internal error (which would
// prefix "internal error:" and report SQLITE_INTERNAL) and never with an extra
// column name appended.
// Oracle: sqlite3 3.46.1.
use fsqlite_core::connection::Connection;

/// Run `sql` on a fresh in-memory connection (after `setup`) and return the
/// error's Display string; panics if it unexpectedly succeeds.
async fn err_of(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql)
        .await
        .expect_err("statement should have been rejected")
        .to_string()
}

fn assert_stock(msg: &str, expected: &str) {
    assert_eq!(msg, expected, "message must match stock verbatim");
    assert!(
        !msg.starts_with("internal error:"),
        "must not be wrapped as an Internal error (was: {msg:?})"
    );
}

#[test]
fn nonconstant_default_error_message_it66z() {
    asupersync::test_utils::run_test(|| async {
        const EXPECTED: &str = "default value of column [b] is not constant";
        // CREATE TABLE with a non-constant default (column reference).
        assert_stock(&err_of(&[], "CREATE TABLE t(a, b DEFAULT (a))").await, EXPECTED);
        // ALTER TABLE ADD COLUMN with a non-constant default.
        assert_stock(
            &err_of(&["CREATE TABLE t(a)"], "ALTER TABLE t ADD COLUMN b DEFAULT (a)").await,
            EXPECTED,
        );
    });
}

#[test]
fn add_primary_key_column_message_it66z() {
    asupersync::test_utils::run_test(|| async {
        assert_stock(
            &err_of(&["CREATE TABLE t(a)"], "ALTER TABLE t ADD COLUMN b INTEGER PRIMARY KEY").await,
            "Cannot add a PRIMARY KEY column",
        );
    });
}

#[test]
fn add_unique_column_message_it66z() {
    asupersync::test_utils::run_test(|| async {
        assert_stock(
            &err_of(&["CREATE TABLE t(a)"], "ALTER TABLE t ADD COLUMN b TEXT UNIQUE").await,
            "Cannot add a UNIQUE column",
        );
    });
}

#[test]
fn add_not_null_no_default_message_it66z() {
    asupersync::test_utils::run_test(|| async {
        // Non-empty table: back-filling a NOT NULL column with NULL is rejected.
        assert_stock(
            &err_of(
                &["CREATE TABLE t(a)", "INSERT INTO t VALUES(1)"],
                "ALTER TABLE t ADD COLUMN b TEXT NOT NULL",
            )
            .await,
            "Cannot add a NOT NULL column with default value NULL",
        );
    });
}

#[test]
fn add_not_null_no_default_empty_table_allowed_it66z() {
    // Regression guard for the has_rows fix: on an EMPTY table, ADD COLUMN
    // NOT NULL without a default is LEGAL (nothing to back-fill), so it must
    // still succeed — the row-presence probe returns false and the reject is
    // skipped.
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("CREATE TABLE t(a)").await.unwrap();
        c.execute("ALTER TABLE t ADD COLUMN b TEXT NOT NULL")
            .await
            .expect("NOT NULL column with no default is legal on an empty table");
    });
}

#[test]
fn rename_drop_no_such_column_message_it66z() {
    asupersync::test_utils::run_test(|| async {
        // Stock quotes the missing column name in the ALTER context and reports
        // SQLITE_ERROR (not Internal): `no such column: "zzz"`.
        assert_stock(
            &err_of(&["CREATE TABLE t(a, b)"], "ALTER TABLE t RENAME COLUMN zzz TO c").await,
            "no such column: \"zzz\"",
        );
        assert_stock(
            &err_of(&["CREATE TABLE t(a, b)"], "ALTER TABLE t DROP COLUMN zzz").await,
            "no such column: \"zzz\"",
        );
    });
}
