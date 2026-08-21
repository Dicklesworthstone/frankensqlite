// Keeper for bd-alter-create-nonconstant-default-internal-vs-err-it66z: ALTER
// TABLE ADD COLUMN / column-DEFAULT rejections must match SQLite's messages
// VERBATIM under SQLITE_ERROR — never wrapped as an Internal error (which would
// prefix "internal error:" and report SQLITE_INTERNAL) and never with an extra
// column name appended.
// Oracle: sqlite3 3.46.1.
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Render a scalar value's text (for `quote()`/`typeof()` assertions).
fn text_of(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.to_string(),
        other => format!("{other:?}"),
    }
}

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
fn bare_identifier_default_is_string_literal_it66z() {
    // SQLite quirk: a BARE (unparenthesized, unquoted) identifier after DEFAULT is
    // a STRING LITERAL, not a column reference — a column DEFAULT cannot reference
    // other columns. `DEFAULT abc` yields the string 'abc' (accepted); the
    // parenthesized `DEFAULT (abc)` stays an expression rejected as non-constant
    // (see nonconstant_default_error_message_it66z). Keyword constants
    // (TRUE/FALSE/NULL/CURRENT_*) lex as keywords and keep their literal meaning.
    // Oracle: sqlite3 3.46.1 — `DEFAULT abc` => quote(b)='abc', typeof(b)=text.
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("CREATE TABLE t(a, b DEFAULT abc)")
            .await
            .expect("bare-identifier DEFAULT is accepted as a string literal");
        c.execute("INSERT INTO t(a) VALUES (1)").await.unwrap();
        let rows = c.query("SELECT quote(b), typeof(b) FROM t").await.unwrap();
        assert_eq!(rows.len(), 1);
        let v = rows[0].values();
        assert_eq!(text_of(&v[0]), "'abc'", "b defaults to the TEXT literal 'abc'");
        assert_eq!(text_of(&v[1]), "text", "typeof(b) is text");

        // A keyword-constant DEFAULT keeps its literal (non-string) meaning — the
        // bare-id path must not swallow TRUE/FALSE/NULL/CURRENT_*.
        let c2 = Connection::open(":memory:").await.unwrap();
        c2.execute("CREATE TABLE t2(a, b DEFAULT true)").await.unwrap();
        c2.execute("INSERT INTO t2(a) VALUES (1)").await.unwrap();
        let r2 = c2.query("SELECT typeof(b) FROM t2").await.unwrap();
        assert_eq!(
            text_of(&r2[0].values()[0]),
            "integer",
            "DEFAULT true stays a boolean/integer, not the string 'true'"
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
