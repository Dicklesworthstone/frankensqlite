// Keeper for bd-s9irk (part): an UNNAMED CHECK constraint violation reports the
// VERBATIM check expression source (`a>0`, `a > 0`) rather than an AST re-render
// (`a > 0` for a `a>0` source). Oracle: sqlite3 3.46.1.
// (Named CHECK -> constraint name, and INTEGER-PK -> UNIQUE message, remain.)
use fsqlite_core::connection::Connection;

async fn err_of(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        c.execute(s).await.unwrap();
    }
    c.execute(sql)
        .await
        .expect_err("CHECK violation should be rejected")
        .to_string()
}

#[test]
fn unnamed_check_violation_reports_verbatim_expr_s9irk() {
    asupersync::test_utils::run_test(|| async {
        // Column-level CHECK, no surrounding spaces in the source.
        assert_eq!(
            err_of(&["CREATE TABLE t(a CHECK(a>0))"], "INSERT INTO t VALUES(-1)").await,
            "CHECK constraint failed: a>0",
        );
        // Spaces in the source are preserved verbatim.
        assert_eq!(
            err_of(&["CREATE TABLE u(a CHECK(a > 0))"], "INSERT INTO u VALUES(-1)").await,
            "CHECK constraint failed: a > 0",
        );
        // Table-level CHECK.
        assert_eq!(
            err_of(&["CREATE TABLE w(a, CHECK(a>0))"], "INSERT INTO w VALUES(-1)").await,
            "CHECK constraint failed: a>0",
        );
    });
}

#[test]
fn named_check_violation_reports_constraint_name_s9irk() {
    asupersync::test_utils::run_test(|| async {
        // A NAMED CHECK reports the constraint NAME, not the expression.
        assert_eq!(
            err_of(&["CREATE TABLE t(a, CONSTRAINT pos CHECK(a>0))"], "INSERT INTO t VALUES(-1)").await,
            "CHECK constraint failed: pos",
        );
        // Column-level named CHECK.
        assert_eq!(
            err_of(
                &["CREATE TABLE u(a INTEGER CONSTRAINT pos CHECK(a>0))"],
                "INSERT INTO u VALUES(-1)",
            )
            .await,
            "CHECK constraint failed: pos",
        );
    });
}
