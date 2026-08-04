//! bd-watzr — Oracle-parity e2e: constraint expression validation vs rusqlite.
//!
//! SQLite restricts the expressions allowed in column constraints at CREATE time:
//! a DEFAULT must be a *constant* — it may not reference another column or contain
//! a subquery ("default value of column X is not constant"), and a CHECK
//! constraint may not contain a subquery ("subqueries prohibited in CHECK
//! constraints"). This pins that frank accepts the constant/simple forms and
//! rejects the non-constant DEFAULT and subquery-in-CHECK forms. Only statement
//! success/failure is compared, on a fresh connection per statement.
#![recursion_limit = "512"]

use fsqlite::Connection;

async fn check_ddl(stmts: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for s in stmts {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => {
                mismatches.push(format!("FRANK_OK / CSQL_ERR: `{s}`\n  csql: ERROR({e})"))
            }
            (Err(e), Ok(())) => {
                mismatches.push(format!("FRANK_ERR / CSQL_OK: `{s}`\n  frank: ERROR({e})"))
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn constant_default_and_simple_check_ok() {
    asupersync::test_utils::run_test(|| async {
        check_ddl(
            &[
                "CREATE TABLE t (a, b DEFAULT (1 + 2))", // constant expression
                "CREATE TABLE t (a DEFAULT 5)",          // literal
                "CREATE TABLE t (a DEFAULT NULL)",       // NULL
                "CREATE TABLE t (a, CHECK (a > 0))",     // simple CHECK
                "CREATE TABLE t (a, b, CHECK (a < b))",  // CHECK over columns (no subquery)
            ],
            "constant_default_and_simple_check_ok",
        )
        .await;
    });
}

#[test]
fn non_constant_default_rejected() {
    asupersync::test_utils::run_test(|| async {
        check_ddl(
            &[
                "CREATE TABLE t (a, b DEFAULT (a))", // references another column
                "CREATE TABLE t (a, b DEFAULT (a + 1))", // references another column
                "CREATE TABLE t (a DEFAULT (SELECT 1))", // subquery in DEFAULT
            ],
            "non_constant_default_rejected",
        )
        .await;
    });
}

#[test]
fn subquery_in_check_rejected() {
    asupersync::test_utils::run_test(|| async {
        check_ddl(
            &[
                "CREATE TABLE t (a, CHECK (a IN (SELECT 1)))", // subquery in CHECK
                "CREATE TABLE t (a, CHECK ((SELECT 1) = 1))",  // subquery in CHECK
            ],
            "subquery_in_check_rejected",
        )
        .await;
    });
}
