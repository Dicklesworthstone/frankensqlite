//! bd-qvqd4 — Oracle-parity e2e: CREATE TABLE validation vs rusqlite.
//!
//! SQLite rejects a few malformed table definitions at CREATE time: a duplicate
//! column name (case-insensitively) — "duplicate column name: X" — and more than
//! one column-level PRIMARY KEY — "table T has more than one primary key". This
//! pins that frank rejects the same definitions (rather than silently accepting
//! the last/first), while still accepting the valid contrasts (a single PK, a
//! composite table-level PRIMARY KEY). Only the success/failure of each statement
//! is compared, on a fresh connection per statement.
#![recursion_limit = "512"]

use fsqlite::Connection;

/// Run a single DDL statement on fresh frank + rusqlite connections and record a
/// divergence if one engine accepts it and the other rejects it.
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
fn valid_table_definitions_ok() {
    asupersync::test_utils::run_test(|| async {
        // Well-formed definitions both engines accept.
        check_ddl(
            &[
                "CREATE TABLE t (a, b)",
                "CREATE TABLE t (a, b, c)",
                "CREATE TABLE t (a PRIMARY KEY, b)", // single column-level PK
                "CREATE TABLE t (a, b, PRIMARY KEY (a, b))", // composite table-level PK
            ],
            "valid_table_definitions_ok",
        )
        .await;
    });
}

#[test]
fn duplicate_column_name_rejected() {
    asupersync::test_utils::run_test(|| async {
        check_ddl(
            &[
                "CREATE TABLE t (a, a)",              // exact duplicate
                "CREATE TABLE t (a INTEGER, A TEXT)", // case-insensitive duplicate
                "CREATE TABLE t (x, y, x)",           // duplicate among more columns
            ],
            "duplicate_column_name_rejected",
        )
        .await;
    });
}

#[test]
fn multiple_primary_keys_rejected() {
    asupersync::test_utils::run_test(|| async {
        check_ddl(
            &[
                "CREATE TABLE t (a PRIMARY KEY, b PRIMARY KEY)",
                "CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER PRIMARY KEY)",
            ],
            "multiple_primary_keys_rejected",
        )
        .await;
    });
}
