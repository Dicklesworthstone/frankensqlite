//! bd-r3l5b — Oracle-parity e2e: RENAME COLUMN / TABLE validation vs rusqlite.
//!
//! rename_propagation_oracle covers the happy-path RENAME COLUMN and RENAME TO.
//! This pins the validation errors: renaming a column that does not exist on the
//! target table ("no such column"), and renaming a table that does not exist
//! ("no such table"). The valid contrasts must still work.
//! Only statement success/failure is compared, on a fresh connection per case.

use fsqlite::Connection;

async fn ddl_case(setup: &[&str], test: &str) -> Option<String> {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in setup {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    let fe = f.execute(test).await;
    let re = r.execute_batch(test);
    match (&fe, &re) {
        (Ok(_), Ok(())) | (Err(_), Err(_)) => None,
        (Ok(_), Err(e)) => Some(format!("FRANK_OK / CSQL_ERR: `{test}`\n  csql: ERROR({e})")),
        (Err(e), Ok(())) => Some(format!(
            "FRANK_ERR / CSQL_OK: `{test}`\n  frank: ERROR({e})"
        )),
    }
}

async fn check(cases: &[(&[&str], &str)], label: &str) {
    let mut mismatches: Vec<String> = Vec::new();
    for (setup, test) in cases.iter() {
        if let Some(m) = ddl_case(setup, test).await {
            mismatches.push(m);
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

const SETUP: &[&str] = &["CREATE TABLE t (a INTEGER, b INTEGER)"];

#[test]
fn rename_valid_ok() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                (SETUP, "ALTER TABLE t RENAME COLUMN a TO a2"),
                (SETUP, "ALTER TABLE t RENAME TO t2"),
            ],
            "rename_valid_ok",
        )
        .await;
    });
}

#[test]
fn rename_unknown_rejected() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                (SETUP, "ALTER TABLE t RENAME COLUMN nope TO x"), // unknown column
                (SETUP, "ALTER TABLE nope_table RENAME COLUMN a TO b"), // unknown table
                (SETUP, "ALTER TABLE nope_table RENAME TO other"), // unknown table
            ],
            "rename_unknown_rejected",
        )
        .await;
    });
}
