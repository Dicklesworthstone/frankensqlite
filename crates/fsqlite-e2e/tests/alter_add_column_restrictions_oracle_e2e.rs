//! bd-eikt1 — Oracle-parity e2e: ALTER TABLE ADD COLUMN restrictions vs rusqlite.
//!
//! alter_column_oracle covers the ADD COLUMN happy paths. SQLite places hard
//! restrictions on the column an ADD COLUMN may introduce: it may not be a
//! PRIMARY KEY ("Cannot add a PRIMARY KEY column") nor UNIQUE ("Cannot add a
//! UNIQUE column"). This pins that frank accepts the legal additions and rejects
//! those two illegal ones. (The NOT-NULL-without-default rule interacts with
//! whether the table has rows and is split out separately.) Only statement
//! success/failure is compared, with the base table recreated per case.
#![recursion_limit = "512"]

use fsqlite::Connection;

/// Create `t(a INTEGER)`, run any extra setup, then run `alter` and return a
/// mismatch description if the two engines disagree on success/failure.
async fn alter_case(extra_setup: &[&str], alter: &str) -> Option<String> {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    f.execute("CREATE TABLE t (a INTEGER)").await.unwrap();
    r.execute_batch("CREATE TABLE t (a INTEGER)").unwrap();
    for s in extra_setup {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    let fe = f.execute(alter).await;
    let re = r.execute_batch(alter);
    match (&fe, &re) {
        (Ok(_), Ok(())) | (Err(_), Err(_)) => None,
        (Ok(_), Err(e)) => Some(format!(
            "FRANK_OK / CSQL_ERR: `{alter}`\n  csql: ERROR({e})"
        )),
        (Err(e), Ok(())) => Some(format!(
            "FRANK_ERR / CSQL_OK: `{alter}`\n  frank: ERROR({e})"
        )),
    }
}

async fn check(cases: &[(&[&str], &str)], label: &str) {
    let mut mismatches: Vec<String> = Vec::new();
    for (setup, alter) in cases {
        if let Some(m) = alter_case(setup, alter).await {
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

#[test]
fn add_column_legal_ok() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                (&[], "ALTER TABLE t ADD COLUMN b TEXT"),
                (&[], "ALTER TABLE t ADD COLUMN b INTEGER DEFAULT 5"),
                (&[], "ALTER TABLE t ADD COLUMN b TEXT NOT NULL DEFAULT 'x'"),
            ],
            "add_column_legal_ok",
        )
        .await;
    });
}

#[test]
fn add_primary_key_or_unique_column_rejected() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                (&[], "ALTER TABLE t ADD COLUMN b INTEGER PRIMARY KEY"), // cannot add PRIMARY KEY
                (&[], "ALTER TABLE t ADD COLUMN b TEXT UNIQUE"),         // cannot add UNIQUE
            ],
            "add_primary_key_or_unique_column_rejected",
        )
        .await;
    });
}

#[test]
fn add_not_null_without_default_when_rows_exist() {
    // With existing rows, back-filling NULL into a NOT NULL column is impossible,
    // so both engines reject ADD COLUMN ... NOT NULL (no default).
    asupersync::test_utils::run_test(|| async {
        check(
            &[(
                &["INSERT INTO t VALUES (1)"],
                "ALTER TABLE t ADD COLUMN b TEXT NOT NULL",
            )],
            "add_not_null_without_default_when_rows_exist",
        )
        .await;
    });
}

#[test]
fn add_not_null_without_default_empty_table() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[(&[], "ALTER TABLE t ADD COLUMN b TEXT NOT NULL")],
            "add_not_null_without_default_empty_table",
        )
        .await;
    });
}
