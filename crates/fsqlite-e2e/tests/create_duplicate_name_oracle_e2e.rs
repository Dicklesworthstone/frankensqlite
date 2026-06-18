//! bd-fozf6 — Oracle-parity e2e: CREATE TABLE/VIEW duplicate-name validation.
//!
//! SQLite rejects a CREATE that would name an object that already exists in the
//! schema — table-clashes-with-table, view-clashes-with-view, view-clashes-with-
//! table (and vice versa) — unless `IF NOT EXISTS` is used. This pins that
//! frank rejects the same conflicts and that IF NOT EXISTS silences them.

use fsqlite::Connection;

fn ddl_case(setup: &[&str], test: &str) -> Option<String> {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in setup {
        f.execute(s).unwrap();
        r.execute_batch(s).unwrap();
    }
    let fe = f.execute(test);
    let re = r.execute_batch(test);
    match (&fe, &re) {
        (Ok(_), Ok(())) | (Err(_), Err(_)) => None,
        (Ok(_), Err(e)) => Some(format!("FRANK_OK / CSQL_ERR: `{test}`\n  csql: ERROR({e})")),
        (Err(e), Ok(())) => Some(format!(
            "FRANK_ERR / CSQL_OK: `{test}`\n  frank: ERROR({e})"
        )),
    }
}

fn check(cases: &[(&[&str], &str)], label: &str) {
    let mismatches: Vec<String> = cases
        .iter()
        .filter_map(|(setup, test)| ddl_case(setup, test))
        .collect();
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn create_if_not_exists_on_existing_ok() {
    check(
        &[
            (
                &["CREATE TABLE t (a)"],
                "CREATE TABLE IF NOT EXISTS t (a)", // already exists -> no error
            ),
            (
                &["CREATE TABLE t (a)", "CREATE VIEW v AS SELECT a FROM t"],
                "CREATE VIEW IF NOT EXISTS v AS SELECT a FROM t",
            ),
            // IF NOT EXISTS silences a CROSS-kind clash too (SQLite behavior).
            (
                &["CREATE TABLE t (a)"],
                "CREATE VIEW IF NOT EXISTS t AS SELECT 1",
            ),
            (
                &["CREATE VIEW w AS SELECT 1"],
                "CREATE TABLE IF NOT EXISTS w (a)",
            ),
        ],
        "create_if_not_exists_on_existing_ok",
    );
}

#[test]
fn create_duplicate_same_kind_rejected() {
    // Same-kind duplicates are correctly rejected by both.
    check(
        &[
            (&["CREATE TABLE t (a)"], "CREATE TABLE t (b)"),
            (
                &["CREATE TABLE u (a)", "CREATE VIEW v AS SELECT a FROM u"],
                "CREATE VIEW v AS SELECT a FROM u",
            ),
        ],
        "create_duplicate_same_kind_rejected",
    );
}

#[test]
fn create_duplicate_cross_kind_rejected() {
    check(
        &[
            // view conflicts with existing table
            (&["CREATE TABLE t (a)"], "CREATE VIEW t AS SELECT 1"),
            // table conflicts with existing view
            (
                &["CREATE TABLE u (a)", "CREATE VIEW w AS SELECT a FROM u"],
                "CREATE TABLE w (a)",
            ),
        ],
        "create_duplicate_cross_kind_rejected",
    );
}
