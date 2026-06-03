//! bd-d3z71 — Oracle-parity e2e: RELEASE/ROLLBACK TO nonexistent savepoint.
//!
//! transaction_savepoint_oracle covers happy-path SAVEPOINT/RELEASE/ROLLBACK TO
//! and nested rollback. SQLite rejects `RELEASE <name>` or `ROLLBACK TO <name>`
//! when no savepoint with that name is active ("no such savepoint: <name>").
//! This pins frank rejects the same shapes, alongside the valid contrasts.

use fsqlite::Connection;

fn stmts_agreement(stmts: &[&str], label: &str) -> Option<String> {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in &stmts[..stmts.len() - 1] {
        let fe = f.execute(s);
        let re = r.execute_batch(s);
        if let (Err(_), Ok(())) | (Ok(_), Err(_)) = (&fe, &re) {
            return Some(format!("{label} setup: disagreement on `{s}`"));
        }
    }
    let last = stmts[stmts.len() - 1];
    let fe = f.execute(last);
    let re = r.execute_batch(last);
    match (&fe, &re) {
        (Ok(_), Ok(())) | (Err(_), Err(_)) => None,
        (Ok(_), Err(e)) => Some(format!("FRANK_OK / CSQL_ERR: `{last}`\n  csql: ERROR({e})")),
        (Err(e), Ok(())) => Some(format!(
            "FRANK_ERR / CSQL_OK: `{last}`\n  frank: ERROR({e})"
        )),
    }
}

fn check(cases: &[(&[&str], &str)], label: &str) {
    let mismatches: Vec<String> = cases
        .iter()
        .filter_map(|(stmts, l)| stmts_agreement(stmts, l))
        .collect();
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn savepoint_valid_ok() {
    check(
        &[
            (&["BEGIN", "SAVEPOINT sp1", "RELEASE sp1"], "sp1 release"),
            (
                &["BEGIN", "SAVEPOINT sp1", "ROLLBACK TO sp1"],
                "sp1 rollback-to",
            ),
        ],
        "savepoint_valid_ok",
    );
}

#[test]
fn release_or_rollback_to_nonexistent_rejected() {
    check(
        &[
            // RELEASE a name that was never declared
            (&["BEGIN", "SAVEPOINT sp1", "RELEASE nope"], "RELEASE nope"),
            // ROLLBACK TO a name that was never declared
            (
                &["BEGIN", "SAVEPOINT sp1", "ROLLBACK TO nope"],
                "ROLLBACK TO nope",
            ),
            // After RELEASE, the savepoint is gone -- using it again errors
            (
                &["BEGIN", "SAVEPOINT sp1", "RELEASE sp1", "RELEASE sp1"],
                "RELEASE after release",
            ),
        ],
        "release_or_rollback_to_nonexistent_rejected",
    );
}
