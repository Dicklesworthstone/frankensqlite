//! bd-u2dim — Oracle-parity e2e: AUTOINCREMENT placement validation vs rusqlite.
//!
//! autoincrement_edge_oracle covers AUTOINCREMENT *behavior*; this pins the
//! placement rule. SQLite only allows `AUTOINCREMENT` on a true `INTEGER PRIMARY
//! KEY` column (a rowid alias) and rejects every other position ("AUTOINCREMENT
//! is only allowed on an INTEGER PRIMARY KEY", or a parse error). This confirms
//! frank accepts the one valid form and rejects the misplaced ones, rather than
//! silently ignoring the keyword. Only statement success/failure is compared, on
//! a fresh connection per statement.

use fsqlite::Connection;

fn check_ddl(stmts: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for s in stmts {
        let f = Connection::open(":memory:").expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        let fe = f.execute(s);
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
fn autoincrement_valid_on_integer_pk_ok() {
    check_ddl(
        &[
            "CREATE TABLE t (a INTEGER PRIMARY KEY AUTOINCREMENT)",
            "CREATE TABLE t (a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)",
        ],
        "autoincrement_valid_on_integer_pk_ok",
    );
}

#[test]
fn autoincrement_without_primary_key_rejected() {
    // AUTOINCREMENT with no PRIMARY KEY is a (parse-level) error on both engines.
    check_ddl(
        &[
            "CREATE TABLE t (a TEXT AUTOINCREMENT)", // not INTEGER, no PK
            "CREATE TABLE t (a INTEGER AUTOINCREMENT)", // INTEGER but no PRIMARY KEY
            "CREATE TABLE t (a AUTOINCREMENT)",      // no type, no PK
        ],
        "autoincrement_without_primary_key_rejected",
    );
}

#[test]
fn autoincrement_on_non_integer_pk_rejected() {
    // PRIMARY KEY present but the column type is not INTEGER -> SQLite errors
    // "AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY".
    check_ddl(
        &["CREATE TABLE t (a TEXT PRIMARY KEY AUTOINCREMENT)"],
        "autoincrement_on_non_integer_pk_rejected",
    );
}
