//! bd-8mtyj — Oracle-parity e2e: UPDATE/DELETE unknown-column rejection vs rusqlite.
//!
//! dml_update_delete_oracle / update_set_eval_oracle cover happy-path UPDATE and
//! DELETE. SQLite rejects an UPDATE/DELETE that names a column not in the target
//! table — whether the unknown name appears on the SET left-hand-side, in the
//! WHERE clause, or in a SET expression — with "no such column: <name>". This
//! pins that frank rejects the same shapes, alongside a working valid contrast.
//! Only statement success/failure is compared on a fresh connection per case.

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
    for (setup, test) in cases {
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

const SETUP: &[&str] = &[
    "CREATE TABLE t (a INTEGER, b INTEGER)",
    "INSERT INTO t VALUES (1,10),(2,20)",
];

#[test]
fn update_delete_valid_ok() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                (SETUP, "UPDATE t SET a = a + 1"),
                (SETUP, "UPDATE t SET b = 99 WHERE a = 1"),
                (SETUP, "DELETE FROM t WHERE a = 2"),
            ],
            "update_delete_valid_ok",
        )
        .await;
    });
}

#[test]
fn update_delete_unknown_column_rejected() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                // SET left-hand-side names a column that does not exist
                (SETUP, "UPDATE t SET nope = 1"),
                // WHERE references an unknown column
                (SETUP, "UPDATE t SET a = 1 WHERE nope = 1"),
                // SET expression references an unknown column
                (SETUP, "UPDATE t SET a = nope + 1"),
                // DELETE WHERE references unknown column
                (SETUP, "DELETE FROM t WHERE nope = 1"),
            ],
            "update_delete_unknown_column_rejected",
        )
        .await;
    });
}
