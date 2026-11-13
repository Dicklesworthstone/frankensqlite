// Integration tests are their own crate root and do not inherit the lib's
// `#![recursion_limit]`; match the 512 used by the other oracle suites.
#![recursion_limit = "512"]

//! GH #169 (bd-gh-check-affinity-ordering): a column's CHECK constraint must be
//! evaluated on the value AFTER column affinity is applied, exactly like stock
//! SQLite (which applies affinity, then evaluates CHECK). rusqlite is the oracle
//! for both the value stored and whether the statement succeeds.
//!
//! Before the fix, fsqlite emitted CHECK before `Opcode::Affinity`, so:
//!   - `CREATE TABLE t(a TEXT CHECK(typeof(a)='text')); INSERT INTO t VALUES(1);`
//!     wrongly FAILED (CHECK saw integer 1) though SQLite coerces 1 -> '1' first.
//!   - `CREATE TABLE u(b INTEGER CHECK(typeof(b)='text')); INSERT INTO u VALUES('5');`
//!     was wrongly ACCEPTED (CHECK saw text '5') though SQLite coerces '5' -> 5
//!     and rejects. Both directions are covered, on INSERT and UPDATE.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Compare Frank vs rusqlite on each read query; return human-readable mismatches.
async fn oracle_compare(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    queries: &[&str],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for query in queries {
        let frank_result = fconn.query(query).await;
        let csql_result: std::result::Result<Vec<Vec<String>>, String> = (|| {
            let mut stmt = rconn.prepare(query).map_err(|e| format!("prepare: {e}"))?;
            let col_count = stmt.column_count();
            let rows: Vec<Vec<String>> = stmt
                .query_map([], |row| {
                    let mut vals = Vec::new();
                    for i in 0..col_count {
                        let v: rusqlite::types::Value = row.get_unwrap(i);
                        let s = match v {
                            rusqlite::types::Value::Null => "NULL".to_owned(),
                            rusqlite::types::Value::Integer(n) => n.to_string(),
                            rusqlite::types::Value::Real(f) => format!("{f}"),
                            rusqlite::types::Value::Text(s) => format!("'{s}'"),
                            rusqlite::types::Value::Blob(b) => format!(
                                "X'{}'",
                                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                            ),
                        };
                        vals.push(s);
                    }
                    Ok(vals)
                })
                .map_err(|e| format!("query: {e}"))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| format!("row: {e}"))?;
            Ok(rows)
        })();
        match (frank_result, csql_result) {
            (Ok(rows), Ok(csql_rows)) => {
                let frank_strs: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| {
                        row.values()
                            .iter()
                            .map(|v| match v {
                                SqliteValue::Null => "NULL".to_owned(),
                                SqliteValue::Integer(n) => n.to_string(),
                                SqliteValue::Float(f) => format!("{f}"),
                                SqliteValue::Text(s) => format!("'{s}'"),
                                SqliteValue::Blob(b) => format!(
                                    "X'{}'",
                                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                                ),
                            })
                            .collect()
                    })
                    .collect();
                if frank_strs != csql_rows {
                    mismatches.push(format!(
                        "MISMATCH: {query}\n  frank: {frank_strs:?}\n  csql:  {csql_rows:?}"
                    ));
                }
            }
            (Ok(rows), Err(csql_err)) => mismatches.push(format!(
                "DIVERGE: {query}\n  frank: OK ({} rows)\n  csql:  ERROR({csql_err})",
                rows.len()
            )),
            (Err(e), Ok(csql_rows)) => mismatches.push(format!(
                "PAIR_FRANK_ERROR: {query}\n  frank: ERROR({e})\n  csql:  {csql_rows:?}"
            )),
            (Err(_), Err(_)) => {}
        }
    }
    mismatches
}

fn assert_no_mismatches(mismatches: &[String], label: &str) {
    if !mismatches.is_empty() {
        for m in mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} {label} mismatch(es)", mismatches.len());
    }
}

/// Apply statements to both engines, asserting they agree on success/failure.
async fn apply_checked(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    stmts: &[&str],
) -> Vec<String> {
    let mut diverged = Vec::new();
    for s in stmts {
        let f = fconn.execute(s).await;
        let r = rconn.execute_batch(s);
        match (f, r) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => {
                diverged.push(format!(
                    "STMT_DIVERGE: {s}\n  frank: OK\n  csql:  ERROR({e})"
                ));
            }
            (Err(e), Ok(())) => {
                diverged.push(format!(
                    "STMT_DIVERGE: {s}\n  frank: ERROR({e})\n  csql:  OK"
                ));
            }
        }
    }
    diverged
}

/// DDL/DML that must succeed on both engines.
async fn apply(fconn: &Connection, rconn: &rusqlite::Connection, stmts: &[&str]) {
    for s in stmts {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn check_sees_affinity_coerced_value_on_insert() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        // TEXT column: INSERT 1 -> affinity coerces to '1' -> typeof='text' -> CHECK passes.
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (a TEXT CHECK(typeof(a) = 'text'))",
                "INSERT INTO t VALUES (1)",
                "INSERT INTO t VALUES (2.5)",
            ],
        )
        .await;
        let m = oracle_compare(&fconn, &rconn, &["SELECT typeof(a), a FROM t ORDER BY a"]).await;
        assert_no_mismatches(&m, "check_sees_affinity_coerced_value_on_insert");
    });
}

#[test]
fn check_rejects_after_affinity_coercion_on_insert() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &["CREATE TABLE u (b INTEGER CHECK(typeof(b) = 'text'))"],
        )
        .await;
        // INTEGER column: '5' -> affinity coerces to 5 -> typeof='integer' -> CHECK fails.
        // Both engines must REJECT (before the fix fsqlite wrongly accepted it).
        let diverged = apply_checked(
            &fconn,
            &rconn,
            &["INSERT INTO u VALUES ('5')", "INSERT INTO u VALUES ('99')"],
        )
        .await;
        assert_no_mismatches(&diverged, "check_rejects_after_affinity_coercion_on_insert");
        // The rejected rows left no trace on either engine.
        let m = oracle_compare(&fconn, &rconn, &["SELECT count(*) FROM u"]).await;
        assert_no_mismatches(&m, "check_rejects_after_affinity_coercion_on_insert_empty");
    });
}

#[test]
fn check_sees_affinity_coerced_value_on_update() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT CHECK(typeof(a) = 'text'))",
                "INSERT INTO t VALUES (1, 'x')",
                // UPDATE to a bare integer: affinity coerces 7 -> '7', CHECK passes.
                "UPDATE t SET a = 7 WHERE id = 1",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, typeof(a), a FROM t ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "check_sees_affinity_coerced_value_on_update");
    });
}
