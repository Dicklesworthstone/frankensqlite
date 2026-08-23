//! bd-aap9u — Oracle-parity e2e: UPSERT with MULTIPLE ON CONFLICT clauses.
//!
//! SQLite 3.35.0+ allows a chain of `ON CONFLICT` clauses on a single INSERT,
//! each targeting a different UNIQUE/PK constraint. When the candidate row
//! violates a constraint, the clauses are examined **in written order** and the
//! first clause whose conflict target is violated is applied (its `DO UPDATE`
//! rewrites the specific conflicting row, or `DO NOTHING` silently skips the
//! insert). A conflict on a constraint that no clause targets raises the default
//! constraint error. A final targetless `ON CONFLICT DO ...` matches any
//! conflict. Every scenario asserts per-statement agreement with rusqlite, then
//! compares resulting table state.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

/// Run `stmts` against both engines, asserting each statement's OK/ERROR outcome
/// agrees, then compare `queries` results row-for-row.
async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

const T: &str = "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER UNIQUE, v)";

/// Bead example: the candidate conflicts on the SECOND target (b); the second
/// clause must apply -> (1,2,200).
#[test]
fn multi_conflict_on_second_target() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                T,
                "INSERT INTO t VALUES(1,2,10)",
                "INSERT INTO t VALUES(9,2,99) ON CONFLICT(a) DO UPDATE SET v=100 ON CONFLICT(b) DO UPDATE SET v=200",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,2,200)
            "multi_conflict_on_second_target",
        )
        .await;
    });
}

/// Conflict on the FIRST target (a) -> first clause applies -> (1,2,100).
#[test]
fn multi_conflict_on_first_target() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                T,
                "INSERT INTO t VALUES(1,2,10)",
                "INSERT INTO t VALUES(1,7,99) ON CONFLICT(a) DO UPDATE SET v=100 ON CONFLICT(b) DO UPDATE SET v=200",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,2,100)
            "multi_conflict_on_first_target",
        )
        .await;
    });
}

/// Candidate conflicts on BOTH targets (same existing row): the FIRST clause in
/// written order wins. Two orderings pin that clause order (not constraint
/// order) decides.
#[test]
fn multi_conflict_both_targets_clause_order_wins() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                T,
                "INSERT INTO t VALUES(1,2,10)",
                "INSERT INTO t VALUES(1,2,99) ON CONFLICT(a) DO UPDATE SET v=100 ON CONFLICT(b) DO UPDATE SET v=200",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,2,100) — clause a wins
            "multi_conflict_both_a_then_b",
        )
        .await;
        scenario(
            &[
                T,
                "INSERT INTO t VALUES(1,2,10)",
                "INSERT INTO t VALUES(1,2,99) ON CONFLICT(b) DO UPDATE SET v=200 ON CONFLICT(a) DO UPDATE SET v=100",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,2,200) — clause b wins
            "multi_conflict_both_b_then_a",
        )
        .await;
    });
}

/// The two targets match DIFFERENT existing rows; the first clause whose target
/// conflicts wins and rewrites THAT row.
#[test]
fn multi_conflict_targets_different_rows() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                T,
                "INSERT INTO t VALUES(1,10,100)",
                "INSERT INTO t VALUES(2,20,200)",
                "INSERT INTO t VALUES(1,20,999) ON CONFLICT(a) DO UPDATE SET v=1111 ON CONFLICT(b) DO UPDATE SET v=2222",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,10,1111),(2,20,200)
            "multi_conflict_targets_different_rows",
        )
        .await;
    });
}

/// A DO NOTHING clause in the chain: on a matching-target conflict it skips the
/// insert without error, leaving the row untouched.
#[test]
fn multi_conflict_do_nothing_clause() {
    asupersync::test_utils::run_test(|| async {
        // 2nd target DO NOTHING, conflict on b -> row unchanged.
        scenario(
            &[
                T,
                "INSERT INTO t VALUES(1,2,10)",
                "INSERT INTO t VALUES(9,2,99) ON CONFLICT(a) DO UPDATE SET v=100 ON CONFLICT(b) DO NOTHING",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,2,10)
            "multi_conflict_2nd_do_nothing",
        )
        .await;
        // 1st target DO NOTHING, conflict on a -> row unchanged.
        scenario(
            &[
                T,
                "INSERT INTO t VALUES(1,2,10)",
                "INSERT INTO t VALUES(1,7,99) ON CONFLICT(a) DO NOTHING ON CONFLICT(b) DO UPDATE SET v=200",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,2,10)
            "multi_conflict_1st_do_nothing",
        )
        .await;
    });
}

/// A trailing targetless `ON CONFLICT DO NOTHING` is a catch-all: a conflict on
/// any constraint not matched by an earlier clause is silently skipped.
#[test]
fn multi_conflict_trailing_bare_do_nothing() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER UNIQUE, c INTEGER UNIQUE, v)",
                "INSERT INTO t VALUES(1,2,3,10)",
                "INSERT INTO t VALUES(9,8,3,99) ON CONFLICT(a) DO UPDATE SET v=100 ON CONFLICT DO NOTHING",
            ],
            &["SELECT a,b,c,v FROM t ORDER BY a"], // (1,2,3,10) — c conflict swallowed
            "multi_conflict_trailing_bare_do_nothing",
        )
        .await;
    });
}

/// No conflict at all -> normal insert alongside the ON CONFLICT chain.
#[test]
fn multi_conflict_no_conflict_inserts() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                T,
                "INSERT INTO t VALUES(1,2,10)",
                "INSERT INTO t VALUES(9,8,99) ON CONFLICT(a) DO UPDATE SET v=100 ON CONFLICT(b) DO UPDATE SET v=200",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,2,10),(9,8,99)
            "multi_conflict_no_conflict_inserts",
        )
        .await;
    });
}

/// A conflict on a constraint that NO clause targets raises the default
/// constraint error (both engines error, table unchanged).
#[test]
fn multi_conflict_untargeted_constraint_errors() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER UNIQUE, c INTEGER UNIQUE, v)",
                "INSERT INTO t VALUES(1,2,3,10)",
                // c=3 conflicts but no clause targets c -> error.
                "INSERT INTO t VALUES(9,8,3,99) ON CONFLICT(a) DO UPDATE SET v=100 ON CONFLICT(b) DO UPDATE SET v=200",
            ],
            &["SELECT a,b,c,v FROM t ORDER BY a"], // (1,2,3,10) — insert rejected
            "multi_conflict_untargeted_constraint_errors",
        )
        .await;
    });
}

/// `excluded.*` and existing-column references resolve independently in each
/// clause's DO UPDATE body.
#[test]
fn multi_conflict_excluded_and_existing_refs() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                T,
                "INSERT INTO t VALUES(1,2,10)",
                // conflict on b -> 2nd clause: v = excluded.v + v = 99 + 10 = 109.
                "INSERT INTO t VALUES(9,2,99) ON CONFLICT(a) DO UPDATE SET v=excluded.v ON CONFLICT(b) DO UPDATE SET v=excluded.v+v",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,2,109)
            "multi_conflict_excluded_and_existing_refs",
        )
        .await;
    });
}

/// Regression for the single-clause target-specificity corollary: an explicit
/// `ON CONFLICT(a) DO NOTHING` must NOT swallow a conflict on a different
/// constraint (b); stock raises the constraint error.
#[test]
fn single_do_nothing_nontarget_conflict_errors() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER UNIQUE, c)",
                "INSERT INTO t VALUES(1,2,3)",
                // b=2 conflicts; target is a -> not swallowed -> error.
                "INSERT INTO t VALUES(9,2,99) ON CONFLICT(a) DO NOTHING",
            ],
            &["SELECT a,b,c FROM t ORDER BY a"], // (1,2,3)
            "single_do_nothing_nontarget_conflict_errors",
        )
        .await;
    });
}

/// Candidate NOT NULL / CHECK constraints are enforced (ABORT) for an upsert
/// row — even when the row would conflict and DO UPDATE — matching stock. Guards
/// against blanket-IGNORE of candidate constraints on the DO UPDATE path.
#[test]
fn upsert_candidate_check_and_notnull_enforced() {
    asupersync::test_utils::run_test(|| async {
        // No-conflict candidate violating CHECK -> error.
        scenario(
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b UNIQUE, v CHECK(v>0))",
                "INSERT INTO t VALUES(1,2,10)",
                "INSERT INTO t VALUES(9,8,-5) ON CONFLICT(a) DO UPDATE SET v=100",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,2,10) — rejected
            "upsert_nonconflict_check_enforced",
        )
        .await;
        // Conflicting DO UPDATE where the CANDIDATE violates CHECK (the SET would
        // be valid) -> stock still errors on the candidate constraint.
        scenario(
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b UNIQUE, v CHECK(v>0))",
                "INSERT INTO t VALUES(1,2,10)",
                "INSERT INTO t VALUES(1,8,-5) ON CONFLICT(a) DO UPDATE SET v=100",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,2,10) — rejected
            "upsert_conflicting_candidate_check_enforced",
        )
        .await;
        // Conflicting DO UPDATE where the CANDIDATE violates NOT NULL (SET fixes
        // it) -> stock still errors on the candidate constraint.
        scenario(
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b NOT NULL)",
                "INSERT INTO t VALUES(1,10)",
                "INSERT INTO t VALUES(1,NULL) ON CONFLICT(a) DO UPDATE SET b=99",
            ],
            &["SELECT a,b FROM t ORDER BY a"], // (1,10) — rejected
            "upsert_conflicting_candidate_notnull_enforced",
        )
        .await;
        // A DO NOTHING clause does NOT suppress a candidate CHECK violation on a
        // conflicting row.
        scenario(
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b UNIQUE, v CHECK(v>0))",
                "INSERT INTO t VALUES(1,2,10)",
                "INSERT INTO t VALUES(1,8,-5) ON CONFLICT(a) DO NOTHING",
            ],
            &["SELECT a,b,v FROM t ORDER BY a"], // (1,2,10) — CHECK error
            "upsert_do_nothing_candidate_check_enforced",
        )
        .await;
    });
}

/// `INSERT OR IGNORE ... ON CONFLICT(a) DO UPDATE`: a conflict on a constraint
/// no clause targets is IGNORED (not ABORT), because the statement-level
/// algorithm propagates to the non-target constraints.
#[test]
fn upsert_or_ignore_untargeted_conflict_ignored() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER UNIQUE, c INTEGER UNIQUE, v)",
                "INSERT INTO t VALUES(1,2,3,10)",
                // c=3 conflicts (untargeted) but OR IGNORE -> row silently skipped.
                "INSERT OR IGNORE INTO t VALUES(9,8,3,99) ON CONFLICT(a) DO UPDATE SET v=100",
            ],
            &["SELECT a,b,c,v FROM t ORDER BY a"], // (1,2,3,10)
            "upsert_or_ignore_untargeted_conflict_ignored",
        )
        .await;
    });
}
