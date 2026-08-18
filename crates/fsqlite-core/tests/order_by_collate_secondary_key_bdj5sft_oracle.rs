//! bd-j5sft: a multi-key ORDER BY must keep its secondary tie-breaker term
//! even when the primary key carries an explicit `COLLATE` and the query
//! projects an *expression* over a *derived table* (subquery source).
//!
//! Root cause: the interpreter's materialized ORDER BY path
//! (`execute_join_select`) appends a hidden sort column for any ORDER BY term
//! not present in the SELECT list. It used to append the term *including* its
//! `COLLATE` wrapper, so the hidden column's effective collation was reported
//! as the overriding collation (e.g. NOCASE). A sibling bare term
//! (`ORDER BY x COLLATE NOCASE, x`) dedups onto that same hidden column and
//! read the column's collation as its fallback — inheriting NOCASE and
//! silently dropping the BINARY tie-breaker.
//!
//! Fix: append the hidden sort column with the COLLATE wrapper stripped, so it
//! carries the column's *intrinsic* collation. A COLLATE now only affects the
//! ORDER BY term that literally spells it.
//!
//! Every expected value below is oracled live against rusqlite (C SQLite).

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

/// One case: schema+data seed statements and the query under test.
struct Case {
    name: &'static str,
    seed: &'static [&'static str],
    sql: &'static str,
}

const CASES: &[Case] = &[
    // ── The exact repro: expression output over a derived table, primary
    //    COLLATE NOCASE, bare secondary tie-breaker. Oracle: 'A','a','B','b'.
    Case {
        name: "repro/quote-derived-nocase-then-binary",
        seed: &["CREATE TABLE q(x TEXT)", "INSERT INTO q VALUES('B'),('a'),('A'),('b')"],
        sql: "SELECT quote(x) FROM (SELECT x FROM q) ORDER BY x COLLATE NOCASE, x",
    },
    // ── Concat expression output variant (x || '!').
    Case {
        name: "concat-output-variant",
        seed: &["CREATE TABLE q(x TEXT)", "INSERT INTO q VALUES('B'),('a'),('A'),('b')"],
        sql: "SELECT x||'!' FROM (SELECT x FROM q) ORDER BY x COLLATE NOCASE, x",
    },
    // ── DESC on the (binary) secondary tie-breaker.
    Case {
        name: "desc-secondary",
        seed: &["CREATE TABLE q(x TEXT)", "INSERT INTO q VALUES('B'),('a'),('A'),('b')"],
        sql: "SELECT quote(x) FROM (SELECT x FROM q) ORDER BY x COLLATE NOCASE, x DESC",
    },
    // ── Three keys: COLLATE NOCASE primary, then two BINARY tie-breakers.
    Case {
        name: "three-keys-nocase-then-two-binary",
        seed: &[
            "CREATE TABLE q3(x TEXT, y INT, z INT)",
            "INSERT INTO q3 VALUES('B',2,9),('a',1,8),('A',1,7),('b',2,6),('A',1,9)",
        ],
        sql: "SELECT quote(x)||'/'||y||'/'||z FROM (SELECT x,y,z FROM q3) \
              ORDER BY x COLLATE NOCASE, y, z",
    },
    // ── Control: SAME query over the BASE table (no derived source). This
    //    uses the VDBE sorter and was already correct; it must stay correct.
    Case {
        name: "control/base-table-stays-correct",
        seed: &["CREATE TABLE q(x TEXT)", "INSERT INTO q VALUES('B'),('a'),('A'),('b')"],
        sql: "SELECT quote(x) FROM q ORDER BY x COLLATE NOCASE, x",
    },
    // ── Control: bare-column output over the derived table (no expression).
    Case {
        name: "control/bare-column-output-over-derived",
        seed: &["CREATE TABLE q(x TEXT)", "INSERT INTO q VALUES('B'),('a'),('A'),('b')"],
        sql: "SELECT x FROM (SELECT x FROM q) ORDER BY x COLLATE NOCASE, x",
    },
    // ── Control: NOCASE primary with a NOCASE secondary (both same collation,
    //    on distinct columns) — the secondary must still order.
    Case {
        name: "control/nocase-primary-nocase-secondary",
        seed: &[
            "CREATE TABLE q2(x TEXT, y TEXT)",
            "INSERT INTO q2 VALUES('B','q'),('a','P'),('A','p'),('b','Q')",
        ],
        sql: "SELECT quote(x)||'/'||quote(y) FROM (SELECT x,y FROM q2) \
              ORDER BY x COLLATE NOCASE, y COLLATE NOCASE",
    },
];

fn oracle(seed: &[&str], sql: &str) -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in seed {
        conn.execute(s, []).unwrap();
    }
    conn.prepare(sql)
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn order_by_collate_secondary_key_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        for case in CASES {
            let expected = oracle(case.seed, case.sql);

            let conn = Connection::open(":memory:").await.unwrap();
            for s in case.seed {
                conn.execute(s).await.unwrap_or_else(|e| panic!("[{}] seed `{s}`: {e:?}", case.name));
            }
            let rows = conn
                .query(case.sql)
                .await
                .unwrap_or_else(|e| panic!("[{}] `{}`: {e:?}", case.name, case.sql));
            let got: Vec<String> = rows
                .iter()
                .map(|r| match r.values()[0] {
                    SqliteValue::Text(ref s) => s.as_ref().to_owned(),
                    ref other => panic!("[{}] not text: {other:?}", case.name),
                })
                .collect();

            assert_eq!(
                got, expected,
                "[{}] `{}` diverged from the C SQLite oracle",
                case.name, case.sql
            );
        }
    });
}
