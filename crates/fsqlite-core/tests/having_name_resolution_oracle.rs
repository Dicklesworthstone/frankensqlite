//! bd-gh-having-name-resolution-v912w (GH #174): HAVING must resolve an
//! unqualified name to a base (input) column before a SELECT output alias that
//! shadows it, matching C SQLite.
//!
//! `SELECT x AS s, SUM(s) FROM t GROUP BY s HAVING s>1` — the HAVING `s` binds
//! to the base column `s`, not the `x AS s` alias. This keeper pins the
//! shadowing case plus qualified / non-shadowing / aggregate-alias controls
//! against the rusqlite oracle.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

const SEED: &[&str] = &[
    "CREATE TABLE t(s INTEGER, x INTEGER)",
    "INSERT INTO t VALUES (1,10),(1,20),(2,30),(2,40)",
];

const QUERIES: &[&str] = &[
    // The bug: HAVING s binds base column s (kept: s=2 group) -> one row 30|4.
    "SELECT x AS s, SUM(s) FROM t GROUP BY s HAVING s>1 ORDER BY 1",
    // Qualified reference — unambiguously the base column.
    "SELECT x AS s, SUM(s) FROM t GROUP BY s HAVING t.s>1 ORDER BY 1",
    // Non-shadowing alias (s2) — HAVING s is unambiguously the base column.
    "SELECT x AS s2, SUM(s) FROM t GROUP BY s HAVING s>1 ORDER BY 1",
    // Aggregate output alias with no shadowing base column — alias resolution
    // must STILL work (regression guard for the reordering).
    "SELECT s, SUM(x) AS total FROM t GROUP BY s HAVING total>30 ORDER BY s",
    // HAVING on the plain grouping base column.
    "SELECT s, SUM(x) FROM t GROUP BY s HAVING s=1 ORDER BY s",
];

fn oracle(sql: &str) -> Vec<(i64, i64)> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in SEED {
        conn.execute(s, []).unwrap();
    }
    conn.prepare(sql)
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn having_name_resolution_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for s in SEED {
            conn.execute(s).await.expect("seed");
        }
        for sql in QUERIES {
            let expected = oracle(sql);
            let rows = conn
                .query(sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
            let got: Vec<(i64, i64)> = rows
                .iter()
                .map(|r| {
                    let int = |i: usize| match r.values()[i] {
                        SqliteValue::Integer(n) => n,
                        ref other => panic!("`{sql}` col {i} not int: {other:?}"),
                    };
                    (int(0), int(1))
                })
                .collect();
            assert_eq!(
                got, expected,
                "HAVING `{sql}` diverged from the C SQLite oracle"
            );
        }
    });
}
