//! bd-gh-row-value-comparisons-s2wd8 (GH #171): row-value BETWEEN.
//!
//! `(a,b) BETWEEN (x,y) AND (z,w)` must expand to lexicographic
//! `(a,b) >= (x,y) AND (a,b) <= (z,w)` (and NOT BETWEEN to `< OR >`), matching
//! C SQLite. The `>=`/`<=` row-value paths already worked; the BETWEEN rewrite
//! (rewrite_row_value_comparisons) was the gap. This keeper pins both the
//! table-WHERE and scalar forms against the rusqlite oracle.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

const SEED: &[&str] = &[
    "CREATE TABLE t(a INT, b INT)",
    "INSERT INTO t VALUES (1,1),(1,2),(1,3),(1,4),(2,1)",
];

// `SELECT a,b ...` queries -> ordered list of (a,b) pairs.
const PAIR_QUERIES: &[&str] = &[
    "SELECT a,b FROM t WHERE (a,b) BETWEEN (1,2) AND (1,3) ORDER BY a,b",
    "SELECT a,b FROM t WHERE (a,b) NOT BETWEEN (1,2) AND (1,3) ORDER BY a,b",
    // Equivalent explicit form must agree (both should already work).
    "SELECT a,b FROM t WHERE (a,b)>=(1,2) AND (a,b)<=(1,3) ORDER BY a,b",
    // Wider window + a 3-wide row value.
    "SELECT a,b FROM t WHERE (a,b) BETWEEN (1,1) AND (2,1) ORDER BY a,b",
];

// Scalar row-value BETWEEN -> single integer (0/1).
const SCALAR_QUERIES: &[&str] = &[
    "SELECT (1,2) BETWEEN (1,1) AND (1,3)",
    "SELECT (1,4) BETWEEN (1,1) AND (1,3)",
    "SELECT (2,0) NOT BETWEEN (1,1) AND (1,3)",
];

fn oracle_pairs(sql: &str) -> Vec<(i64, i64)> {
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

fn oracle_scalar(sql: &str) -> i64 {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

#[test]
fn row_value_between_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for s in SEED {
            conn.execute(s).await.expect("seed");
        }

        for sql in PAIR_QUERIES {
            let expected = oracle_pairs(sql);
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
                "row-value query `{sql}` diverged from the C SQLite oracle"
            );
        }

        for sql in SCALAR_QUERIES {
            let expected = oracle_scalar(sql);
            let rows = conn
                .query(sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
            let got = match rows[0].values()[0] {
                SqliteValue::Integer(n) => n,
                ref other => panic!("`{sql}` not int: {other:?}"),
            };
            assert_eq!(
                got, expected,
                "scalar row-value `{sql}` diverged from the C SQLite oracle"
            );
        }
    });
}
