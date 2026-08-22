//! bd-gh-row-value-is-distinct-8glg9 (GH #243): row-value IS DISTINCT FROM / IS
//! NOT DISTINCT FROM with NULL components.
//!
//! `(a,b) IS DISTINCT FROM (c,d)` must expand NULL-safely to componentwise
//! `a IS NOT c OR b IS NOT d` (and IS NOT DISTINCT FROM to `a IS c AND b IS d`),
//! matching C SQLite. The row-value IS/IS-NOT family once returned a constant
//! (0 for every row). This keeper pins the projection and WHERE forms against
//! the rusqlite oracle, including NULL components.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

const SEED: &[&str] = &[
    "CREATE TABLE t(a, b)",
    "INSERT INTO t VALUES (1,NULL),(1,2),(NULL,NULL),(2,NULL),(NULL,2),(1,1)",
];

// `SELECT rowid, <bool-expr> FROM t ORDER BY rowid` -> ordered (rowid, 0/1).
const PROJECTIONS: &[&str] = &[
    "SELECT rowid, ((a,b) IS DISTINCT FROM (1,NULL)) FROM t ORDER BY rowid",
    "SELECT rowid, ((a,b) IS NOT DISTINCT FROM (1,NULL)) FROM t ORDER BY rowid",
    "SELECT rowid, ((a,b) IS DISTINCT FROM (1,2)) FROM t ORDER BY rowid",
    "SELECT rowid, ((a,b) IS NOT DISTINCT FROM (NULL,NULL)) FROM t ORDER BY rowid",
];

// WHERE forms -> ordered rowids.
const FILTERS: &[&str] = &[
    "SELECT rowid FROM t WHERE (a,b) IS DISTINCT FROM (1,NULL) ORDER BY rowid",
    "SELECT rowid FROM t WHERE (a,b) IS NOT DISTINCT FROM (1,NULL) ORDER BY rowid",
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

fn oracle_ids(sql: &str) -> Vec<i64> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in SEED {
        conn.execute(s, []).unwrap();
    }
    conn.prepare(sql)
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn row_value_is_distinct_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for s in SEED {
            conn.execute(s).await.expect("seed");
        }

        let int = |v: &SqliteValue, ctx: &str| -> i64 {
            match v {
                SqliteValue::Integer(n) => *n,
                other => panic!("{ctx}: expected integer, got {other:?}"),
            }
        };

        for sql in PROJECTIONS {
            let expected = oracle_pairs(sql);
            let rows = conn
                .query(sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
            let got: Vec<(i64, i64)> = rows
                .iter()
                .map(|r| (int(&r.values()[0], sql), int(&r.values()[1], sql)))
                .collect();
            assert_eq!(
                got, expected,
                "row-value IS-family `{sql}` diverged from the C SQLite oracle"
            );
        }

        for sql in FILTERS {
            let expected = oracle_ids(sql);
            let rows = conn
                .query(sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
            let got: Vec<i64> = rows.iter().map(|r| int(&r.values()[0], sql)).collect();
            assert_eq!(
                got, expected,
                "row-value IS-family filter `{sql}` diverged from the C SQLite oracle"
            );
        }
    });
}
