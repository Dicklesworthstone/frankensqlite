//! GH #419: recursive CTE evaluation must not stop at a hidden round cap.
//!
//! Every recursion below needs more than 1000 rounds, which the engine used to
//! truncate silently (1001 rows, no error). Each answer is checked against the
//! bundled stock SQLite so the expectations are not hand-computed.
#![cfg(all(feature = "native", not(target_arch = "wasm32")))]
#![recursion_limit = "512"]

use fsqlite::{Connection, SqliteValue};

fn oracle_rows(sql: &str) -> Vec<Vec<SqliteValue>> {
    let oracle = rusqlite::Connection::open_in_memory().expect("stock in-memory oracle");
    let mut statement = oracle.prepare(sql).expect("oracle prepare");
    let column_count = statement.column_count();
    let rows = statement
        .query_map([], |row| {
            (0..column_count)
                .map(|index| {
                    Ok(match row.get_ref(index)? {
                        rusqlite::types::ValueRef::Null => SqliteValue::Null,
                        rusqlite::types::ValueRef::Integer(value) => SqliteValue::Integer(value),
                        rusqlite::types::ValueRef::Real(value) => SqliteValue::Float(value),
                        rusqlite::types::ValueRef::Text(bytes) => {
                            SqliteValue::Text(String::from_utf8_lossy(bytes).into_owned().into())
                        }
                        rusqlite::types::ValueRef::Blob(bytes) => SqliteValue::Blob(bytes.into()),
                    })
                })
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .expect("oracle query");
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .expect("oracle rows")
}

async fn assert_matches_oracle(connection: &Connection, sql: &str) {
    let expected = oracle_rows(sql);
    let actual: Vec<Vec<SqliteValue>> = connection
        .query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect();
    assert_eq!(actual, expected, "{sql}");
}

#[test]
fn recursion_deeper_than_a_thousand_rounds_is_complete() {
    asupersync::test_utils::run_test(|| async {
        let connection = Connection::open(":memory:").await.expect("open");
        let cases = [
            // General materialization path (mixed aggregates on the CTE).
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<5000) \
             SELECT count(*), sum(x), max(x) FROM c",
            // Direct integer-series sum fast path.
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1000000) \
             SELECT sum(x) FROM c",
            // Direct sum consumer over a two-column recursion.
            "WITH RECURSIVE c(x, y) AS (SELECT 1, 1 UNION ALL SELECT x+1, y*1 FROM c WHERE x<2500) \
             SELECT sum(y) FROM c",
            // UNION (deduplicating) recursion.
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION SELECT x+1 FROM c WHERE x<3000) \
             SELECT count(*), max(x) FROM c",
            // Plain scan with ORDER BY: no consumer budget applies, full table.
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<4000) \
             SELECT x FROM c ORDER BY x DESC LIMIT 3",
            // Consumer WHERE must not shrink the recursion.
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<4000) \
             SELECT x FROM c WHERE x > 3996 LIMIT 3",
            // Recursive arm that joins another CTE and filters with NOT EXISTS
            // (the reporter's Sudoku solver, GH #419).
            "WITH RECURSIVE \
               input(sud) AS (VALUES('53..7....6..195....98....6.8...6...34..8.3..17...2...6.6....28....419..5....8..79')), \
               digits(z, lp) AS (VALUES('1', 1) UNION ALL SELECT CAST(lp+1 AS TEXT), lp+1 FROM digits WHERE lp<9), \
               x(s, ind) AS ( \
                 SELECT sud, instr(sud, '.') FROM input \
                 UNION ALL \
                 SELECT substr(s, 1, ind-1) || z || substr(s, ind+1), \
                        instr(substr(s, 1, ind-1) || z || substr(s, ind+1), '.') \
                   FROM x, digits AS z \
                  WHERE ind>0 \
                    AND NOT EXISTS ( \
                          SELECT 1 FROM digits AS lp \
                           WHERE z.z = substr(s, ((ind-1)/9)*9 + lp, 1) \
                              OR z.z = substr(s, ((ind-1)%9) + (lp-1)*9 + 1, 1) \
                              OR z.z = substr(s, (((ind-1)/3) % 3) * 3 + ((ind-1)/27) * 27 + lp + ((lp-1) / 3) * 6, 1))) \
             SELECT s FROM x WHERE ind=0",
        ];
        for sql in cases {
            assert_matches_oracle(&connection, sql).await;
        }
    });
}

#[test]
fn unbounded_recursion_stops_at_the_consumer_limit() {
    asupersync::test_utils::run_test(|| async {
        let connection = Connection::open(":memory:").await.expect("open");
        // No terminating WHERE: only the consumer's LIMIT ends the recursion,
        // as it does in stock sqlite3's streaming evaluation.
        let cases = [
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) \
             SELECT x FROM c LIMIT 5 OFFSET 2995",
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) \
             SELECT x * 2 AS doubled, x FROM c LIMIT 3",
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) \
             SELECT * FROM c LIMIT 0",
            // GH #152: a LIMIT inside the CTE still caps it, past 1000 rows.
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 2500) \
             SELECT count(*), max(x) FROM c",
            // Both limits present: the smaller stopping point wins and the
            // consumer still sees exactly its own window.
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 1500) \
             SELECT x FROM c LIMIT 2 OFFSET 1498",
        ];
        for sql in cases {
            assert_matches_oracle(&connection, sql).await;
        }
    });
}
