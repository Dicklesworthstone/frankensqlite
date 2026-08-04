//! Issue #117 regression: correlated `NOT EXISTS` / `EXISTS` in a WHERE clause
//! is routed to the connection-level interpreter (`execute_join_select`), which
//! evaluated the correlated subquery with a per-outer-row linear scan of the
//! child table (O(outer * child) — the ~480x anti-join cliff).
//!
//! The fix memoizes the child column's value-set once per query and answers
//! each outer row in O(1). These tests pin the *correctness* of that memo: the
//! value-set must answer membership with exactly the same semantics as the
//! per-row scan (`values_equal_sqlite`), including cross-type INTEGER/REAL
//! equality, NULL handling, and repeated outer rows. A coarse performance
//! sanity check guards against the cliff returning.
#![recursion_limit = "512"]

use std::time::Instant;

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

async fn ids(conn: &Connection, sql: &str) -> Vec<i64> {
    conn.query(sql)
        .await
        .unwrap()
        .iter()
        .map(|r| match &r.values()[0] {
            SqliteValue::Integer(i) => *i,
            other => panic!("expected integer id, got {other:?}"),
        })
        .collect()
}

#[test]
fn not_exists_anti_join_correct() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, k INTEGER, expires INTEGER)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE ledger (fk INTEGER)")
            .await
            .unwrap();

        // parent ids 1..=20.
        for id in 1..=20 {
            conn.execute(&format!(
                "INSERT INTO parent (id, k, expires) VALUES ({id}, {}, {})",
                id % 3,
                1000 + id
            ))
            .await
            .unwrap();
        }
        // ledger references a subset of parents (and some have multiple refs, and a
        // NULL fk that must never satisfy the correlated equality).
        let referenced = [2, 3, 3, 5, 8, 8, 8, 13, 21];
        for fk in referenced {
            conn.execute(&format!("INSERT INTO ledger (fk) VALUES ({fk})"))
                .await
                .unwrap();
        }
        conn.execute("INSERT INTO ledger (fk) VALUES (NULL)")
            .await
            .unwrap();

        let ref_set: std::collections::HashSet<i64> = referenced.into_iter().collect();

        // NOT EXISTS: parents with no ledger row.
        let got = ids(
            &conn,
            "SELECT id FROM parent \
             WHERE NOT EXISTS (SELECT 1 FROM ledger WHERE ledger.fk = parent.id) \
             ORDER BY id",
        )
        .await;
        let expected: Vec<i64> = (1..=20).filter(|id| !ref_set.contains(id)).collect();
        assert_eq!(got, expected, "NOT EXISTS anti-join produced wrong rows");

        // EXISTS (semi-join): parents that ARE referenced.
        let got_exists = ids(
            &conn,
            "SELECT id FROM parent \
             WHERE EXISTS (SELECT 1 FROM ledger WHERE ledger.fk = parent.id) \
             ORDER BY id",
        )
        .await;
        let expected_exists: Vec<i64> = (1..=20).filter(|id| ref_set.contains(id)).collect();
        assert_eq!(
            got_exists, expected_exists,
            "EXISTS semi-join produced wrong rows"
        );

        // Combined with extra outer predicates (the GH#117 shape): the memo must
        // not change which rows survive the additional filters.
        let got_combo = ids(
            &conn,
            "SELECT id FROM parent \
             WHERE parent.k = 1 \
               AND NOT EXISTS (SELECT 1 FROM ledger WHERE ledger.fk = parent.id) \
               AND parent.expires > 1010 \
             ORDER BY id",
        )
        .await;
        let expected_combo: Vec<i64> = (1..=20)
            .filter(|id| id % 3 == 1 && !ref_set.contains(id) && 1000 + id > 1010)
            .collect();
        assert_eq!(
            got_combo, expected_combo,
            "NOT EXISTS + outer filters wrong"
        );
    });
}

#[test]
fn not_exists_cross_type_numeric_equality() {
    asupersync::test_utils::run_test(|| async {
        // The correlated column is REAL but stores whole numbers; the outer column
        // is an INTEGER primary key. SQLite's `=` (and the engine's
        // `values_equal_sqlite`) treat 2 == 2.0, so the memo must match across the
        // INTEGER/REAL boundary in both directions.
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE ledger (fk REAL)").await.unwrap();
        for id in 1..=6 {
            conn.execute(&format!("INSERT INTO parent (id) VALUES ({id})"))
                .await
                .unwrap();
        }
        // REAL fk values equal (as numbers) to parent ids 2 and 4.
        conn.execute("INSERT INTO ledger (fk) VALUES (2.0)")
            .await
            .unwrap();
        conn.execute("INSERT INTO ledger (fk) VALUES (4.0)")
            .await
            .unwrap();

        let got = ids(
            &conn,
            "SELECT id FROM parent \
             WHERE NOT EXISTS (SELECT 1 FROM ledger WHERE ledger.fk = parent.id) \
             ORDER BY id",
        )
        .await;
        // 2 and 4 are referenced (cross-type), so NOT EXISTS keeps {1,3,5,6}.
        assert_eq!(got, vec![1, 3, 5, 6]);
    });
}

#[test]
fn not_exists_text_key_equality() {
    asupersync::test_utils::run_test(|| async {
        // Correlated equality on a TEXT key exercises the exact-equality branch of
        // the value-set (binary text equality, no NOCASE folding).
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE ban (name TEXT)").await.unwrap();
        let names = ["alice", "bob", "carol", "dave", "erin"];
        for (i, n) in names.iter().enumerate() {
            conn.execute(&format!(
                "INSERT INTO parent (id, name) VALUES ({}, '{n}')",
                i + 1
            ))
            .await
            .unwrap();
        }
        conn.execute("INSERT INTO ban (name) VALUES ('bob')")
            .await
            .unwrap();
        conn.execute("INSERT INTO ban (name) VALUES ('dave')")
            .await
            .unwrap();
        // Case mismatch must NOT match (binary equality).
        conn.execute("INSERT INTO ban (name) VALUES ('CAROL')")
            .await
            .unwrap();

        let got = ids(
            &conn,
            "SELECT id FROM parent p \
             WHERE NOT EXISTS (SELECT 1 FROM ban b WHERE b.name = p.name) \
             ORDER BY id",
        )
        .await;
        // bob(2) and dave(4) are banned; CAROL != carol so carol(3) stays.
        assert_eq!(got, vec![1, 3, 5]);
    });
}

#[test]
fn not_exists_perf_is_subquadratic() {
    asupersync::test_utils::run_test(|| async {
        // Build a large parent table and an indexed child, then run the anti-join
        // idiom. Before the fix this took multiple seconds (O(parent * child));
        // after the memo it is ~O(parent + child). A generous wall-clock ceiling
        // guards against the cliff returning without being flaky on shared CI.
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, k INTEGER)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE ledger (fk INTEGER)")
            .await
            .unwrap();

        let n = 6000;
        conn.execute("BEGIN").await.unwrap();
        for id in 1..=n {
            conn.execute(&format!(
                "INSERT INTO parent (id, k) VALUES ({id}, {})",
                id % 7
            ))
            .await
            .unwrap();
        }
        // Reference roughly half the parents.
        for id in (2..=n).step_by(2) {
            conn.execute(&format!("INSERT INTO ledger (fk) VALUES ({id})"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        let start = Instant::now();
        let rows = conn
            .query(
                "SELECT id FROM parent \
                 WHERE k = 1 \
                   AND NOT EXISTS (SELECT 1 FROM ledger WHERE ledger.fk = parent.id) \
                 ORDER BY id",
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        // Correctness: odd ids with k==1 and not referenced.
        let expected: Vec<i64> = (1..=n).filter(|id| id % 7 == 1 && id % 2 == 1).collect();
        let got: Vec<i64> = rows
            .iter()
            .map(|r| match &r.values()[0] {
                SqliteValue::Integer(i) => *i,
                other => panic!("expected integer, got {other:?}"),
            })
            .collect();
        assert_eq!(got, expected, "perf-shape anti-join produced wrong rows");

        // With O(parent*child) this 6000x3000 shape took seconds; the memo brings
        // it well under a second even on a contended host. 5s is a loose ceiling
        // that still catches a return of the quadratic cliff.
        assert!(
            elapsed.as_secs_f64() < 5.0,
            "correlated NOT EXISTS took {elapsed:?} — the O(n*m) cliff may have returned",
        );
    });
}
