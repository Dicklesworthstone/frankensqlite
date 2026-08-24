// Keeper for bd-nd2ju / GH#377 (L2): WITHOUT ROWID PRIMARY KEY point seeks for
// the two shapes the single-column bare-`pk = const` L1 hoist leaves as scans —
//   * a COMPOSITE full-key equality (`WHERE a = ? AND b = ?` on `PRIMARY KEY
//     (a, b)`), one `NoConflict` probe against the multi-field table key; and
//   * `pk = <const> AND <residual>` (single- or multi-column PK plus an extra
//     conjunct), the PK probe positions the cursor and the full WHERE is
//     re-applied per row so the residual stays enforced.
//
// Oracle: rusqlite bundled SQLite, which plans `SEARCH t USING PRIMARY KEY
// (a=? [AND b=?]...)` for all of these. A partial-key prefix is deliberately
// left as a scan (future scope) and is asserted NOT to claim a false SEARCH.
use fsqlite_core::connection::Connection;

const SETUP_COMPOSITE: &str = "CREATE TABLE ck(\
        a INTEGER NOT NULL, \
        b TEXT NOT NULL, \
        v INTEGER, \
        PRIMARY KEY(a, b)\
    ) WITHOUT ROWID";

const SETUP_SINGLE: &str = "CREATE TABLE sk(\
        id TEXT NOT NULL, \
        v INTEGER, \
        w INTEGER, \
        PRIMARY KEY(id)\
    ) WITHOUT ROWID";

fn rus_plan(setup: &str, sql: &str) -> String {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(setup).unwrap();
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| Ok(row.get_unwrap::<_, String>(n - 1)))
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .join(" | ")
}

async fn frank_plan(conn: &Connection, sql: &str) -> String {
    let rows = conn
        .query(&format!("EXPLAIN QUERY PLAN {sql}"))
        .await
        .unwrap();
    rows.iter()
        .map(|r| match &r.values()[3] {
            fsqlite_types::SqliteValue::Text(t) => t.to_string(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn is_search(detail: &str) -> bool {
    let up = detail.to_ascii_uppercase();
    up.contains("SEARCH") && !up.contains("SCAN")
}

#[test]
fn nd2ju_l2_composite_and_residual_plan_search_matching_oracle() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        c.execute(SETUP_COMPOSITE).await.unwrap();
        c.execute(SETUP_SINGLE).await.unwrap();

        let cases = [
            (SETUP_COMPOSITE, "SELECT v FROM ck WHERE a = 1 AND b = 'x'"),
            (SETUP_COMPOSITE, "SELECT v FROM ck WHERE b = 'x' AND a = 1"),
            (
                SETUP_COMPOSITE,
                "SELECT v FROM ck WHERE a = 1 AND b = 'x' AND v = 9",
            ),
            (
                SETUP_COMPOSITE,
                "SELECT a FROM ck AS e WHERE e.a = 1 AND e.b = 'x'",
            ),
            (SETUP_SINGLE, "SELECT w FROM sk WHERE id = 'x' AND v = 5"),
            (
                SETUP_SINGLE,
                "SELECT w FROM sk WHERE id = 'x' AND v = 5 LIMIT 1",
            ),
        ];
        for (setup, sql) in cases {
            let frank = frank_plan(&c, sql).await;
            let rus = rus_plan(setup, sql);
            assert!(
                is_search(&frank),
                "{sql}: frank must SEARCH, got {frank:?} (oracle {rus:?})"
            );
            assert_eq!(
                is_search(&frank),
                is_search(&rus),
                "{sql}: SEARCH/SCAN mismatch — frank {frank:?} vs oracle {rus:?}"
            );
            // The un-aliased shapes match the oracle EQP detail byte-for-byte.
            if !sql.contains(" AS ") {
                assert_eq!(frank, rus, "EQP detail must match oracle for {sql:?}");
            }
            assert!(
                frank.contains("USING PRIMARY KEY"),
                "{sql}: expected a PRIMARY KEY seek detail, got {frank:?}"
            );
        }
    });
}

#[test]
fn nd2ju_l2_partial_key_prefix_stays_scan_no_false_search() {
    // A partial-key prefix (`WHERE a = ?` on `PRIMARY KEY (a, b)`) is a b-tree
    // range, not a point probe — deliberately future scope. Assert frank does
    // NOT claim a (false) PRIMARY KEY point seek. Stock diverges here (it does a
    // prefix range SEARCH), so this is an honest known-gap guard, not a parity
    // claim.
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        c.execute(SETUP_COMPOSITE).await.unwrap();
        let frank = frank_plan(&c, "SELECT v FROM ck WHERE a = 1").await;
        assert!(
            !frank.contains("USING PRIMARY KEY"),
            "prefix-only probe must not claim a full-PK point seek, got {frank:?}"
        );
    });
}

#[test]
fn nd2ju_l2_composite_seek_returns_correct_rows() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        c.execute(SETUP_COMPOSITE).await.unwrap();
        let oracle = rusqlite::Connection::open_in_memory().unwrap();
        oracle.execute_batch(SETUP_COMPOSITE).unwrap();
        for (a, b, v) in [
            (1_i64, "x", 10_i64),
            (1, "y", 11),
            (2, "x", 20),
            (2, "y", 21),
        ] {
            let ins = format!("INSERT INTO ck(a, b, v) VALUES({a}, '{b}', {v})");
            c.execute(&ins).await.unwrap();
            oracle.execute(&ins, []).unwrap();
        }

        // Full-key present -> exactly one row; absent -> empty; composite +
        // residual narrows to the matching row (and rejects a non-matching v).
        let queries = [
            "SELECT v FROM ck WHERE a = 1 AND b = 'x'",
            "SELECT v FROM ck WHERE a = 2 AND b = 'y'",
            "SELECT v FROM ck WHERE a = 1 AND b = 'zzz'",
            "SELECT v FROM ck WHERE a = 9 AND b = 'x'",
            "SELECT v FROM ck WHERE a = 1 AND b = 'x' AND v = 10",
            "SELECT v FROM ck WHERE a = 1 AND b = 'x' AND v = 999",
            "SELECT v FROM ck WHERE b = 'y' AND a = 2",
        ];
        for sql in queries {
            let frank: Vec<i64> = c
                .query(sql)
                .await
                .unwrap()
                .iter()
                .map(|r| match &r.values()[0] {
                    fsqlite_types::SqliteValue::Integer(i) => *i,
                    other => panic!("unexpected value {other:?}"),
                })
                .collect();
            let rus: Vec<i64> = oracle
                .prepare(sql)
                .unwrap()
                .query_map([], |row| row.get::<_, i64>(0))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(frank, rus, "row parity mismatch for {sql:?}");
        }
    });
}

#[test]
fn nd2ju_l2_single_pk_residual_returns_correct_rows() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        c.execute(SETUP_SINGLE).await.unwrap();
        let oracle = rusqlite::Connection::open_in_memory().unwrap();
        oracle.execute_batch(SETUP_SINGLE).unwrap();
        for (id, v, w) in [("a", 5_i64, 100_i64), ("b", 6, 200), ("c", 5, 300)] {
            let ins = format!("INSERT INTO sk(id, v, w) VALUES('{id}', {v}, {w})");
            c.execute(&ins).await.unwrap();
            oracle.execute(&ins, []).unwrap();
        }
        let queries = [
            "SELECT w FROM sk WHERE id = 'a' AND v = 5",   // matches
            "SELECT w FROM sk WHERE id = 'a' AND v = 999", // residual rejects
            "SELECT w FROM sk WHERE id = 'b' AND v = 6",   // matches
            "SELECT w FROM sk WHERE id = 'zzz' AND v = 5", // absent PK
        ];
        for sql in queries {
            let frank: Vec<i64> = c
                .query(sql)
                .await
                .unwrap()
                .iter()
                .map(|r| match &r.values()[0] {
                    fsqlite_types::SqliteValue::Integer(i) => *i,
                    other => panic!("unexpected value {other:?}"),
                })
                .collect();
            let rus: Vec<i64> = oracle
                .prepare(sql)
                .unwrap()
                .query_map([], |row| row.get::<_, i64>(0))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(frank, rus, "row parity mismatch for {sql:?}");
        }
    });
}
