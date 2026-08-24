// Keeper for bd-nd2ju / GH#377: WITHOUT ROWID single-column PRIMARY KEY
// equality seeks the table b-tree (SEARCH ... USING PRIMARY KEY) instead of
// full-scanning every row. Oracle: rusqlite bundled SQLite.
//
// Before this fix a WITHOUT ROWID table's PK was never a `table.indexes`
// candidate, so the planner emitted a FullTableScan and every point lookup read
// every row (O(n); quadratic under the GH#377 bulk-ingest trigger guard). The
// codegen now hoists a direct table-b-tree `NoConflict` seek before the
// directive, and EXPLAIN QUERY PLAN reports the program-verified PK seek.
use fsqlite_core::connection::Connection;

const SETUP_WOR: &str = "CREATE TABLE facts(\
        id TEXT NOT NULL, \
        capture_id TEXT NOT NULL, \
        natural_key TEXT NOT NULL, \
        v INTEGER, \
        PRIMARY KEY(id), \
        UNIQUE(capture_id, natural_key)\
    ) WITHOUT ROWID";

fn rus_plan(sql: &str) -> String {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(SETUP_WOR).unwrap();
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
fn nd2ju_wr_pk_equality_plans_search_matching_oracle() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        c.execute(SETUP_WOR).await.unwrap();

        // Access-path parity vs the oracle for every point-lookup shape.
        let cases = [
            ("pk-eq", "SELECT v FROM facts WHERE id = 'x'"),
            ("pk-eq-limit", "SELECT v FROM facts WHERE id = 'x' LIMIT 1"),
            ("pk-eq-alias", "SELECT 1 FROM facts AS e WHERE e.id = 'x'"),
            // dd210b763 regression guard: the composite UNIQUE autoindex still
            // seeks (this bead must not disturb the landed WR-UNIQUE path).
            (
                "uniq-eq",
                "SELECT v FROM facts WHERE capture_id = 'c' AND natural_key = 'n'",
            ),
        ];
        for (label, sql) in cases {
            let frank = frank_plan(&c, sql).await;
            let rus = rus_plan(sql);
            assert!(
                is_search(&frank),
                "{label}: frank must SEARCH, got {frank:?} (oracle {rus:?})"
            );
            assert_eq!(
                is_search(&frank),
                is_search(&rus),
                "{label}: SEARCH/SCAN mismatch — frank {frank:?} vs oracle {rus:?}"
            );
        }

        // Exact EQP-detail parity for the un-aliased single-column PK lookups
        // (stock: `SEARCH facts USING PRIMARY KEY (id=?)`).
        for sql in [
            "SELECT v FROM facts WHERE id = 'x'",
            "SELECT v FROM facts WHERE id = 'x' LIMIT 1",
        ] {
            let frank = frank_plan(&c, sql).await;
            let rus = rus_plan(sql);
            assert_eq!(frank, rus, "EQP detail must match oracle for {sql:?}");
            assert!(
                frank.contains("USING PRIMARY KEY"),
                "expected a PRIMARY KEY seek detail, got {frank:?}"
            );
        }
    });
}

#[test]
fn nd2ju_wr_pk_seek_returns_correct_rows() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        c.execute(SETUP_WOR).await.unwrap();
        for (id, cap, nat, v) in [
            ("a", "c1", "n1", 10),
            ("b", "c2", "n2", 20),
            ("c", "c3", "n3", 30),
        ] {
            c.execute(&format!(
                "INSERT INTO facts(id, capture_id, natural_key, v) VALUES('{id}','{cap}','{nat}',{v})"
            ))
            .await
            .unwrap();
        }

        // Present key returns exactly its row.
        let rows = c
            .query("SELECT v FROM facts WHERE id = 'b'")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "expected one row for present PK");
        assert_eq!(rows[0].values()[0], fsqlite_types::SqliteValue::Integer(20));

        // Absent key returns nothing (the seek's NoConflict miss path).
        let rows = c
            .query("SELECT v FROM facts WHERE id = 'zzz'")
            .await
            .unwrap();
        assert!(rows.is_empty(), "expected no rows for absent PK, got {rows:?}");

        // LIMIT is honored on the single-row seek.
        let rows = c
            .query("SELECT v FROM facts WHERE id = 'a' LIMIT 1")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], fsqlite_types::SqliteValue::Integer(10));

        // Parity spot-check against the oracle across every PK value plus a miss.
        let oracle = rusqlite::Connection::open_in_memory().unwrap();
        oracle.execute_batch(SETUP_WOR).unwrap();
        for (id, cap, nat, v) in [
            ("a", "c1", "n1", 10),
            ("b", "c2", "n2", 20),
            ("c", "c3", "n3", 30),
        ] {
            oracle
                .execute(
                    "INSERT INTO facts(id, capture_id, natural_key, v) VALUES(?1,?2,?3,?4)",
                    rusqlite::params![id, cap, nat, v],
                )
                .unwrap();
        }
        for key in ["a", "b", "c", "missing"] {
            let sql = format!("SELECT v FROM facts WHERE id = '{key}'");
            let frank: Vec<i64> = c
                .query(&sql)
                .await
                .unwrap()
                .iter()
                .map(|r| match &r.values()[0] {
                    fsqlite_types::SqliteValue::Integer(i) => *i,
                    other => panic!("unexpected value {other:?}"),
                })
                .collect();
            let rus: Vec<i64> = oracle
                .prepare(&sql)
                .unwrap()
                .query_map([], |row| row.get::<_, i64>(0))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(frank, rus, "row parity mismatch for id={key:?}");
        }
    });
}
