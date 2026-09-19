//! Generic SQL regression for HFDT's repeated capture-consistency EXISTS.
//! These are engine-only inputs, not financial data or provider proof.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

const PROBE: &str = "SELECT 1 FROM items WHERE owner = 'group' AND category <> 'middle' LIMIT 1";

struct ProfileGuard;

impl Drop for ProfileGuard {
    fn drop(&mut self) {
        set_hot_path_profile_enabled(false);
    }
}

async fn execute_both(conn: &Connection, oracle: &rusqlite::Connection, sql: &str) {
    oracle.execute_batch(sql).expect("stock SQLite execution");
    conn.execute_batch(sql)
        .await
        .expect("FrankenSQLite execution");
}

async fn compare(conn: &Connection, oracle: &rusqlite::Connection, sql: &str, expected: usize) {
    let mut statement = oracle.prepare(sql).expect("stock prepare");
    let stock = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .expect("stock query")
        .collect::<Result<Vec<_>, _>>()
        .expect("stock rows");
    let rows = conn.query(sql).await.expect("engine query");
    let actual = rows
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    let expected_rows = stock
        .into_iter()
        .map(|value| vec![SqliteValue::Integer(value)])
        .collect::<Vec<_>>();
    assert_eq!(actual, expected_rows, "{sql}");
    assert_eq!(actual.len(), expected, "{sql}");
}

// One test owns the process-global profiling switch; no concurrent test can
// contaminate the dynamic instruction counts below.
#[test]
fn index_extrema_exclusion_preserves_sqlite_results_and_bounds_empty_probe() {
    asupersync::test_utils::run_test(|| async {
        for suffix in ["", " WITHOUT ROWID"] {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("index-exclusion.db");
            let conn = Connection::open(path.to_str().expect("path"))
                .await
                .expect("open");
            let oracle = rusqlite::Connection::open_in_memory().expect("oracle");
            execute_both(
                &conn,
                &oracle,
                &format!(
                    "CREATE TABLE items(owner TEXT NOT NULL, ordinal INTEGER NOT NULL,
                 category TEXT, PRIMARY KEY(owner, ordinal)){suffix};
                 CREATE INDEX by_category ON items(category, ordinal, owner);"
                ),
            )
            .await;
            compare(&conn, &oracle, PROBE, 0).await;
            let values = (0..512)
                .map(|i| format!("('group',{i},'middle')"))
                .collect::<Vec<_>>()
                .join(",");
            execute_both(
                &conn,
                &oracle,
                &format!("INSERT INTO items VALUES {values};"),
            )
            .await;
            compare(&conn, &oracle, PROBE, 0).await;

            set_hot_path_profile_enabled(true);
            let guard = ProfileGuard;
            let before = hot_path_profile_snapshot().vdbe.opcodes_executed_total;
            compare(&conn, &oracle, PROBE, 0).await;
            let bounded = hot_path_profile_snapshot().vdbe.opcodes_executed_total - before;
            // Negative performance control: the same rows/predicate with an
            // explicit full scan must do more work, not report a zero-run green.
            let before = hot_path_profile_snapshot().vdbe.opcodes_executed_total;
            compare(
                &conn,
                &oracle,
                "SELECT 1 FROM items NOT INDEXED
                 WHERE owner = 'group' AND category <> 'middle' LIMIT 1",
                0,
            )
            .await;
            let scanned = hot_path_profile_snapshot().vdbe.opcodes_executed_total - before;
            drop(guard);
            eprintln!(
                "index_exclusion suffix={suffix:?} rows=512 bounded_ops={bounded} scan_ops={scanned}"
            );
            assert!(
                (1..256).contains(&bounded),
                "unbounded empty probe: {bounded}"
            );
            assert!(
                scanned > 512 && scanned > bounded,
                "scan control did not execute: {scanned}"
            );

            // Checking only the minimum would incorrectly suppress this row.
            execute_both(
                &conn,
                &oracle,
                "INSERT INTO items VALUES ('group',512,'z');",
            )
            .await;
            compare(&conn, &oracle, PROBE, 1).await;
            execute_both(
                &conn,
                &oracle,
                "DELETE FROM items WHERE ordinal=512;
                INSERT INTO items VALUES ('group',512,'a');",
            )
            .await;
            compare(&conn, &oracle, PROBE, 1).await;
            // A differing extremum for another owner must retain the residual.
            execute_both(
                &conn,
                &oracle,
                "DELETE FROM items WHERE ordinal=512;
                INSERT INTO items VALUES ('other',512,'a');",
            )
            .await;
            compare(&conn, &oracle, PROBE, 0).await;
            execute_both(
                &conn,
                &oracle,
                "INSERT INTO items VALUES ('group',513,NULL);",
            )
            .await;
            compare(&conn, &oracle, PROBE, 0).await;
            compare(
                &conn,
                &oracle,
                "SELECT 1 FROM items WHERE owner='group' OR category<>'middle' LIMIT 1",
                1,
            )
            .await;
            compare(
                &conn,
                &oracle,
                "SELECT 1 FROM items WHERE owner='group' AND category<>NULL LIMIT 1",
                0,
            )
            .await;
            compare(
                &conn,
                &oracle,
                "SELECT 1 FROM items WHERE owner='group' AND 'middle'<>category LIMIT 1",
                0,
            )
            .await;

            // Partial indexes do not represent all rows. This one hides the
            // qualifying row and must never establish whole-table absence.
            execute_both(
                &conn,
                &oracle,
                "DROP INDEX by_category;
                CREATE INDEX partial_category ON items(category) WHERE category='middle';
                INSERT INTO items VALUES ('group',514,'z');",
            )
            .await;
            compare(&conn, &oracle, PROBE, 1).await;
            execute_both(
                &conn,
                &oracle,
                "DROP INDEX partial_category;
                CREATE INDEX descending_category ON items(category DESC);",
            )
            .await;
            compare(&conn, &oracle, PROBE, 1).await;
            execute_both(
                &conn,
                &oracle,
                "DROP INDEX descending_category;
                CREATE INDEX folded_category ON items(category COLLATE NOCASE);",
            )
            .await;
            compare(&conn, &oracle, PROBE, 1).await;

            execute_both(
                &conn,
                &oracle,
                &format!(
                    "CREATE TABLE guarded(owner TEXT NOT NULL, ordinal INTEGER NOT NULL,
                 category TEXT NOT NULL, PRIMARY KEY(owner,ordinal)){suffix};
                 CREATE INDEX guarded_category ON guarded(category,ordinal,owner);
                 CREATE TRIGGER guard_before BEFORE INSERT ON guarded
                 WHEN EXISTS(SELECT 1 FROM guarded AS old
                     WHERE old.owner=NEW.owner AND old.category<>NEW.category)
                 BEGIN SELECT RAISE(ABORT,'mixed category'); END;
                 CREATE TRIGGER guard_after AFTER INSERT ON guarded
                 WHEN EXISTS(SELECT 1 FROM guarded AS old
                     WHERE old.owner=NEW.owner AND old.category<>NEW.category)
                 BEGIN SELECT RAISE(ABORT,'mixed category'); END;
                 INSERT INTO guarded VALUES ('group',0,'middle'),('group',1,'middle');"
                ),
            )
            .await;
            for bad in [
                "INSERT INTO guarded VALUES ('group',2,'other')",
                "INSERT INTO guarded VALUES ('new',0,'middle'),('new',1,'other')",
            ] {
                assert!(
                    oracle.execute_batch(bad).is_err(),
                    "stock must reject: {bad}"
                );
                assert!(
                    conn.execute_batch(bad).await.is_err(),
                    "engine must reject: {bad}"
                );
            }
            compare(&conn, &oracle, "SELECT COUNT(*) FROM guarded", 1).await;
        }
    });
}
