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

            // A secondary index sharing only the first PK column must not
            // turn a complete primary-key lookup into a growing-prefix scan.
            execute_both(
                &conn,
                &oracle,
                "CREATE INDEX by_owner_category ON items(owner, category);",
            )
            .await;
            // Force the backfilled secondary index too: a direct PK route
            // must not conceal malformed row locators in that index.
            set_hot_path_profile_enabled(true);
            let guard = ProfileGuard;
            let before = hot_path_profile_snapshot().vdbe.opcodes_executed_total;
            compare(
                &conn,
                &oracle,
                "SELECT ordinal FROM items INDEXED BY by_owner_category
                 WHERE owner='group' AND ordinal=511 LIMIT 1",
                1,
            )
            .await;
            let prefix_ops = hot_path_profile_snapshot().vdbe.opcodes_executed_total - before;
            let before = hot_path_profile_snapshot().vdbe.opcodes_executed_total;
            compare(
                &conn,
                &oracle,
                "SELECT 1 FROM items WHERE owner='group' AND ordinal=511 LIMIT 1",
                1,
            )
            .await;
            compare(
                &conn,
                &oracle,
                "SELECT 1 FROM items WHERE owner='group' AND ordinal=512 LIMIT 1",
                0,
            )
            .await;
            let point_ops = hot_path_profile_snapshot().vdbe.opcodes_executed_total - before;
            let before = hot_path_profile_snapshot().vdbe.opcodes_executed_total;
            compare(
                &conn,
                &oracle,
                "SELECT 1 FROM items NOT INDEXED
                 WHERE owner='group' AND ordinal=512 LIMIT 1",
                0,
            )
            .await;
            let scan_ops = hot_path_profile_snapshot().vdbe.opcodes_executed_total - before;
            drop(guard);
            eprintln!(
                "full_pk_competing_index suffix={suffix:?} hit_and_miss_ops={point_ops} scan_ops={scan_ops} forced_prefix_ops={prefix_ops}"
            );
            assert!((1..512).contains(&point_ops), "full-PK scan: {point_ops}");
            assert!(scan_ops > 512 && scan_ops > point_ops);
            assert!(prefix_ops > 512 && prefix_ops > point_ops);
            for (sql, expected) in [
                (
                    "SELECT ordinal FROM items WHERE owner='group' AND ordinal=511 AND category='middle' LIMIT 1",
                    1,
                ),
                (
                    "SELECT ordinal FROM items WHERE owner='group' AND ordinal=511 AND category='other' LIMIT 1",
                    0,
                ),
                (
                    "SELECT ordinal FROM items WHERE owner='group' AND ordinal=511 AND ordinal=510 LIMIT 1",
                    0,
                ),
                (
                    "SELECT ordinal FROM items WHERE owner='group' AND ordinal=511 LIMIT 0",
                    0,
                ),
                (
                    "SELECT ordinal FROM items WHERE owner='group' AND ordinal=511 LIMIT 1 OFFSET 1",
                    0,
                ),
            ] {
                compare(&conn, &oracle, sql, expected).await;
            }
            execute_both(
                &conn,
                &oracle,
                "CREATE UNIQUE INDEX by_reordered_pk ON items(ordinal,owner);
                 CREATE INDEX by_owner_nocase ON items(owner COLLATE NOCASE,category);
                 CREATE INDEX by_owner_expression ON items(owner,lower(category))
                    WHERE ordinal>=0;",
            )
            .await;
            for sql in [
                "SELECT ordinal FROM items INDEXED BY by_reordered_pk
                 WHERE ordinal=511 AND owner='group'",
                "SELECT ordinal FROM items INDEXED BY by_owner_expression
                 WHERE owner='group' AND ordinal>=0 AND ordinal=511",
            ] {
                compare(&conn, &oracle, sql, 1).await;
            }

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
                 INSERT INTO guarded VALUES {values};"
                ),
            )
            .await;
            let mut trigger_costs = Vec::new();
            for (ordinal, hint) in [(512, ""), (513, " NOT INDEXED")] {
                execute_both(
                    &conn,
                    &oracle,
                    &format!(
                        "DROP TRIGGER IF EXISTS guard_before;
                         DROP TRIGGER IF EXISTS guard_after;
                         CREATE TRIGGER guard_before BEFORE INSERT ON guarded
                         WHEN EXISTS(SELECT 1 FROM guarded AS member{hint}
                             WHERE member.owner=NEW.owner AND member.category<>NEW.category)
                         BEGIN SELECT RAISE(ABORT,'mixed category'); END;
                         CREATE TRIGGER guard_after AFTER INSERT ON guarded
                         WHEN EXISTS(SELECT 1 FROM guarded AS member{hint}
                             WHERE member.owner=NEW.owner AND member.category<>NEW.category)
                         BEGIN SELECT RAISE(ABORT,'mixed category'); END;"
                    ),
                )
                .await;
                set_hot_path_profile_enabled(true);
                let guard = ProfileGuard;
                let before = hot_path_profile_snapshot().vdbe.opcodes_executed_total;
                compare(
                    &conn,
                    &oracle,
                    &format!(
                        "INSERT INTO guarded VALUES ('group',{ordinal},'middle') RETURNING ordinal"
                    ),
                    1,
                )
                .await;
                trigger_costs
                    .push(hot_path_profile_snapshot().vdbe.opcodes_executed_total - before);
                drop(guard);
                for bad in [
                    "INSERT INTO guarded VALUES ('group',514,'other') RETURNING ordinal",
                    "INSERT INTO guarded VALUES ('new',0,'middle'),('new',1,'other') RETURNING ordinal",
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
            eprintln!("trigger_exclusion suffix={suffix:?} bounded_and_scan_ops={trigger_costs:?}");
            assert!((1..512).contains(&trigger_costs[0]), "{trigger_costs:?}");
            assert!(trigger_costs[1] > 512 && trigger_costs[1] > trigger_costs[0]);
            conn.close().await.expect("close engine connection");
            let reopened = rusqlite::Connection::open(&path).expect("stock reopen");
            let integrity = reopened
                .prepare("PRAGMA integrity_check")
                .expect("prepare integrity")
                .query_map([], |row| row.get::<_, String>(0))
                .expect("integrity query")
                .collect::<Result<Vec<_>, _>>()
                .expect("integrity rows");
            assert_eq!(integrity, ["ok"], "stock reopen {suffix:?}");
        }

        // The column's BINARY collation does not describe this table-level
        // NOCASE PK. Keep its separate BINARY index key and NOCASE PK suffix.
        let dir = tempfile::tempdir().expect("override tempdir");
        let path = dir.path().join("pk-collation-override.db");
        let stock = rusqlite::Connection::open(&path).expect("stock producer");
        stock
            .execute_batch(
                "CREATE TABLE overridden(owner TEXT,ordinal INTEGER,category TEXT,
                 PRIMARY KEY(owner COLLATE NOCASE,ordinal)) WITHOUT ROWID;
                 INSERT INTO overridden VALUES ('group',0,'value'),('group',1,'value');",
            )
            .expect("stock schema and rows");
        stock.close().expect("close stock");
        let conn = Connection::open(path.to_string_lossy())
            .await
            .expect("open overridden PK");
        conn.execute("CREATE INDEX separate_collations ON overridden(owner,category)")
            .await
            .expect("backfill overridden PK index");
        conn.close().await.expect("close overridden PK");
        let stock = rusqlite::Connection::open(&path).expect("stock reopen override");
        let integrity: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("override integrity");
        assert_eq!(integrity, "ok");
    });
}
