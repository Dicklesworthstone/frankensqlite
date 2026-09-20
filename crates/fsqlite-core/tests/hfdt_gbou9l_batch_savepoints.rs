//! Migration-copy regression: explicit caller-owned rollback must not clone
//! the complete catalog per constrained INSERT. Generic SQL, not provider proof.
use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

struct ProfileGuard;
impl Drop for ProfileGuard {
    fn drop(&mut self) {
        set_hot_path_profile_enabled(false);
    }
}

async fn count(conn: &Connection) -> i64 {
    match conn
        .query_row("SELECT count(*) FROM copied")
        .await
        .unwrap()
        .values()[0]
    {
        SqliteValue::Integer(value) => value,
        ref value => panic!("unexpected count: {value:?}"),
    }
}

#[test]
fn caller_owned_batch_rollback_avoids_per_row_catalog_snapshots() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("batch.sqlite");
        let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
        conn.execute(
            "CREATE TABLE copied (id INTEGER PRIMARY KEY, value INTEGER NOT NULL CHECK(value > 0))",
        )
        .await
        .unwrap();
        conn.execute("CREATE UNIQUE INDEX copied_value ON copied(value)")
            .await
            .unwrap();
        let sql = "INSERT INTO copied VALUES (?1, ?2)";
        let rows = (1..=128)
            .map(|n| vec![SqliteValue::Integer(n), SqliteValue::Integer(n)])
            .collect::<Vec<_>>();
        assert!(
            conn.execute_many_with_params_skip_statement_savepoint_in_explicit_txn(sql, &rows)
                .await
                .is_err()
        );
        conn.execute("BEGIN IMMEDIATE").await.unwrap();
        set_hot_path_profile_enabled(true);
        let guard = ProfileGuard;
        let before = hot_path_profile_snapshot().connection_snapshots;
        assert_eq!(
            conn.execute_many_with_params_skip_statement_savepoint_in_explicit_txn(sql, &rows)
                .await
                .unwrap(),
            rows.len()
        );
        let snapshots = hot_path_profile_snapshot().connection_snapshots - before;
        drop(guard);
        assert_eq!(count(&conn).await, 128);
        conn.execute("COMMIT").await.unwrap();
        drop(conn);
        let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
        assert_eq!(count(&conn).await, 128);

        // CHECK and UNIQUE still reject; the caller rolls back the entire batch.
        for bad in [0, 1] {
            conn.execute("BEGIN IMMEDIATE").await.unwrap();
            let failing = vec![
                vec![SqliteValue::Integer(129), SqliteValue::Integer(129)],
                vec![SqliteValue::Integer(130), SqliteValue::Integer(bad)],
            ];
            assert!(
                conn.execute_many_with_params_skip_statement_savepoint_in_explicit_txn(
                    sql, &failing
                )
                .await
                .is_err()
            );
            assert!(conn.in_transaction());
            conn.execute("ROLLBACK").await.unwrap();
            assert_eq!(count(&conn).await, 128);
        }
        // Ordinary execution still owns statement atomicity inside BEGIN.
        conn.execute("BEGIN IMMEDIATE").await.unwrap();
        assert!(
            conn.execute("INSERT INTO copied VALUES (129,129),(130,0)")
                .await
                .is_err()
        );
        assert_eq!(count(&conn).await, 128);
        conn.execute("ROLLBACK").await.unwrap();
        // Outside BEGIN the opt-in API must retain ordinary statement atomicity.
        assert!(
            conn.execute_with_params_skip_statement_savepoint_in_explicit_txn(
                "INSERT INTO copied VALUES (?1,?2),(?3,?4)",
                &[
                    SqliteValue::Integer(129),
                    SqliteValue::Integer(129),
                    SqliteValue::Integer(130),
                    SqliteValue::Integer(0)
                ],
            )
            .await
            .is_err()
        );
        assert_eq!(count(&conn).await, 128);
        // Opt-in multi-row INSERT must still see an earlier buffered write.
        conn.execute("CREATE TABLE buffered (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("BEGIN IMMEDIATE").await.unwrap();
        conn.execute_with_params(
            "INSERT INTO buffered VALUES (?1)",
            &[SqliteValue::Integer(1)],
        )
        .await
        .unwrap();
        assert!(
            conn.execute_with_params_skip_statement_savepoint_in_explicit_txn(
                "INSERT INTO buffered VALUES (?1), (?2)",
                &[SqliteValue::Integer(2), SqliteValue::Integer(1)],
            )
            .await
            .is_err()
        );
        conn.execute("ROLLBACK").await.unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM buffered")
                .await
                .unwrap()
                .values(),
            &[SqliteValue::Integer(0)]
        );
        let stock = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            stock
                .query_row("SELECT count(*) FROM copied", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            128
        );
        eprintln!(
            "batch rows={} full_connection_snapshots={snapshots}",
            rows.len()
        );
        assert_eq!(
            snapshots, 0,
            "explicit rollback ownership must avoid one full catalog clone per row"
        );
    });
}
