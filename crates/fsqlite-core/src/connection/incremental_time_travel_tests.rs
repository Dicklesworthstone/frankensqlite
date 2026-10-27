//! GH494 guards for Connection's explicit-COMMIT page-image publication.
//!
//! These inspect the production capture's own get_page counter, not an
//! independently reconstructed write set. They do NOT claim to bound mirror
//! hydration or total transaction latency; gh494_memory_commit_scaling remains
//! the full transaction-plus-read gate and must not be relaxed for this change.

#[allow(clippy::wildcard_imports)]
use super::*;

fn latest(conn: &Connection) -> (u64, Rc<TimeTravelImage>) {
    let snapshots = conn.time_travel_snapshots.borrow();
    let snapshot = snapshots.last().expect("committed history");
    (snapshot.commit_seq, Rc::clone(&snapshot.image))
}

async fn exec(conn: &Connection, statements: &[&str]) {
    for statement in statements {
        conn.execute(statement).await.expect(statement);
    }
}

async fn integers(conn: &Connection, sql: &str) -> Vec<Vec<i64>> {
    conn.query(sql)
        .await
        .expect(sql)
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Integer(value) => *value,
                    other => panic!("expected integer, found {other:?}: {sql}"),
                })
                .collect()
        })
        .collect()
}

async fn grow_resident(conn: &Connection, from: usize, to: usize) {
    if from == to {
        return;
    }
    let payload = format!("X'{}'", "ab".repeat(4096));
    conn.execute("BEGIN").await.unwrap();
    for id in from + 1..=to {
        conn.execute(&format!("INSERT INTO resident VALUES({id},{payload})"))
            .await
            .unwrap();
    }
    conn.execute("COMMIT").await.unwrap();
}

async fn exercise_capture_bound(stages: &[usize], repetitions: i64) {
    let conn = Connection::open(":memory:".to_owned()).await.unwrap();
    exec(
        &conn,
        &[
            "CREATE TABLE resident(id INTEGER PRIMARY KEY, payload BLOB NOT NULL)",
            "CREATE TABLE hot(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
            "BEGIN",
            "INSERT INTO hot VALUES(1,0)",
            "COMMIT",
        ],
    )
    .await;
    let mut resident = 0;
    let mut value = 0;
    for &stage in stages {
        grow_resident(&conn, resident, stage).await;
        resident = stage;
        for _ in 0..repetitions {
            value += 1;
            let (_, previous) = latest(&conn);
            conn.execute("BEGIN").await.unwrap();
            conn.execute(&format!("UPDATE hot SET value={value} WHERE id=1"))
                .await
                .unwrap();
            conn.execute("COMMIT").await.unwrap();
            let (_, image) = latest(&conn);
            assert!(!image.page_capture.full_capture, "resident rows={resident}");
            assert!(
                image.page_capture.pages_read <= 16,
                "resident rows={resident}, {:?}",
                image.page_capture
            );
            assert_eq!(previous.pages.len(), image.pages.len());
            assert_eq!(
                previous.pager_commit_seq.unwrap().get().checked_add(1),
                image.pager_commit_seq.map(|sequence| sequence.get())
            );
            assert_eq!(
                integers(&conn, "SELECT value FROM hot WHERE id=1").await,
                vec![vec![value]]
            );
        }
    }
    assert_eq!(
        integers(
            &conn,
            "SELECT count(*), coalesce(sum(length(payload)),0) FROM resident"
        )
        .await,
        vec![vec![resident as i64, resident as i64 * 4096]]
    );
}

#[test]
fn explicit_commit_capture_tracks_pages_not_resident_rows() {
    asupersync::test_utils::run_test(|| async {
        exercise_capture_bound(&[0, 16, 64, 256], 3).await;
    });
}

#[test]
#[ignore = "32 MiB release-mode production-capture regression"]
fn explicit_commit_capture_stays_bounded_through_32_mib() {
    asupersync::test_utils::run_test(|| async {
        exercise_capture_bound(&[0, 128, 512, 2048, 8192], 101).await;
    });
}

#[test]
fn incremental_history_preserves_trigger_index_savepoint_and_full_rollback() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:".to_owned()).await.unwrap();
        exec(
            &conn,
            &[
                "CREATE TABLE hot(id INTEGER PRIMARY KEY, k INTEGER NOT NULL)",
                "CREATE UNIQUE INDEX hot_k ON hot(k)",
                "CREATE TABLE audit(id INTEGER PRIMARY KEY, k INTEGER NOT NULL)",
                "CREATE TRIGGER hot_audit AFTER UPDATE ON hot BEGIN INSERT INTO audit(k) VALUES(NEW.k); END",
                "BEGIN",
                "INSERT INTO hot VALUES(1,10),(2,20)",
                "COMMIT",
            ],
        )
        .await;
        let (before, old_image) = latest(&conn);
        exec(
            &conn,
            &[
                "BEGIN",
                "UPDATE hot SET k=30 WHERE id=1",
                "SAVEPOINT nested",
                "UPDATE hot SET k=99 WHERE id=1",
                "DELETE FROM hot WHERE id=2",
                "ROLLBACK TO nested",
                "RELEASE nested",
                "COMMIT",
            ],
        )
        .await;
        let (after, image) = latest(&conn);
        assert!(!image.page_capture.full_capture);
        assert_eq!(
            integers(&conn, "SELECT id,k FROM hot INDEXED BY hot_k ORDER BY k").await,
            vec![vec![2, 20], vec![1, 30]]
        );
        assert_eq!(
            integers(&conn, "SELECT k FROM audit ORDER BY id").await,
            vec![vec![30]]
        );
        assert_eq!(
            integers(
                &conn,
                &format!("SELECT id,k FROM hot FOR SYSTEM_TIME AS OF COMMITSEQ {before} ORDER BY id")
            )
            .await,
            vec![vec![1, 10], vec![2, 20]]
        );
        assert_eq!(
            integers(
                &conn,
                &format!("SELECT k FROM audit FOR SYSTEM_TIME AS OF COMMITSEQ {after} ORDER BY id")
            )
            .await,
            vec![vec![30]]
        );

        exec(
            &conn,
            &["BEGIN", "UPDATE hot SET k=50 WHERE id=1", "ROLLBACK"],
        )
        .await;
        let (unchanged, _) = latest(&conn);
        assert_eq!(
            unchanged, after,
            "rollback must not publish a new history entry"
        );
        assert_eq!(
            integers(&conn, "SELECT id,k FROM hot INDEXED BY hot_k ORDER BY k").await,
            vec![vec![2, 20], vec![1, 30]]
        );
        assert_eq!(
            integers(&conn, "SELECT k FROM audit ORDER BY id").await,
            vec![vec![30]]
        );
        // Read an old snapshot again after both a commit and rollback; the
        // shared directory must not have been mutated in place.
        let historical = conn
            .materialize_time_travel_image(&conn.op_cx().unwrap(), &old_image)
            .await
            .expect("historical image still decodes");
        drop(historical);
        assert_eq!(
            integers(
                &conn,
                &format!("SELECT id,k FROM hot FOR SYSTEM_TIME AS OF COMMITSEQ {before} ORDER BY id")
            )
            .await,
            vec![vec![1, 10], vec![2, 20]]
        );
    });
}

#[test]
fn disabled_capture_gap_is_reseeded_instead_of_losing_unobserved_writes() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:".to_owned()).await.unwrap();
        exec(
            &conn,
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER)",
                "BEGIN",
                "INSERT INTO t VALUES(1,10)",
                "COMMIT",
            ],
        )
        .await;
        let (old, _) = latest(&conn);
        exec(
            &conn,
            &[
                "PRAGMA fsqlite_capture_time_travel_snapshots=false",
                "BEGIN",
                "INSERT INTO t VALUES(2,20)",
                "COMMIT",
            ],
        )
        .await;
        assert_eq!(latest(&conn).0, old);
        exec(
            &conn,
            &[
                "PRAGMA fsqlite_capture_time_travel_snapshots=true",
                "BEGIN",
                "UPDATE t SET k=30 WHERE id=1",
                "COMMIT",
            ],
        )
        .await;
        let (now, image) = latest(&conn);
        assert!(
            image.page_capture.full_capture,
            "the unobserved commit has no delta receipt"
        );
        assert_eq!(
            integers(
                &conn,
                &format!("SELECT id,k FROM t FOR SYSTEM_TIME AS OF COMMITSEQ {now} ORDER BY id")
            )
            .await,
            vec![vec![1, 30], vec![2, 20]]
        );
        exec(&conn, &["BEGIN", "UPDATE t SET k=40 WHERE id=1", "COMMIT"]).await;
        assert!(!latest(&conn).1.page_capture.full_capture);
    });
}
