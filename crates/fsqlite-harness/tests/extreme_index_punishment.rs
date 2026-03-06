use fsqlite::Connection;
use fsqlite::compat::BatchExt;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_extreme_index_punishment() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("extreme_index_punishment.db");
    let db = db_path.to_string_lossy().into_owned();

    let setup = Connection::open(&db).unwrap();
    setup
        .execute_batch(
            "PRAGMA journal_mode=WAL;
         PRAGMA busy_timeout=5000;
         CREATE TABLE items (id INTEGER PRIMARY KEY, category INTEGER, data TEXT);
         CREATE INDEX idx_category ON items(category);",
        )
        .unwrap();

    let n_threads = 20;
    let ops_per_thread = 200;
    let barrier = Arc::new(Barrier::new(n_threads));

    let mut handles = vec![];
    for tid in 0..n_threads {
        let bar = barrier.clone();
        let path = db.clone();
        handles.push(thread::spawn(move || -> Result<(), String> {
            let conn = Connection::open(&path).map_err(|e| format!("{:?}", e))?;
            conn.execute_batch("PRAGMA busy_timeout=5000;")
                .map_err(|e| format!("{:?}", e))?;
            bar.wait();

            for i in 0..ops_per_thread {
                let id = tid * 10000 + i;
                let category = 42; // ALL threads hammer the EXACT same index bucket to force maximum splits/merges
                let data = format!("thread_{}_op_{}", tid, i);
                let insert_sql = format!(
                    "INSERT INTO items (id, category, data) VALUES ({}, {}, '{}')",
                    id, category, data
                );

                loop {
                    if conn.in_transaction() {
                        match conn.execute("ROLLBACK;") {
                            Ok(_) => {}
                            Err(_err) if !conn.in_transaction() => {}
                            Err(err) => {
                                return Err(format!(
                                    "thread={tid} op={i} failed to clear stale transaction before retry: {err}"
                                ));
                            }
                        }
                    }

                    match conn.execute("BEGIN CONCURRENT;") {
                        Ok(_) => {}
                        Err(err) if err.is_transient() => {
                            thread::sleep(std::time::Duration::from_millis(1));
                            continue;
                        }
                        Err(err) => {
                            return Err(format!(
                                "thread={tid} op={i} begin failed unexpectedly: {err}"
                            ));
                        }
                    }

                    match conn.execute(&insert_sql) {
                        Ok(changes) => {
                            if changes != 1 {
                                let _ = conn.execute("ROLLBACK;");
                                return Err(format!(
                                    "thread={tid} op={i} insert affected {changes} rows"
                                ));
                            }
                        }
                        Err(err) if err.is_transient() => {
                            match conn.execute("ROLLBACK;") {
                                Ok(_) => {}
                                Err(_rollback_err) if !conn.in_transaction() => {}
                                Err(rollback_err) => {
                                    return Err(format!(
                                        "thread={tid} op={i} rollback after transient insert error failed: {rollback_err}"
                                    ));
                                }
                            }
                            thread::sleep(std::time::Duration::from_millis(1));
                            continue;
                        }
                        Err(err) => {
                            let _ = conn.execute("ROLLBACK;");
                            return Err(format!(
                                "thread={tid} op={i} insert failed unexpectedly: {err}"
                            ));
                        }
                    }

                    match conn.execute("COMMIT;") {
                        Ok(_) => break,
                        Err(err) if err.is_transient() => {
                            match conn.execute("ROLLBACK;") {
                                Ok(_) => {}
                                Err(_rollback_err) if !conn.in_transaction() => {}
                                Err(rollback_err) => {
                                    return Err(format!(
                                        "thread={tid} op={i} rollback after transient commit error failed: {rollback_err}"
                                    ));
                                }
                            }
                            thread::sleep(std::time::Duration::from_millis(1));
                        }
                        Err(err) => {
                            return Err(format!(
                                "thread={tid} op={i} commit failed unexpectedly: {err}"
                            ));
                        }
                    }
                }
            }
            Ok(())
        }));
    }

    for h in handles {
        let res = h.join().unwrap();
        assert!(res.is_ok(), "Thread failed: {:?}", res);
    }

    let rows = setup.query("SELECT COUNT(*) FROM items").unwrap();
    let total: i64 = match &rows[0].values()[0] {
        fsqlite_types::SqliteValue::Integer(i) => *i,
        _ => panic!("Expected integer"),
    };

    assert_eq!(total, (n_threads * ops_per_thread) as i64);

    // Also verify the index is intact
    let rows = setup
        .query("SELECT COUNT(*) FROM items INDEXED BY idx_category WHERE category = 42")
        .unwrap();
    let index_total: i64 = match &rows[0].values()[0] {
        fsqlite_types::SqliteValue::Integer(i) => *i,
        _ => panic!("Expected integer"),
    };
    assert_eq!(index_total, (n_threads * ops_per_thread) as i64);
}
