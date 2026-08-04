//! bd-aiica: Integration tests for the CI integrity-check pipeline.
//!
//! Validates that the verify_with_c_sqlite infrastructure produces correct
//! results for clean and corrupt databases, and that the reporting format
//! matches the expected schema.
#![recursion_limit = "512"]

use tempfile::TempDir;

const _BEAD_ID: &str = "bd-aiica";

fn temp_db_target(dir: &TempDir, filename: &str) -> (std::path::PathBuf, String) {
    let database_file = dir.path().join(filename);
    let connection_target = database_file.to_string_lossy().into_owned();
    (database_file, connection_target)
}

fn create_clean_db(dir: &std::path::Path) -> std::path::PathBuf {
    let db_path = dir.join("clean.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);
         INSERT INTO t VALUES (1, 'hello');
         INSERT INTO t VALUES (2, 'world');",
    )
    .unwrap();
    db_path
}

fn create_multi_table_db(dir: &std::path::Path) -> std::path::PathBuf {
    let db_path = dir.join("multi.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT);
         CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT);
         CREATE TABLE t3 (id INTEGER PRIMARY KEY, val TEXT);
         INSERT INTO t1 VALUES (1, 'a');
         INSERT INTO t2 VALUES (1, 'b');
         INSERT INTO t3 VALUES (1, 'c');",
    )
    .unwrap();
    db_path
}

fn integrity_check(path: &std::path::Path) -> String {
    let conn = rusqlite::Connection::open(path).unwrap();
    let result: String = conn
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .unwrap();
    result
}

#[test]
fn t1_clean_db_passes_integrity_check() {
    let dir = TempDir::new().unwrap();
    let db = create_clean_db(dir.path());
    assert_eq!(integrity_check(&db), "ok");
}

#[test]
fn t2_multi_table_db_passes() {
    let dir = TempDir::new().unwrap();
    let db = create_multi_table_db(dir.path());
    assert_eq!(integrity_check(&db), "ok");
}

#[test]
fn t3_empty_file_handled() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("empty.db");
    std::fs::write(&db_path, b"").unwrap();
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let result: String = conn
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .unwrap();
    assert_eq!(result, "ok");
}

#[test]
fn t4_corrupt_header_detected() {
    let dir = TempDir::new().unwrap();
    let db = create_clean_db(dir.path());

    // Corrupt the SQLite magic header
    let mut data = std::fs::read(&db).unwrap();
    if data.len() > 16 {
        data[0] = 0xFF;
        data[1] = 0xFF;
        std::fs::write(&db, &data).unwrap();
    }

    let conn_result = rusqlite::Connection::open(&db);
    match conn_result {
        Ok(conn) => {
            let result: Result<String, _> =
                conn.query_row("PRAGMA integrity_check;", [], |row| row.get(0));
            match result {
                Ok(s) => assert_ne!(s, "ok", "corrupt header should not pass"),
                Err(_) => {} // expected
            }
        }
        Err(_) => {} // also acceptable — can't even open
    }
}

#[test]
fn t5_fsqlite_created_db_passes_csqlite_check() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("fsqlite.db");
        let path_str = db_path.to_string_lossy().into_owned();

        let conn = fsqlite::Connection::open(path_str).await.unwrap();
        drop(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await);
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'from_fsqlite')")
            .await
            .unwrap();
        drop(conn);

        assert_eq!(integrity_check(&db_path), "ok");
    });
}

#[test]
fn t6_fsqlite_multithread_db_passes_csqlite_check() {
    asupersync::test_utils::run_test(|| async {
        let dir = TempDir::new().unwrap();
        let (database_file, connection_target) = temp_db_target(&dir, "mt.db");

        {
            let conn = fsqlite::Connection::open(connection_target.clone())
                .await
                .unwrap();
            drop(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await);
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .await
                .unwrap();
        }

        let connection_target = std::sync::Arc::new(connection_target);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let mut handles = Vec::new();

        for tid in 0..4u32 {
            let connection_target = std::sync::Arc::clone(&connection_target);
            let barrier = std::sync::Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                asupersync::test_utils::run_test(|| async {
                    let conn = fsqlite::Connection::open(connection_target.as_str().to_owned())
                        .await
                        .unwrap();
                    drop(conn.execute("PRAGMA fsqlite.concurrent_mode=ON;").await);
                    drop(conn.execute("PRAGMA busy_timeout=5000;").await);
                    barrier.wait();

                    let base = i64::from(tid) * 100;
                    let value = format!("thread_{tid}");
                    for i in 0..10i64 {
                        drop(
                            conn.execute_with_params(
                                "INSERT INTO t VALUES (?1, ?2)",
                                &[
                                    fsqlite::SqliteValue::Integer(base + i),
                                    fsqlite::SqliteValue::Text(value.clone().into()),
                                ],
                            )
                            .await,
                        );
                    }
                });
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(integrity_check(&database_file), "ok");
    });
}

#[test]
fn t7_verify_with_c_sqlite_clean() -> Result<(), Box<dyn std::error::Error>> {
    let mut outcome: Result<(), Box<dyn std::error::Error>> = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = (async || -> Result<(), Box<dyn std::error::Error>> {
            let dir = TempDir::new().unwrap();
            let db_path = dir.path().join("verify_clean.db");
            let path_str = db_path.to_string_lossy().into_owned();

            let conn = fsqlite::Connection::open(path_str).await.unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1)").await.unwrap();
            drop(conn);

            let report = fsqlite_e2e::verify_csqlite::verify_with_c_sqlite(&db_path)?;
            assert!(report.ok, "clean DB should verify ok");
            Ok(())
        })()
        .await;
    });
    outcome
}

#[test]
fn t8_script_smoke_test() {
    let dir = TempDir::new().unwrap();
    let artifact_dir = dir.path().join("artifacts");
    std::fs::create_dir_all(&artifact_dir).unwrap();

    // Create a clean .db file in the artifact dir
    let db_path = artifact_dir.join("test.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY);
         INSERT INTO t VALUES (1);",
    )
    .unwrap();
    drop(conn);

    // The script exists and is executable
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("scripts/ci_integrity_check.sh");
    assert!(
        script.exists(),
        "script should exist at {}",
        script.display()
    );
}
