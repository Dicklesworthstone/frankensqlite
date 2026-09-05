//! bd-aoj0g: measurement harness for the :memory: secondary-index INSERT
//! cost curve. Not a pass/fail keeper — run explicitly with `--ignored` to
//! print ns/insert per 1000-row batch at rising table depth for six
//! shapes:
//!   A) no secondary index, k = i % 100      (table-only baseline)
//!   B) index on k,        k = i             (sequential/monotonic keys)
//!   C) index on k,        k = i % 100       (the bd-aoj0g O(n) ramp shape)
//! AOJ0G_ROWS selects the row count. AOJ0G_DB_DIR selects file-backed runs;
//! omit it for :memory:. Timed INSERT loops are unchanged from the baseline.

use fsqlite::Connection;
use std::time::Instant;

const BATCH: usize = 1_000;

fn rows() -> usize {
    let count = std::env::var("AOJ0G_ROWS").map_or(10_000, |value| {
        value.parse().expect("AOJ0G_ROWS is an integer")
    });
    assert!(count >= BATCH && count.is_multiple_of(BATCH));
    count
}

async fn open_case(name: &str) -> Connection {
    let path = std::env::var("AOJ0G_DB_DIR").map_or_else(
        |_| ":memory:".to_owned(),
        |directory| {
            std::fs::create_dir_all(&directory).unwrap();
            let path = std::path::Path::new(&directory).join(format!("{}.db", &name[..1]));
            assert!(!path.exists(), "measurement requires a fresh database");
            path.to_str().unwrap().to_owned()
        },
    );
    let conn = Connection::open(&path).await.unwrap();
    assert!(conn.is_concurrent_mode_default());
    conn
}

async fn check_and_time_reads(conn: &Connection, name: &str) {
    assert_eq!(
        conn.query("PRAGMA integrity_check").await.unwrap()[0].values()[0].as_text(),
        Some("ok")
    );
    let start = Instant::now();
    for i in 0..BATCH {
        let id = i * rows() / BATCH + 1;
        let result = conn
            .query(&format!("SELECT v FROM t WHERE id={id}"))
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].values()[0].as_text(),
            Some(format!("payload-{}", id - 1).as_str())
        );
    }
    println!(
        "[aoj0g-read] {name}: ns/query={}",
        start.elapsed().as_nanos() / BATCH as u128
    );
}

async fn run_case_with_commit_every_batch(name: &str, key_mod: Option<usize>) {
    let conn = open_case(name).await;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);")
        .await
        .unwrap();
    conn.execute("CREATE INDEX idx_t_k ON t(k);").await.unwrap();
    let mut batch_times = Vec::new();
    for batch_start in (0..rows()).step_by(BATCH) {
        conn.execute("BEGIN;").await.unwrap();
        let start = Instant::now();
        for i in batch_start..batch_start + BATCH {
            let k = key_mod.map_or(i, |m| i % m);
            conn.execute(&format!(
                "INSERT INTO t (id, k, v) VALUES ({}, {}, 'payload-{}');",
                i + 1,
                k,
                i
            ))
            .await
            .unwrap();
        }
        let ns_per_insert = start.elapsed().as_nanos() / BATCH as u128;
        batch_times.push(ns_per_insert);
        conn.execute("COMMIT;").await.unwrap();
    }
    let first = batch_times.first().copied().unwrap_or(0);
    let last = batch_times.last().copied().unwrap_or(0);
    let ramp = if first > 0 {
        format!("{:.2}x", last as f64 / first as f64)
    } else {
        "n/a".to_owned()
    };
    println!(
        "[aoj0g] {name}: per-batch ns/insert = {batch_times:?} | first {first} last {last} ramp {ramp}"
    );
    check_and_time_reads(&conn, name).await;
    conn.close().await.unwrap();
}

async fn run_case(name: &str, with_index: bool, key_mod: Option<usize>) {
    let conn = open_case(name).await;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);")
        .await
        .unwrap();
    if with_index {
        conn.execute("CREATE INDEX idx_t_k ON t(k);").await.unwrap();
    }
    conn.execute("BEGIN;").await.unwrap();
    let mut batch_times = Vec::new();
    for batch_start in (0..rows()).step_by(BATCH) {
        let start = Instant::now();
        for i in batch_start..batch_start + BATCH {
            let k = key_mod.map_or(i, |m| i % m);
            conn.execute(&format!(
                "INSERT INTO t (id, k, v) VALUES ({}, {}, 'payload-{}');",
                i + 1,
                k,
                i
            ))
            .await
            .unwrap();
        }
        let ns_per_insert = start.elapsed().as_nanos() / BATCH as u128;
        batch_times.push(ns_per_insert);
    }
    conn.execute("COMMIT;").await.unwrap();
    let first = batch_times.first().copied().unwrap_or(0);
    let last = batch_times.last().copied().unwrap_or(0);
    let ramp = if first > 0 {
        format!("{:.2}x", last as f64 / first as f64)
    } else {
        "n/a".to_owned()
    };
    println!(
        "[aoj0g] {name}: per-batch ns/insert = {batch_times:?} | first {first} last {last} ramp {ramp}"
    );
    check_and_time_reads(&conn, name).await;
    conn.close().await.unwrap();
}

async fn compare_integer_rows(franken: &Connection, stock: &rusqlite::Connection) -> String {
    let sql = "SELECT id,u,k,length(v) FROM t ORDER BY id";
    let actual: Vec<String> = franken
        .query(sql)
        .await
        .unwrap()
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    let mut statement = stock.prepare(sql).unwrap();
    let expected: Vec<String> = statement
        .query_map([], |row| {
            Ok((0..4)
                .map(|index| row.get::<_, i64>(index).unwrap().to_string())
                .collect::<Vec<_>>()
                .join("|"))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        franken.query("PRAGMA integrity_check").await.unwrap()[0].values()[0].as_text(),
        Some("ok")
    );
    actual.join("\n")
}

#[test]
fn aoj0g_indexed_savepoint_statement_failure_oracle() {
    asupersync::test_utils::run_test(|| async {
        // Preserve the physical image for independent sqlite3 inspection.
        let directory = tempfile::tempdir().unwrap().keep();
        for file_backed in [false, true] {
            let path = directory.join("savepoints.db");
            let franken = Connection::open(if file_backed {
                path.to_str().unwrap()
            } else {
                ":memory:"
            })
            .await
            .unwrap();
            assert!(franken.is_concurrent_mode_default());
            let stock = rusqlite::Connection::open_in_memory().unwrap();
            for sql in [
                "CREATE TABLE t(id INTEGER PRIMARY KEY,u INTEGER UNIQUE,k INTEGER,v TEXT)",
                "CREATE INDEX idx_t_k ON t(k)",
                "BEGIN",
            ] {
                franken.execute(sql).await.unwrap();
                stock.execute_batch(sql).unwrap();
            }
            for id in 1..=512 {
                let sql = format!(
                    "INSERT INTO t VALUES({id},{id},{},'{}')",
                    id % 13,
                    "x".repeat(9000)
                );
                franken.execute(&sql).await.unwrap();
                stock.execute_batch(&sql).unwrap();
            }
            for sql in [
                "SAVEPOINT outer_sp",
                "UPDATE t SET k=99,v='changed' WHERE id<=10",
                "SAVEPOINT inner_sp",
                "DELETE FROM t WHERE id BETWEEN 11 AND 30",
                "RELEASE inner_sp",
                "INSERT INTO t VALUES(1001,1001,7,'first'),(1002,1,8,'duplicate')",
                "ROLLBACK TO outer_sp",
                "UPDATE t SET k=88 WHERE id=512",
                "ROLLBACK TO outer_sp",
                "RELEASE outer_sp",
                "COMMIT",
            ] {
                println!("[aoj0g-oracle] file_backed={file_backed} step={sql}");
                let actual = franken.execute(sql).await;
                let expected = stock.execute_batch(sql);
                assert_eq!(
                    actual.is_ok(),
                    expected.is_ok(),
                    "{sql}: {actual:?} vs {expected:?}"
                );
                compare_integer_rows(&franken, &stock).await;
                let index_sql = "SELECT id FROM t INDEXED BY idx_t_k WHERE k=7 ORDER BY id";
                let actual: Vec<String> = franken
                    .query(index_sql)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values()[0].to_string())
                    .collect();
                let mut statement = stock.prepare(index_sql).unwrap();
                let expected: Vec<String> = statement
                    .query_map([], |row| Ok(row.get::<_, i64>(0)?.to_string()))
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
                assert_eq!(actual, expected, "index contents after {sql}");
            }
            let expected = compare_integer_rows(&franken, &stock).await;
            franken.close().await.unwrap();
            if file_backed {
                let output = std::process::Command::new("sqlite3")
                    .arg(&path)
                    .arg("PRAGMA integrity_check; SELECT id,u,k,length(v) FROM t ORDER BY id;")
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                assert_eq!(
                    String::from_utf8(output.stdout).unwrap(),
                    format!("ok\n{expected}\n")
                );
            }
        }
        println!("[aoj0g-oracle] preserved {}", directory.display());
    });
}

#[test]
#[ignore = "bd-aoj0g measurement harness, run explicitly with --ignored; prints timings"]
fn aoj0g_insert_phase_profile() {
    asupersync::test_utils::run_test(|| async {
        run_case("A no-index    k=i%100", false, Some(100)).await;
        run_case("B with-index  k=i     ", true, None).await;
        run_case("C with-index  k=i%100", true, Some(100)).await;
        // bd-aoj0g duplicate-run hypothesis probes: if the seek walks the
        // duplicate run linearly, D (one key, all rows duplicates) should
        // ramp hardest and E (10 dups/key at 10k rows) mildly — while a pure
        // cache-locality cause would rank C >= E > D (D touches one hot
        // subtree with perfect locality).
        run_case("D with-index  k=1     ", true, Some(1)).await;
        run_case("E with-index  k=i%1000", true, Some(1000)).await;
        // F: same key shape as C but a fresh transaction per 1000-row batch.
        // Flat F => the O(n) state lives in the open transaction (staged
        // pages / witness set); ramping F => persistent structure (page
        // cache, mvcc globals, index size itself).
        run_case_with_commit_every_batch("F txn/batch   k=i%100", Some(100)).await;
    });
}
