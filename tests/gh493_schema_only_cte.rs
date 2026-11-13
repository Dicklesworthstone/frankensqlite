//! GH#493: isolate schema-only CTE hydration from fixture construction.
//!
//! The ignored profile runs each query in a fresh process. Run with
//! GH493_EXPECT=baseline before the fix, or GH493_EXPECT=bounded after it:
//! cargo test -p fsqlite-core --no-default-features --features native,ext-json \
//!   --test gh493_schema_only_cte gh493_isolated_profile -- --ignored --exact --nocapture
//! No elapsed-time/RSS thresholds: the per-connection hydration counter is the
//! deterministic oracle. VmRSS/VmHWM are supplementary Linux measurements.

#![recursion_limit = "512"]

use std::path::Path;
use std::process::Command;
use std::time::Instant;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const BULK_ROWS: i64 = 4096;
const POINT: &str = "SELECT v FROM a WHERE id = 'k042'";
const JOIN: &str =
    "SELECT a.id, a.v + b.v FROM a JOIN b ON b.id = a.id ORDER BY a.id";
const CTE: &str = "WITH c AS (SELECT id, v FROM a) \
    SELECT c.id, c.v + b.v FROM c JOIN b ON b.id = c.id ORDER BY c.id";

fn fixture(path: &Path) {
    let mut db = rusqlite::Connection::open(path).expect("create stock SQLite fixture");
    db.execute_batch(
        "PRAGMA journal_mode=DELETE; PRAGMA synchronous=OFF; \
         CREATE TABLE bulk(id INTEGER PRIMARY KEY, body TEXT NOT NULL); \
         CREATE TABLE a(id TEXT PRIMARY KEY, v INTEGER NOT NULL); \
         CREATE TABLE b(id TEXT PRIMARY KEY, v INTEGER NOT NULL);",
    )
    .expect("fixture schema");
    let tx = db.transaction().expect("fixture transaction");
    {
        let body = "x".repeat(2048);
        let mut insert = tx.prepare("INSERT INTO bulk VALUES (?1, ?2)").unwrap();
        for id in 0..BULK_ROWS {
            insert.execute(rusqlite::params![id, &body]).unwrap();
        }
    }
    for id in 0..100_i64 {
        let key = format!("k{id:03}");
        tx.execute("INSERT INTO a VALUES (?1, ?2)", rusqlite::params![&key, id])
            .unwrap();
        tx.execute(
            "INSERT INTO b VALUES (?1, ?2)",
            rusqlite::params![&key, id * 2],
        )
        .unwrap();
    }
    tx.commit().expect("commit fixture");
    let bytes = std::fs::metadata(path).unwrap().len();
    assert!(bytes >= 8 * 1024 * 1024, "fixture must have unrelated bulk pages");
    assert!(bytes <= 24 * 1024 * 1024, "fixture must stay bounded");
}

fn proc_kib(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix(field)?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

#[test]
#[ignore = "isolated measurement entry point; use gh493_isolated_profile"]
fn gh493_measurement_child() {
    let path = std::env::var("GH493_FIXTURE").expect("profile parent supplies the fixture");
    let shape = std::env::var("GH493_SHAPE").expect("profile parent supplies the shape");
    let expectation = std::env::var("GH493_EXPECT").unwrap_or_else(|_| "bounded".to_owned());
    assert!(matches!(expectation.as_str(), "baseline" | "bounded" | "observe"));
    let sql = match shape.as_str() {
        "point" => POINT,
        "join" => JOIN,
        "cte" => CTE,
        _ => panic!("unknown profile shape: {shape}"),
    };
    asupersync::test_utils::run_test(|| async {
        let open_started = Instant::now();
        let conn = Connection::open_existing_schema_only(&path)
            .await
            .expect("open existing schema-only fixture");
        let open_ns = open_started.elapsed().as_nanos();
        let hydrated_before = conn.memdb_row_hydration_count();
        assert_eq!(hydrated_before, 0, "schema-only open hydrated user rows");
        let rss_before = proc_kib("VmRSS:");
        let hwm_before = proc_kib("VmHWM:");
        let started = Instant::now();
        let rows = conn.query(sql).await.expect("profile query");
        let elapsed_ns = started.elapsed().as_nanos();
        let rss_after = proc_kib("VmRSS:");
        let hwm_after = proc_kib("VmHWM:");
        let hydrated = conn.memdb_row_hydration_count() - hydrated_before;
        if shape == "point" {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values(), &[SqliteValue::Integer(42)]);
        } else {
            assert_eq!(rows.len(), 100);
            for (id, row) in (0..100_i64).zip(&rows) {
                assert_eq!(
                    row.values(),
                    &[
                        SqliteValue::Text(format!("k{id:03}").into()),
                        SqliteValue::Integer(id * 3),
                    ]
                );
            }
        }
        println!(
            "GH493_MEASUREMENT {}",
            serde_json::json!({
                "shape": shape,
                "expectation": expectation,
                "fixture_bytes": std::fs::metadata(&path).unwrap().len(),
                "bulk_rows": BULK_ROWS,
                "result_rows": rows.len(),
                "open_ns": open_ns,
                "query_ns": elapsed_ns,
                "hydrated_rows": hydrated,
                "rss_before_kib": rss_before,
                "rss_after_kib": rss_after,
                "rss_growth_kib": rss_after.zip(rss_before).map(|(a, b)| i128::from(a) - i128::from(b)),
                "hwm_before_kib": hwm_before,
                "hwm_after_kib": hwm_after,
            })
        );
        match (expectation.as_str(), shape.as_str()) {
            ("baseline", "cte") => assert!(
                hydrated >= u64::try_from(BULK_ROWS).unwrap(),
                "baseline did not reproduce whole-file hydration: {hydrated} rows"
            ),
            ("observe", _) => {},
            _ => assert_eq!(hydrated, 0, "schema-only {shape} hydrated persistent rows"),
        }
        conn.close_without_checkpoint().await.expect("close reader");
    });
}

#[test]
#[ignore = "explicit before/after profile; each sample uses a fresh subprocess"]
fn gh493_isolated_profile() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gh493.db");
    fixture(&path);
    let executable = std::env::current_exe().unwrap();
    for sample in 0..3 {
        for shape in ["point", "join", "cte"] {
            let output = Command::new(&executable)
                .args([
                    "--ignored", "--exact", "gh493_measurement_child", "--nocapture",
                    "--test-threads=1",
                ])
                .env("GH493_FIXTURE", &path)
                .env("GH493_SHAPE", shape)
                .output()
                .expect("spawn isolated profile child");
            println!("GH493_SAMPLE sample={sample} shape={shape}");
            print!("{}", String::from_utf8_lossy(&output.stdout));
            eprint!("{}", String::from_utf8_lossy(&output.stderr));
            assert!(output.status.success(), "sample {sample}, {shape}: {}", output.status);
        }
    }
}
