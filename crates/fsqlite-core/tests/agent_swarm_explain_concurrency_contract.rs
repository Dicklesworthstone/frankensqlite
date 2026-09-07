use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn real_reader_gauge_tracks_held_snapshots() -> TestResult {
    const MODE: &str = "FSQLITE_READER_GAUGE_TEST_MODE";
    let Ok(mode) = std::env::var(MODE) else {
        for mode in ["enabled", "disabled"] {
            let output = std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", "real_reader_gauge_tracks_held_snapshots", "--nocapture"])
                .env(MODE, mode)
                .env("FRANKENSQLITE_METRICS_DISABLE", if mode == "disabled" { "1" } else { "0" })
                .output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("reader_gauge mode={mode}\n{stdout}\n{stderr}");
            assert!(output.status.success(), "isolated {mode} snapshot keeper failed");
            assert!(stderr.contains("event=public_reader_snapshots_verified"));
        }
        return Ok(());
    };
    assert!(matches!(mode.as_str(), "enabled" | "disabled"));
    let enabled = mode == "enabled";
    let gauge = &fsqlite_observability::metrics::global().active_readers;
    let expect = |count, phase| {
        assert_eq!(gauge.get(), if enabled { count } else { 0 }, "{phase}");
    };
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let directory = tempfile::tempdir()?;
            for memory in [true, false] {
                let path = directory.path().join("reader-gauge.db");
                let conn = Connection::open(if memory { ":memory:" } else { path.to_str().unwrap() }).await?;
                assert!(conn.is_concurrent_mode_default());
                expect(0, "open without a held transaction");
                conn.execute("BEGIN").await?;
                expect(1, "default concurrent transaction also owns a read snapshot");
                assert!(conn.execute("BEGIN").await.is_err());
                expect(1, "rejected nested BEGIN must not add a snapshot");
                conn.execute("CREATE TABLE snapshots(id INTEGER PRIMARY KEY)").await?;
                conn.execute("INSERT INTO snapshots VALUES(7)").await?;
                conn.execute("SAVEPOINT s").await?;
                conn.execute("INSERT INTO snapshots VALUES(8)").await?;
                conn.execute("ROLLBACK TO s").await?;
                conn.execute("RELEASE s").await?;
                expect(1, "savepoint operations retain one snapshot");
                conn.execute("COMMIT").await?;
                expect(0, "explicit commit releases the snapshot");
                for _ in 0..3 {
                    let prepared = conn.prepare("SELECT id FROM snapshots").await?;
                    assert_eq!(prepared.query().await?[0].values(), &[SqliteValue::Integer(7)]);
                    // Memory reads retain one reusable pager snapshot; file
                    // reads release it so external commits can become visible.
                    expect(i64::from(memory), "completed prepared read snapshot lifetime");
                }
                conn.execute("BEGIN").await?;
                expect(1, "BEGIN replaces a cached snapshot without double counting");
                let other = Connection::open(":memory:").await?;
                other.execute("BEGIN").await?;
                expect(2, "held snapshots aggregate across databases");
                conn.execute("ROLLBACK").await?;
                expect(1, "one database rollback preserves the other's contribution");
                other.close().await?;
                expect(0, "awaited close releases an active snapshot");
                conn.query("SELECT id FROM snapshots").await?;
                expect(i64::from(memory), "autocommit read cache remains counted");
                conn.close().await?;
                expect(0, "close releases a cached snapshot");
                eprintln!("event=public_reader_snapshots_verified mode={mode} memory={memory}");
            }
            let exposition = fsqlite_observability::metrics::render_prometheus();
            if enabled {
                assert!(exposition.lines().any(|line| line == "fsqlite_active_readers 0"));
            } else {
                assert!(exposition.is_empty());
            }
            Ok(())
        }.await;
    });
    outcome
}

#[test]
fn real_schema_metrics_follow_public_cache_invalidations() -> TestResult {
    const MODE: &str = "FSQLITE_SCHEMA_METRICS_TEST_MODE";
    let Ok(mode) = std::env::var(MODE) else {
        for mode in ["enabled", "disabled"] {
            let output = std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", "real_schema_metrics_follow_public_cache_invalidations", "--nocapture"])
                .env(MODE, mode)
                .env("FRANKENSQLITE_METRICS_DISABLE", if mode == "disabled" { "1" } else { "0" })
                .output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("schema_metrics mode={mode}\n{stdout}\n{stderr}");
            assert!(output.status.success(), "isolated {mode} schema keeper failed");
            assert!(stderr.contains("event=public_schema_metrics_verified"));
        }
        return Ok(());
    };
    assert!(matches!(mode.as_str(), "enabled" | "disabled"));
    let enabled = mode == "enabled";
    let counter = &fsqlite_observability::metrics::global().schema_epoch_bumps_total;
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("schema-metrics.db");
            let path = path.to_str().expect("UTF-8 temporary path");
            let conn = Connection::open(path).await?;
            assert!(conn.is_concurrent_mode_default());
            let before = counter.get();
            let delta = |expected| assert_eq!(counter.get() - before, if enabled { expected } else { 0 });
            conn.execute("CREATE TABLE metric_schema(id INTEGER PRIMARY KEY);").await.expect("create schema metric table");
            delta(1);
            conn.execute("INSERT INTO metric_schema VALUES(1);").await.expect("insert schema metric row");
            {
                let prepared = conn.prepare("SELECT * FROM metric_schema;").await?;
                assert_eq!(prepared.query().await?[0].values(), &[SqliteValue::Integer(1)]);
                conn.execute("ALTER TABLE metric_schema ADD COLUMN added TEXT;").await.expect("ALTER after prepared read");
                delta(2);
                assert_eq!(prepared.query().await?[0].values(), &[SqliteValue::Integer(1), SqliteValue::Null]);
            }
            conn.execute("CREATE TABLE IF NOT EXISTS metric_schema(id INTEGER);").await?;
            assert!(conn.execute("CREATE TABLE metric_schema(id INTEGER);").await.is_err());
            conn.query("SELECT * FROM metric_schema;").await?;
            delta(2);
            conn.execute("CREATE INDEX metric_schema_idx ON metric_schema(added);").await.expect("CREATE INDEX after prepared read");
            delta(3);
            conn.execute("CREATE TEMP TABLE metric_temp(value INTEGER);").await.expect("create TEMP metric table");
            delta(4);
            conn.execute("DROP TABLE metric_temp;").await.expect("drop TEMP metric table");
            delta(5);
            conn.execute("BEGIN;").await?;
            conn.execute("CREATE TABLE metric_rolled_back(value INTEGER);").await.expect("create table in explicit transaction");
            delta(6);
            conn.execute("ROLLBACK;").await?;
            assert!(conn.query("SELECT * FROM metric_rolled_back;").await.is_err());
            // The local cache was invalidated even though DDL was rolled back.
            delta(6);
            let peer = Connection::open(path).await?;
            peer.query("SELECT * FROM metric_schema;").await?;
            delta(6);
            conn.execute("CREATE TABLE metric_peer_visible(value INTEGER);").await.expect("create table while peer is open");
            delta(7);
            peer.query("SELECT * FROM metric_peer_visible;").await?;
            delta(7);
            peer.close().await?;
            conn.close().await?;
            delta(7);
            let exposition = fsqlite_observability::metrics::render_prometheus();
            if enabled {
                assert!(exposition.lines().any(|line| line == format!("fsqlite_schema_epoch_bumps_total {}", counter.get())));
            } else {
                assert_eq!(counter.get(), 0);
                assert!(exposition.is_empty());
            }
            eprintln!("event=public_schema_metrics_verified mode={mode} bumps={}", counter.get());
            Ok(())
        }.await;
    });
    outcome
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn real_metrics_http_starts_from_public_connection_open() -> TestResult {
    use std::io::{Read as _, Write as _};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::time::Duration;

    const MODE: &str = "FSQLITE_HTTP_METRICS_TEST_MODE";
    const ADDRESS: &str = "FSQLITE_HTTP_METRICS_TEST_ADDRESS";
    let Ok(mode) = std::env::var(MODE) else {
        for mode in ["enabled", "disabled", "unset", "occupied"] {
            let reservation = TcpListener::bind("127.0.0.1:0")?;
            let address = reservation.local_addr()?.to_string();
            // Keep the occupied-port case reserved for the whole child run.
            // Other modes release this OS-selected port immediately before
            // spawning; any intervening bind collision fails the keeper.
            let reservation = (mode == "occupied").then_some(reservation);
            let mut command = std::process::Command::new(std::env::current_exe()?);
            command
                .args(["--exact", "real_metrics_http_starts_from_public_connection_open", "--nocapture"])
                .env(MODE, mode)
                .env(ADDRESS, &address)
                .env("FRANKENSQLITE_METRICS_DISABLE", if mode == "disabled" { "1" } else { "0" });
            if mode == "unset" {
                command.env_remove("FRANKENSQLITE_METRICS_BIND");
            } else {
                command.env("FRANKENSQLITE_METRICS_BIND", &address);
            }
            let output = command.output()?;
            drop(reservation);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("http_metrics mode={mode}\n{stdout}\n{stderr}");
            assert!(output.status.success(), "isolated {mode} HTTP keeper failed");
            assert!(stderr.contains("event=public_metrics_http_verified"));
        }
        return Ok(());
    };
    assert!(matches!(mode.as_str(), "enabled" | "disabled" | "unset" | "occupied"));
    let address: SocketAddr = std::env::var(ADDRESS)?.parse()?;
    let scrape = || -> TestResult<String> {
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("Content-Type: text/plain; version=0.0.4"));
        Ok(response)
    };
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            assert!(conn.is_concurrent_mode_default());
            let peer = Connection::open(":memory:").await?;
            if mode == "enabled" {
                assert!(scrape()?.lines().any(|line| line == "fsqlite_commits_total 0"));
            }
            conn.execute("CREATE TABLE private_metric_rows(id INTEGER PRIMARY KEY, body TEXT);").await?;
            conn.execute("BEGIN;").await?;
            if mode == "enabled" {
                assert!(scrape()?.lines().any(|line| line == "fsqlite_active_writers 1"));
            }
            conn.execute("INSERT INTO private_metric_rows VALUES(1,'private metric payload');").await?;
            conn.execute("COMMIT;").await?;
            if mode == "enabled" {
                assert!(scrape()?.lines().any(|line| line == "fsqlite_active_writers 0"));
            }
            conn.execute("BEGIN;").await?;
            conn.execute("INSERT INTO private_metric_rows VALUES(2,'rolled back');").await?;
            conn.execute("ROLLBACK;").await?;
            assert_eq!(conn.query("SELECT count(*) FROM private_metric_rows;").await?[0].values(), &[SqliteValue::Integer(1)]);
            assert_eq!(peer.query("SELECT 42;").await?[0].values(), &[SqliteValue::Integer(42)]);
            match mode.as_str() {
                "enabled" => {
                    let response = scrape()?;
                    assert!(response.lines().any(|line| line == "fsqlite_commits_total 2"));
                    assert!(!response.contains("private_metric_rows"));
                    assert!(!response.contains("private metric payload"));
                }
                "disabled" | "unset" => {
                    let unused_port = TcpListener::bind(address)?;
                    assert_eq!(unused_port.local_addr()?, address, "normal opens must not bind");
                }
                "occupied" => {
                    assert!(TcpListener::bind(address).is_err(), "parent still owns the port");
                }
                _ => unreachable!("mode checked before execution"),
            }
            peer.close().await?;
            conn.close().await?;
            eprintln!("bead_id=bd-zywqc.11.1 event=public_metrics_http_verified mode={mode}");
            Ok(())
        }.await;
    });
    outcome
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn metrics_http_pragma_controls_one_live_endpoint() -> TestResult {
    use std::io::{Read as _, Write as _};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::time::Duration;

    const MODE: &str = "FSQLITE_HTTP_PRAGMA_TEST_MODE";
    const ADDRESS: &str = "FSQLITE_HTTP_PRAGMA_TEST_ADDRESS";
    let Ok(mode) = std::env::var(MODE) else {
        for mode in ["manual", "env", "disabled"] {
            let reservation = TcpListener::bind("127.0.0.1:0")?;
            let address = reservation.local_addr()?.to_string();
            let mut command = std::process::Command::new(std::env::current_exe()?);
            command
                .args(["--exact", "metrics_http_pragma_controls_one_live_endpoint", "--nocapture"])
                .env(MODE, mode)
                .env(ADDRESS, &address)
                .env("FRANKENSQLITE_METRICS_DISABLE", if mode == "disabled" { "1" } else { "0" });
            if mode == "manual" {
                command.env_remove("FRANKENSQLITE_METRICS_BIND");
            } else {
                command.env("FRANKENSQLITE_METRICS_BIND", &address);
            }
            drop(reservation);
            let output = command.output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("http_pragma mode={mode}\n{stdout}\n{stderr}");
            assert!(output.status.success(), "isolated {mode} PRAGMA keeper failed");
            assert!(stderr.contains("event=public_metrics_http_pragma_verified"));
        }
        return Ok(());
    };
    assert!(matches!(mode.as_str(), "manual" | "env" | "disabled"));
    let address: SocketAddr = std::env::var(ADDRESS)?.parse()?;
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            assert!(conn.is_concurrent_mode_default());
            let query = "PRAGMA enable_metrics_http;";
            let prepared = conn.prepare(query).await?;
            assert_eq!(prepared.column_names(), &["enable_metrics_http"]);
            let initial = i64::from(mode == "env");
            assert_eq!(conn.query(query).await?[0].values(), &[SqliteValue::Integer(initial)]);
            assert!(conn.execute("PRAGMA enable_metrics_http=2;").await.is_err());
            conn.execute("PRAGMA enable_metrics_http=0;").await?;
            assert_eq!(conn.query(query).await?[0].values(), &[SqliteValue::Integer(initial)]);
            if mode == "manual" {
                let occupied = TcpListener::bind(address)?;
                let error = conn.execute(&format!("PRAGMA enable_metrics_http='{address}';"))
                    .await.expect_err("occupied endpoint must report its bind failure");
                assert!(matches!(error, fsqlite_error::FrankenError::Io(_)));
                assert_eq!(conn.query(query).await?[0].values(), &[SqliteValue::Integer(0)]);
                assert_eq!(conn.query("SELECT 42;").await?[0].values(), &[SqliteValue::Integer(42)]);
                drop(occupied);
                conn.execute(&format!("PRAGMA enable_metrics_http='{address}';")).await?;
            } else {
                conn.execute("PRAGMA enable_metrics_http=1;").await?;
            }
            let enabled = i64::from(mode != "disabled");
            assert_eq!(conn.query(query).await?[0].values(), &[SqliteValue::Integer(enabled)]);
            let peer = Connection::open(":memory:").await?;
            assert_eq!(peer.query(query).await?[0].values(), &[SqliteValue::Integer(enabled)]);
            // A second enable must share the first listener, even when its
            // requested address is occupied by an unrelated real socket.
            let second = TcpListener::bind("127.0.0.1:0")?;
            peer.execute(&format!("PRAGMA enable_metrics_http='{}';", second.local_addr()?)).await?;
            conn.execute("CREATE TABLE http_pragma_rows(id INTEGER);").await?;
            conn.execute("BEGIN;").await?;
            conn.execute("INSERT INTO http_pragma_rows VALUES(7);").await?;
            conn.execute("COMMIT;").await?;
            conn.execute("PRAGMA enable_metrics_http=0;").await?;
            assert_eq!(conn.query(query).await?[0].values(), &[SqliteValue::Integer(enabled)]);
            if enabled == 1 {
                let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
                stream.set_read_timeout(Some(Duration::from_secs(5)))?;
                stream.set_write_timeout(Some(Duration::from_secs(5)))?;
                stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
                let mut response = String::new();
                stream.read_to_string(&mut response)?;
                assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
                assert!(response.lines().any(|line| line == "fsqlite_commits_total 2"));
                assert!(!response.contains("http_pragma_rows"));
            } else {
                let unused = TcpListener::bind(address)?;
                assert_eq!(unused.local_addr()?, address);
            }
            peer.close().await?;
            drop(prepared);
            conn.close().await?;
            eprintln!("bead_id=bd-zywqc.11.1 event=public_metrics_http_pragma_verified mode={mode}");
            Ok(())
        }.await;
    });
    outcome
}

#[test]
fn real_commit_metrics_count_public_durable_outcomes() -> TestResult {
    // The registry is process-wide. Each mode runs in its own process so other
    // tests cannot contaminate exact deltas or the once-read environment flag.
    let Ok(mode) = std::env::var("FSQLITE_COMMIT_METRICS_TEST_MODE") else {
        for mode in ["enabled", "disabled"] {
            let output = std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", "real_commit_metrics_count_public_durable_outcomes", "--nocapture"])
                .env("FSQLITE_COMMIT_METRICS_TEST_MODE", mode)
                .env("FRANKENSQLITE_METRICS_DISABLE", if mode == "disabled" { "1" } else { "0" })
                .output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("commit_metrics mode={mode}\n{stdout}\n{stderr}");
            assert!(output.status.success(), "isolated {mode} keeper failed");
            assert!(stderr.contains("event=public_commit_metrics_verified"), "child must execute the SQL scenarios");
        }
        return Ok(());
    };
    assert!(matches!(mode.as_str(), "enabled" | "disabled"));
    let enabled = mode == "enabled";
    assert_eq!(fsqlite_observability::metrics::metrics_disabled(), !enabled);
    let counter = &fsqlite_observability::metrics::global().commits_total;
    let duration = &fsqlite_observability::metrics::global().commit_duration_seconds;
    let fsync = &fsqlite_observability::metrics::global().fsync_duration_seconds;
    let busy = &fsqlite_observability::metrics::global().conflicts_busy_snapshot_total;
    fsqlite_core::connection::set_hot_path_profile_enabled(false);
    let delta = |before, expected| {
        assert_eq!(counter.get() - before, if enabled { expected } else { 0 });
        assert_eq!(duration.count(), counter.get(), "each completed transaction contributes one latency sample");
    };
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let file = dir.path().join("commit-metrics.db");
            for path in [":memory:", file.to_str().expect("UTF-8 path")] {
                let conn = Connection::open(path).await?;
                assert!(conn.is_concurrent_mode_default());
                conn.query("PRAGMA journal_mode=WAL;").await?;
                conn.execute("PRAGMA synchronous=FULL;").await?;
                conn.execute("PRAGMA fsqlite.autocommit_retain=OFF;").await?;
                conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value INTEGER);").await?;
                for id in 1..=3 {
                    let before = counter.get();
                    let fsync_before = fsync.count();
                    conn.execute("BEGIN;").await?;
                    assert!(conn.is_concurrent_transaction());
                    conn.execute(&format!("INSERT INTO t VALUES({id},10);")).await?;
                    delta(before, 0);
                    conn.execute("COMMIT;").await?;
                    delta(before, 1);
                    if enabled && path != ":memory:" {
                        assert!(fsync.count() > fsync_before, "durable SQL commit records a real WAL barrier");
                    } else if !enabled {
                        assert_eq!(fsync.count(), 0);
                    }
                    assert!(conn.execute("COMMIT;").await.is_err());
                    delta(before, 1);
                }
                let before = counter.get();
                conn.execute("BEGIN;").await?;
                conn.execute("INSERT INTO t VALUES(99,10);").await?;
                conn.execute("ROLLBACK;").await?;
                conn.query("SELECT * FROM t;").await?;
                delta(before, 0);
                // Explicit read-only COMMIT is a transaction completion; an
                // implicit read snapshot is not an autocommit write commit.
                conn.execute("BEGIN;").await?;
                conn.query("SELECT * FROM t;").await?;
                conn.execute("COMMIT;").await?;
                delta(before, 1);
                for id in 4..=6 {
                    let before = counter.get();
                    conn.execute(&format!("INSERT INTO t VALUES({id},10);")).await?;
                    delta(before, 1);
                    assert!(conn.execute(&format!("INSERT INTO t VALUES({id},99);")).await.is_err());
                    delta(before, 1);
                }
                let before = counter.get();
                {
                    let statement = conn.prepare("INSERT INTO t VALUES(?1,10);").await?;
                    delta(before, 0);
                    statement.execute_with_params(&[SqliteValue::Integer(7)]).await?;
                    delta(before, 1);
                    statement.execute_with_params(&[SqliteValue::Integer(8)]).await?;
                    delta(before, 2);
                }
                conn.execute_batch("INSERT INTO t VALUES(9,10); INSERT INTO t VALUES(10,10);").await?;
                delta(before, 4);
                let before = counter.get();
                assert_eq!(conn.query("SELECT count(*),sum(value) FROM t;").await?[0].values(), &[SqliteValue::Integer(10), SqliteValue::Integer(100)]);
                conn.close().await?;
                delta(before, 0);
            }

            // Explicit opt-out exercises the existing private-memory retained
            // path. No default is changed, and parked writes must not count.
            let conn = Connection::open(":memory:").await?;
            assert!(conn.is_concurrent_mode_default());
            conn.execute("PRAGMA fsqlite.concurrent_mode=OFF;").await?;
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY);").await?;
            let before = counter.get();
            conn.execute("INSERT INTO t VALUES(1);").await?;
            conn.execute("INSERT INTO t VALUES(2);").await?;
            delta(before, 0);
            conn.execute("PRAGMA fsqlite.autocommit_retain=OFF;").await?;
            delta(before, 1);
            conn.execute("PRAGMA fsqlite.autocommit_retain=OFF;").await?;
            delta(before, 1);
            conn.execute("INSERT INTO t VALUES(3);").await?;
            delta(before, 2);
            assert_eq!(conn.query("SELECT count(*) FROM t;").await?[0].values(), &[SqliteValue::Integer(3)]);
            conn.close().await?;
            delta(before, 2);

            #[cfg(feature = "ext-fts5")]
            {
                let conn = Connection::open(":memory:").await?;
                conn.execute("PRAGMA fsqlite.autocommit_retain=OFF;").await?;
                let before = counter.get();
                conn.execute("CREATE VIRTUAL TABLE docs USING fts5(body);").await?;
                delta(before, 1);
                conn.execute("INSERT INTO docs VALUES('commit latency');").await?;
                delta(before, 2);
                assert_eq!(conn.query("SELECT count(*) FROM docs WHERE docs MATCH 'latency';").await?[0].values(), &[SqliteValue::Integer(1)]);
                conn.close().await?;
                delta(before, 2);
            }

            // Genuine SSI write skew: the rejected COMMIT is not counted,
            // while the independent survivor publishes exactly once.
            let path = dir.path().join("commit-metrics-ssi.db");
            let path = path.to_str().expect("UTF-8 path");
            let first = Connection::open(path).await?;
            first.query("PRAGMA journal_mode=WAL;").await?;
            for table in ["a", "b"] {
                first.execute(&format!("CREATE TABLE {table}(id INTEGER PRIMARY KEY, value INTEGER);")).await?;
                first.execute(&format!("INSERT INTO {table} VALUES(1,10);")).await?;
            }
            let second = Connection::open(path).await?;
            let before = counter.get();
            first.execute("BEGIN;").await?;
            second.execute("BEGIN;").await?;
            first.query("SELECT value FROM b;").await?;
            second.query("SELECT value FROM a;").await?;
            first.execute("UPDATE a SET value=11;").await?;
            second.execute("UPDATE b SET value=11;").await?;
            let busy_before = busy.get();
            assert!(matches!(first.execute("COMMIT;").await, Err(fsqlite_error::FrankenError::BusySnapshot { .. })));
            assert_eq!(busy.get() - busy_before, u64::from(enabled));
            delta(before, 0);
            first.execute("ROLLBACK;").await?;
            second.execute("COMMIT;").await?;
            delta(before, 1);
            assert_eq!(busy.get() - busy_before, u64::from(enabled), "rollback and survivor must not recount the rejection");
            assert_eq!(first.query("SELECT value FROM a;").await?[0].values(), &[SqliteValue::Integer(10)]);
            assert_eq!(first.query("SELECT value FROM b;").await?[0].values(), &[SqliteValue::Integer(11)]);
            first.close().await?;
            second.close().await?;
            delta(before, 1);
            let text = fsqlite_observability::metrics::render_prometheus();
            if enabled {
                assert!(text.lines().any(|line| line == format!("fsqlite_commits_total {}", counter.get())));
                assert!(duration.sum().is_finite() && duration.sum() > 0.0);
                assert!(text.lines().any(|line| line == format!("fsqlite_conflicts_total{{response=\"busy_snapshot\"}} {}", busy.get())));
                assert!(!text.contains("commit-metrics") && !text.contains("INSERT INTO"));
            } else {
                assert!(text.is_empty());
                assert_eq!(counter.get(), 0);
                assert_eq!(duration.count(), 0);
                assert_eq!(duration.sum(), 0.0);
                assert_eq!(fsync.count(), 0);
                assert_eq!(fsync.sum(), 0.0);
                assert_eq!(busy.get(), 0);
            }
            eprintln!("bead_id=bd-zywqc.11.1.3 event=public_commit_metrics_verified mode={mode} commits={}", counter.get());
            Ok(())
        }.await;
    });
    outcome
}

#[test]
#[cfg(feature = "diagnostic-pragmas")]
fn real_commit_events_identify_validation_publication_and_statement() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("private-commit-evidence.db");
            let conn = Connection::open(path.to_str().unwrap()).await?;
            assert!(conn.is_concurrent_mode_default());
            conn.query("PRAGMA journal_mode=WAL;").await?;
            conn.execute("CREATE TABLE private_rows(id INTEGER PRIMARY KEY, body TEXT);").await?;
            conn.query("PRAGMA fsqlite.commit_reset;").await?;
            let prepared_insert = conn.prepare("INSERT INTO private_rows VALUES(7,'private commit payload');").await?;
            assert!(conn.query("PRAGMA fsqlite.commit_events;").await?.is_empty(), "preparation is not a commit");
            conn.execute("BEGIN;").await?;
            prepared_insert.execute().await?;
            assert!(conn.query("PRAGMA fsqlite.commit_events;").await?.is_empty(), "a write inside an open transaction is not publication");
            conn.execute("COMMIT;").await?;
            let events = rows_to_values(&conn.query("PRAGMA fsqlite.commit_events;").await?);
            assert_eq!(events.len(), 2, "actual default-concurrent write needs validation and publication evidence");
            let allowed = &events[0];
            let published = &events[1];
            assert_eq!(allowed.len(), 11);
            assert_eq!(allowed[10], published[10], "transaction epoch must match across validation and publication");
            assert_eq!(allowed[6], SqliteValue::Text("validation_allowed".into()));
            assert!(matches!(&allowed[7], SqliteValue::Text(reason) if matches!(reason.as_ref(), "ssi_clean" | "fcw_clean")));
            assert_eq!(allowed[8], SqliteValue::Null, "permission to commit is not publication");
            assert_eq!(published[6], SqliteValue::Text("published".into()));
            assert_eq!(published[7], SqliteValue::Text("concurrent_write_committed".into()));
            for column in 1..=5 {
                assert_eq!(allowed[column], published[column], "identity column {column}");
                assert!(matches!(allowed[column], SqliteValue::Integer(_)), "actual identity required");
            }
            let SqliteValue::Integer(first_event) = allowed[0] else { panic!("event identity required") };
            assert_eq!(published[0], SqliteValue::Integer(first_event + 1));
            assert!(matches!((&published[8], &published[5]), (SqliteValue::Integer(commit), SqliteValue::Integer(snapshot)) if commit > snapshot));
            assert!(matches!(&published[9], SqliteValue::Text(backend) if matches!(backend.as_ref(), "unix" | "io_uring" | "windows")));
            assert_eq!(conn.query("SELECT body FROM private_rows WHERE id=7;").await?[0].values(), &[SqliteValue::Text("private commit payload".into())]);
            conn.query("SELECT 'validation_allowed', 'concurrent_write_committed';").await?;
            assert_eq!(rows_to_values(&conn.query("PRAGMA fsqlite.commit_events;").await?), events, "unrelated reads and synthetic labels must not create events");
            let rendered = format!("{events:?}");
            assert!(!rendered.contains("private_rows") && !rendered.contains("private commit") && !rendered.contains("private-commit-evidence"));
            let peer = Connection::open(path.to_str().unwrap()).await?;
            assert!(peer.query("PRAGMA fsqlite.commit_events;").await?.is_empty(), "evidence belongs to its executing connection");

            conn.execute("BEGIN;").await?;
            conn.query("SELECT id FROM private_rows;").await?;
            conn.execute("COMMIT;").await?;
            let after_read = rows_to_values(&conn.query("PRAGMA fsqlite.commit_events;").await?);
            assert_eq!(after_read.len(), 3);
            assert_eq!(after_read[2][6], SqliteValue::Text("read_finished".into()));
            assert_eq!(after_read[2][7], SqliteValue::Text("concurrent_read_finished".into()));
            assert_eq!(after_read[2][8], SqliteValue::Null, "ending a read-only transaction does not publish writes");
            assert_ne!(after_read[2][2], published[2], "later statement must have a new identity");
            assert_ne!(after_read[2][3], published[3], "later transaction must have a new identity");
            conn.begin_transaction().await?;
            conn.commit_transaction().await?;
            let after_api = rows_to_values(&conn.query("PRAGMA fsqlite.commit_events;").await?);
            assert_eq!(after_api.len(), 4);
            assert_eq!(after_api[3][2], SqliteValue::Null, "direct API must not inherit the last SQL statement identity");
            conn.execute("BEGIN;").await?;
            conn.execute("ROLLBACK;").await?;
            assert!(conn.execute("COMMIT;").await.is_err());
            assert_eq!(rows_to_values(&conn.query("PRAGMA fsqlite.commit_events;").await?), after_api, "rollback and a rejected inactive COMMIT cannot become publication");
            eprintln!("bead_id=bd-6hdwo.14 event=public_commit_provenance_verified records={after_api:?}");
            drop(prepared_insert);
            peer.close().await?;
            conn.close().await?;
            let stock = rusqlite::Connection::open(path)?;
            assert_eq!(stock.query_row("SELECT body FROM private_rows WHERE id=7", [], |row| row.get::<_, String>(0))?, "private commit payload");
            assert_eq!(stock.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?, "ok");
            Ok(())
        }.await;
    });
    outcome
}

#[test]
#[cfg(feature = "diagnostic-pragmas")]
fn real_commit_validation_reports_the_begin_time_policy() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            conn.execute("CREATE TABLE policy_rows(id INTEGER PRIMARY KEY, value INTEGER);").await?;
            conn.execute("INSERT INTO policy_rows VALUES(1,0);").await?;
            for (begin_policy, later_policy, expected) in [("OFF", "ON", "fcw_clean"), ("ON", "OFF", "ssi_clean")] {
                conn.execute(&format!("PRAGMA fsqlite.serializable={begin_policy};")).await?;
                conn.query("PRAGMA fsqlite.commit_reset;").await?;
                conn.execute("BEGIN;").await?;
                assert!(conn.is_concurrent_transaction());
                conn.execute("UPDATE policy_rows SET value=value+1 WHERE id=1;").await?;
                conn.execute(&format!("PRAGMA fsqlite.serializable={later_policy};")).await?;
                conn.execute("COMMIT;").await?;
                let events = rows_to_values(&conn.query("PRAGMA fsqlite.commit_events;").await?);
                assert_eq!(events.len(), 2);
                assert_eq!(events[0][6], SqliteValue::Text("validation_allowed".into()));
                assert_eq!(events[0][7], SqliteValue::Text(expected.into()), "diagnostics must identify the preparer selected from BEGIN's policy");
                assert_eq!(events[1][7], SqliteValue::Text("concurrent_write_committed".into()));
            }
            assert_eq!(conn.query("SELECT value FROM policy_rows;").await?[0].values(), &[SqliteValue::Integer(2)]);
            conn.close().await?;
            eprintln!("bead_id=bd-6hdwo.14 event=begin_time_commit_validation_verified fcw_then_ssi=true");
            Ok(())
        }.await;
    });
    outcome
}

#[test]
#[cfg(feature = "diagnostic-pragmas")]
fn real_commit_evidence_is_bounded_and_joint_capture_clears_it() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            for _ in 0..70 {
                conn.execute("BEGIN;").await?;
                conn.execute("COMMIT;").await?;
            }
            let events = rows_to_values(&conn.query("PRAGMA fsqlite.commit_events;").await?);
            assert_eq!(events.len(), 64);
            assert_eq!(events[0][0], SqliteValue::Integer(7));
            assert_eq!(events[63][0], SqliteValue::Integer(70));
            assert!(events.iter().all(|row| row[7] == SqliteValue::Text("concurrent_read_finished".into())));
            assert!(events.iter().all(|row| row[6] == SqliteValue::Text("read_finished".into()) && row[8] == SqliteValue::Null));
            let stats = conn.query("PRAGMA fsqlite.commit_stats;").await?;
            for (key, expected) in [("capacity", 64), ("retained_events", 64), ("dropped_events", 6)] {
                assert!(stats.iter().any(|row| matches!(row.values(), [SqliteValue::Text(name), SqliteValue::Integer(value)] if name.as_ref() == key && *value == expected)), "incorrect {key}: {stats:?}");
            }
            conn.query("WITH c(v) AS (SELECT 7) SELECT v FROM c;").await?;
            assert!(!conn.query("PRAGMA fsqlite.fallback_events;").await?.is_empty());
            conn.execute("PRAGMA fsqlite.diagnostic_capture=OFF;").await?;
            assert_eq!(conn.query("PRAGMA fsqlite.diagnostic_capture;").await?[0].values(), &[SqliteValue::Integer(0), SqliteValue::Integer(0), SqliteValue::Integer(0)]);
            assert!(conn.query("PRAGMA fsqlite.commit_events;").await?.is_empty());
            assert!(conn.query("PRAGMA fsqlite.fallback_events;").await?.is_empty());
            assert!(conn.ssi_decisions_snapshot().is_empty());
            conn.execute("BEGIN;").await?;
            assert_eq!(conn.query("WITH c(v) AS (SELECT 8) SELECT v FROM c;").await?[0].values(), &[SqliteValue::Integer(8)]);
            conn.execute("COMMIT;").await?;
            assert!(conn.query("PRAGMA fsqlite.commit_events;").await?.is_empty());
            assert!(conn.query("PRAGMA fsqlite.fallback_events;").await?.is_empty());
            assert!(conn.ssi_decisions_snapshot().is_empty());
            assert!(conn.execute("PRAGMA fsqlite.diagnostic_capture='invalid';").await.is_err());
            assert_eq!(conn.query("PRAGMA fsqlite.diagnostic_capture;").await?[0].values(), &[SqliteValue::Integer(0), SqliteValue::Integer(0), SqliteValue::Integer(0)]);
            conn.execute("PRAGMA fsqlite.diagnostic_capture=ON;").await?;
            assert!(conn.query("PRAGMA fsqlite.commit_events;").await?.is_empty(), "enabling cannot resurrect old evidence");
            conn.execute("BEGIN;").await?;
            conn.execute("COMMIT;").await?;
            let resumed = rows_to_values(&conn.query("PRAGMA fsqlite.commit_events;").await?);
            assert_eq!(resumed.len(), 1);
            assert_eq!(resumed[0][0], SqliteValue::Integer(71));
            conn.query("PRAGMA fsqlite.commit_reset;").await?;
            conn.execute("BEGIN;").await?;
            conn.execute("COMMIT;").await?;
            assert_eq!(conn.query("PRAGMA fsqlite.commit_events;").await?[0].values()[0], SqliteValue::Integer(72));
            eprintln!("bead_id=bd-6hdwo.14 event=public_commit_capture_bound_verified retained=64 dropped=6 disabled_events=0 resumed_id=71 reset_id=72");
            conn.close().await?;
            Ok(())
        }.await;
    });
    outcome
}

#[test]
#[cfg(feature = "diagnostic-pragmas")]
fn real_ssi_evidence_capture_preserves_validation_and_reports_retention() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("ssi-retention.db");
            let path = path.to_str().expect("temporary path is UTF-8");
            let monitor = Connection::open(path).await?;
            monitor.query("PRAGMA journal_mode=WAL;").await?;
            monitor.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, value INTEGER);").await?;
            monitor.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, value INTEGER);").await?;
            monitor.execute("INSERT INTO a VALUES(1,10);").await?;
            monitor.execute("INSERT INTO b VALUES(1,20);").await?;
            let first = Connection::open(path).await?;
            let second = Connection::open(path).await?;
            assert!(first.is_concurrent_mode_default() && second.is_concurrent_mode_default());
            let mut previous_txn = None;
            for (round, capture) in [true, false, true].into_iter().enumerate() {
                first.execute(if capture {
                    "PRAGMA fsqlite.diagnostic_capture=ON;"
                } else {
                    "PRAGMA fsqlite.diagnostic_capture=OFF;"
                }).await?;
                let enabled = first.query("PRAGMA ssi_evidence_capture;").await?;
                assert_eq!(enabled[0].values(), &[SqliteValue::Integer(i64::from(capture))]);
                assert!(first.ssi_decisions_snapshot().is_empty(), "disable clears the previous round's evidence");
                monitor.query("PRAGMA fsqlite.conflict_reset;").await?;
                first.execute("BEGIN;").await?;
                second.execute("BEGIN;").await?;
                first.query("SELECT value FROM b WHERE id=1;").await?;
                second.query("SELECT value FROM a WHERE id=1;").await?;
                first.execute("UPDATE a SET value=11 WHERE id=1;").await?;
                second.execute("UPDATE b SET value=value+1 WHERE id=1;").await?;
                let error = first.execute("COMMIT;").await.expect_err("capture must not affect SSI rejection");
                assert!(matches!(error, fsqlite_error::FrankenError::BusySnapshot { .. }));
                let cards = first.ssi_decisions_snapshot();
                assert_eq!(cards.len(), usize::from(capture));
                let commits = rows_to_values(&first.query("PRAGMA fsqlite.commit_events;").await?);
                assert_eq!(commits.len(), usize::from(capture), "capture must govern actual rejected commit observations");
                if let Some(card) = cards.first() {
                    assert_ne!(previous_txn, Some(card.txn));
                    previous_txn = Some(card.txn);
                    assert_eq!(commits[0][3], SqliteValue::Integer(i64::try_from(card.txn.id.get())?));
                    assert_eq!(commits[0][10], SqliteValue::Integer(i64::from(card.txn.epoch.get())));
                    assert_eq!(commits[0][6], SqliteValue::Text("validation_rejected".into()));
                    assert_eq!(commits[0][7], SqliteValue::Text("ssi_pivot".into()));
                    assert_eq!(commits[0][8], SqliteValue::Null, "rejection cannot claim publication");
                }
                let stats = first.query("PRAGMA fsqlite.ssi_evidence_stats;").await?;
                for (key, expected) in [("capture_enabled", i64::from(capture)), ("capacity", 4096), ("max_payload_bytes", 65_536), ("retained", i64::from(capture)), ("pending", 0), ("pending_dropped", 0), ("oversized_dropped", 0), ("retained_evicted", 0)] {
                    assert!(stats.iter().any(|row| matches!(row.values(), [SqliteValue::Text(name), SqliteValue::Integer(value)] if name.as_ref() == key && *value == expected)), "incorrect {key} stats: {stats:?}");
                }
                assert_eq!(monitor.query("PRAGMA fsqlite.conflict_log;").await?.len(), 1, "shared conflict diagnostics are independent of connection-local SSI evidence capture");
                first.execute("ROLLBACK;").await?;
                second.execute("COMMIT;").await?;
                assert_eq!(monitor.query("SELECT value FROM a WHERE id=1;").await?[0].values(), &[SqliteValue::Integer(10)]);
                assert_eq!(monitor.query("SELECT value FROM b WHERE id=1;").await?[0].values(), &[SqliteValue::Integer(21 + i64::try_from(round)?) ]);
                assert!(monitor.ssi_decisions_snapshot().is_empty(), "other connections do not inherit decision cards");
            }
            let before = first.ssi_evidence_retention_snapshot();
            assert!(first.execute("PRAGMA ssi_evidence_capture='invalid';").await.is_err());
            assert_eq!(first.ssi_evidence_retention_snapshot(), before, "invalid control does not mutate evidence");
            eprintln!("bead_id=bd-6hdwo.14 event=public_ssi_retention_capture_verified stats={before:?}");
            first.close().await?;
            second.close().await?;
            monitor.close().await?;
            Ok(())
        }.await;
    });
    outcome
}

#[test]
#[cfg(feature = "diagnostic-pragmas")]
fn real_ssi_early_abort_reports_unmeasured_edges() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("early-ssi.db");
            let path = path.to_str().expect("temporary path is UTF-8");
            let monitor = Connection::open(path).await?;
            monitor.query("PRAGMA journal_mode=WAL;").await?;
            for table in ["a", "c", "d", "e"] {
                monitor.execute(&format!("CREATE TABLE {table}(id INTEGER PRIMARY KEY, value INTEGER);")).await?;
                monitor.execute(&format!("INSERT INTO {table} VALUES(1,10);")).await?;
            }
            let pivot = Connection::open(path).await?;
            let upstream = Connection::open(path).await?;
            let downstream = Connection::open(path).await?;
            let mut overlapping = Vec::new();
            for _ in 0..32 {
                let connection = Connection::open(path).await?;
                connection.execute("BEGIN;").await?;
                overlapping.push(connection);
            }
            monitor.query("PRAGMA fsqlite.conflict_reset;").await?;
            for (connection, read, write) in [(&pivot, "a", "c"), (&upstream, "e", "a"), (&downstream, "c", "d")] {
                assert!(connection.is_concurrent_mode_default());
                connection.execute("BEGIN;").await?;
                connection.query(&format!("SELECT value FROM {read} WHERE id=1;")).await?;
                connection.execute(&format!("UPDATE {write} SET value=11 WHERE id=1;")).await?;
            }
            downstream.execute("COMMIT;").await?;
            upstream.execute("COMMIT;").await?;
            assert!(monitor.query("PRAGMA fsqlite.conflict_log;").await?.is_empty());
            let error = pivot.execute("COMMIT;").await.expect_err("propagated pivot abort is enforced");
            assert!(matches!(error, fsqlite_error::FrankenError::BusySnapshot { .. }));
            let cards = pivot.ssi_decisions_snapshot();
            assert_eq!(cards.len(), 1);
            let events = monitor.query("PRAGMA fsqlite.conflict_log;").await?;
            assert_eq!(events.len(), 1);
            let SqliteValue::Text(event) = &events[0].values()[2] else {
                panic!("event text required");
            };
            assert!(event.contains("reason: MarkedForAbort"), "actual early path required: {event}");
            assert!(event.contains("in_edge_count: None") && event.contains("out_edge_count: None"));
            assert!(event.contains(&format!("txn: {:?}", cards[0].txn)));
            for (table, expected) in [("a", 11), ("c", 10), ("d", 11), ("e", 10)] {
                assert_eq!(monitor.query(&format!("SELECT value FROM {table} WHERE id=1;")).await?[0].values(), &[SqliteValue::Integer(expected)]);
            }
            pivot.execute("ROLLBACK;").await?;
            for connection in overlapping {
                connection.execute("ROLLBACK;").await?;
                connection.close().await?;
            }
            assert_eq!(rows_to_values(&monitor.query("PRAGMA fsqlite.conflict_log;").await?), rows_to_values(&events));
            eprintln!("bead_id=bd-6hdwo.14 event=public_early_ssi_abort_verified events={:?}", rows_to_values(&events));
            pivot.close().await?;
            upstream.close().await?;
            downstream.close().await?;
            monitor.close().await?;
            Ok(())
        }.await;
    });
    outcome
}

#[test]
#[cfg(feature = "diagnostic-pragmas")]
fn real_ssi_abort_reaches_shared_conflict_pragmas() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("actual-ssi-diagnostics.db");
            let path = path.to_str().expect("temporary path is UTF-8");
            let monitor = Connection::open(path).await?;
            let mode = monitor.query("PRAGMA journal_mode=WAL;").await?;
            assert_eq!(mode[0].values(), &[SqliteValue::Text("wal".into())]);
            monitor.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, value INTEGER);").await?;
            monitor.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, value INTEGER);").await?;
            monitor.execute("INSERT INTO a VALUES(1,10);").await?;
            monitor.execute("INSERT INTO b VALUES(1,20);").await?;

            // Ordinary contract rows cannot manufacture a real engine event.
            monitor.execute("CREATE TABLE fsqlite_explain_concurrency_contract(event TEXT);").await?;
            monitor.execute("INSERT INTO fsqlite_explain_concurrency_contract VALUES('SsiAbort Pivot');").await?;
            monitor.query("PRAGMA fsqlite.conflict_reset;").await?;
            assert!(monitor.query("PRAGMA fsqlite.conflict_log;").await?.is_empty());

            let first = Connection::open(path).await?;
            let second = Connection::open(path).await?;
            assert!(first.is_concurrent_mode_default() && second.is_concurrent_mode_default());
            first.execute("BEGIN;").await?;
            second.execute("BEGIN;").await?;
            assert!(first.is_concurrent_transaction() && second.is_concurrent_transaction());
            assert_eq!(first.query("SELECT value FROM b WHERE id=1;").await?[0].values(), &[SqliteValue::Integer(20)]);
            assert_eq!(second.query("SELECT value FROM a WHERE id=1;").await?[0].values(), &[SqliteValue::Integer(10)]);
            first.execute("UPDATE a SET value=11 WHERE id=1;").await?;
            second.execute("UPDATE b SET value=21 WHERE id=1;").await?;
            assert!(monitor.query("PRAGMA fsqlite.conflict_log;").await?.is_empty());
            let error = first.execute("COMMIT;").await.expect_err("real write skew must be rejected");
            assert!(matches!(error, fsqlite_error::FrankenError::BusySnapshot { .. }), "unexpected rejection: {error:?}");

            let cards = first.ssi_decisions_snapshot();
            assert_eq!(cards.len(), 1, "one real rejection creates one SSI decision");
            let events = monitor.query("PRAGMA fsqlite.conflict_log;").await?;
            assert_eq!(events.len(), 1, "the other connection observes exactly one abort");
            let SqliteValue::Text(event) = &events[0].values()[2] else {
                panic!("conflict log event must be text: {:?}", events[0].values());
            };
            assert!(event.contains("SsiAbort") && event.contains("reason: Pivot"));
            assert!(event.contains("in_edge_count: Some(1),") && event.contains("out_edge_count: Some(1),"));
            assert!(event.contains(&format!("txn: {:?}", cards[0].txn)), "event and actual decision must identify the same transaction");
            let stats = monitor.query("PRAGMA fsqlite.conflict_stats;").await?;
            for (name, expected) in [("conflicts_total", 1), ("ssi_aborts", 1), ("fcw_drifts", 0), ("page_contentions", 0), ("conflicts_resolved", 0)] {
                assert!(stats.iter().any(|row| matches!(row.values(), [SqliteValue::Text(key), SqliteValue::Integer(value)] if key.as_ref() == name && *value == expected)), "missing exact metric {name}={expected}: {stats:?}");
            }

            second.execute("COMMIT;").await?;
            assert_eq!(monitor.query("SELECT value FROM a WHERE id=1;").await?[0].values(), &[SqliteValue::Integer(10)]);
            assert_eq!(monitor.query("SELECT value FROM b WHERE id=1;").await?[0].values(), &[SqliteValue::Integer(21)]);
            assert_eq!(rows_to_values(&monitor.query("PRAGMA fsqlite.conflict_log;").await?), rows_to_values(&events), "clean commit and reads add no conflict or resolution");
            eprintln!("bead_id=bd-6hdwo.14 event=public_ssi_conflict_observed txn={:?} events={:?} stats={:?}", cards[0].txn, rows_to_values(&events), rows_to_values(&stats));
            monitor.query("PRAGMA fsqlite.conflict_reset;").await?;
            assert!(first.query("PRAGMA fsqlite.conflict_log;").await?.is_empty(), "reset applies to the shared observer");
            first.close().await?;
            second.close().await?;
            monitor.close().await?;
            Ok(())
        }
        .await;
    });
    outcome
}

struct ConcurrencyDiagnostic<'a> {
    event_seq: i64,
    diagnostic_surface: &'a str,
    statement_fingerprint: &'a str,
    plan_id: &'a str,
    table_name: &'a str,
    index_name: Option<&'a str>,
    range_id: Option<&'a str>,
    queue_name: Option<&'a str>,
    lease_key: Option<&'a str>,
    worker_id: &'a str,
    transaction_id: &'a str,
    hotspot_kind: &'a str,
    page_number: Option<i64>,
    page_start: Option<i64>,
    page_end: Option<i64>,
    predicted_conflict_count: i64,
    observed_conflict_count: i64,
    retry_count: i64,
    abort_count: i64,
    busy_family: &'a str,
    conflict_reason: Option<&'a str>,
    fallback_reason: Option<&'a str>,
    external_wait: Option<&'a str>,
    coordination_strategy: &'a str,
    diagnostics_available: bool,
    suggested_next_inspection: &'a str,
    first_failure_diag: &'a str,
}

fn sql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_optional_text(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_owned(), sql_text)
}

fn sql_optional_i64(value: Option<i64>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |number| number.to_string())
}

fn sql_bool(value: bool) -> &'static str {
    if value { "1" } else { "0" }
}

fn row_values(row: &Row) -> Vec<SqliteValue> {
    row.values().to_vec()
}

fn rows_to_values(rows: &[Row]) -> Vec<Vec<SqliteValue>> {
    rows.iter().map(row_values).collect()
}

async fn install_concurrency_schema(conn: &Connection) -> TestResult {
    conn.execute(
        "CREATE TABLE fsqlite_concurrency_reason_codes_contract(
            reason_code TEXT NOT NULL PRIMARY KEY,
            hotspot_kind TEXT NOT NULL,
            severity TEXT NOT NULL,
            retryable INTEGER NOT NULL,
            contention_family TEXT NOT NULL,
            concurrency_impact TEXT NOT NULL,
            suggested_next_inspection TEXT NOT NULL,
            human_text TEXT NOT NULL
        );",
    )
    .await?;
    conn.execute(
        "CREATE TABLE fsqlite_explain_concurrency_contract(
            event_seq INTEGER NOT NULL PRIMARY KEY,
            event_ts_ms INTEGER NOT NULL,
            diagnostic_surface TEXT NOT NULL,
            trace_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            scenario_id TEXT NOT NULL,
            statement_fingerprint TEXT NOT NULL,
            plan_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            index_name TEXT,
            range_id TEXT,
            queue_name TEXT,
            lease_key TEXT,
            worker_id TEXT NOT NULL,
            transaction_id TEXT NOT NULL,
            hotspot_kind TEXT NOT NULL,
            page_number INTEGER,
            page_start INTEGER,
            page_end INTEGER,
            predicted_conflict_count INTEGER NOT NULL,
            observed_conflict_count INTEGER NOT NULL,
            retry_count INTEGER NOT NULL,
            abort_count INTEGER NOT NULL,
            busy_family TEXT NOT NULL,
            conflict_reason TEXT,
            fallback_reason TEXT,
            external_wait TEXT,
            coordination_strategy TEXT NOT NULL,
            diagnostics_available INTEGER NOT NULL,
            suggested_next_inspection TEXT NOT NULL,
            first_failure_diag TEXT NOT NULL
        );",
    )
    .await?;
    conn.execute(
        "CREATE INDEX idx_fsqlite_explain_concurrency_contract_join
            ON fsqlite_explain_concurrency_contract(
                statement_fingerprint,
                plan_id,
                table_name,
                hotspot_kind
            );",
    )
    .await?;
    seed_reason_codes(conn).await?;
    Ok(())
}

async fn seed_reason_codes(conn: &Connection) -> TestResult {
    conn.execute(
        "INSERT INTO fsqlite_concurrency_reason_codes_contract(
            reason_code,
            hotspot_kind,
            severity,
            retryable,
            contention_family,
            concurrency_impact,
            suggested_next_inspection,
            human_text
        ) VALUES
            (
                'ok_low_conflict',
                'none',
                'notice',
                0,
                'page_mvcc',
                'lock_free_expected',
                'none',
                'Statement is expected to stay on the low-conflict MVCC path.'
            ),
            (
                'hot_page_predicted',
                'page',
                'warning',
                1,
                'page_mvcc',
                'retry_may_increase',
                'inspect_page_heat',
                'Planner and observed writes point at a hot page.'
            ),
            (
                'coordination_wait',
                'coordination',
                'notice',
                1,
                'queue_lease_range',
                'ownership_wait_only',
                'inspect_coordination_owner',
                'Queue, lease, or range ownership explains the wait.'
            ),
            (
                'compatibility_fallback',
                'fallback',
                'warning',
                0,
                'compatibility',
                'may_reduce_parallelism',
                'inspect_fallback_reason',
                'Statement uses a compatibility-backed path.'
            ),
            (
                'external_resource_wait',
                'external',
                'warning',
                1,
                'external',
                'outside_mvcc',
                'inspect_vfs_or_runtime_wait',
                'The wait is outside page-level MVCC coordination.'
            ),
            (
                'diagnostics_unavailable',
                'unknown',
                'warning',
                1,
                'unknown',
                'unknown',
                'inspect_first_failure_diag',
                'Concurrency diagnostics were unavailable.'
            );",
    )
    .await?;
    Ok(())
}

async fn record_diagnostic(conn: &Connection, diagnostic: ConcurrencyDiagnostic<'_>) -> TestResult {
    conn.execute(&format!(
        "INSERT INTO fsqlite_explain_concurrency_contract(
            event_seq,
            event_ts_ms,
            diagnostic_surface,
            trace_id,
            run_id,
            scenario_id,
            statement_fingerprint,
            plan_id,
            table_name,
            index_name,
            range_id,
            queue_name,
            lease_key,
            worker_id,
            transaction_id,
            hotspot_kind,
            page_number,
            page_start,
            page_end,
            predicted_conflict_count,
            observed_conflict_count,
            retry_count,
            abort_count,
            busy_family,
            conflict_reason,
            fallback_reason,
            external_wait,
            coordination_strategy,
            diagnostics_available,
            suggested_next_inspection,
            first_failure_diag
        ) VALUES (
            {event_seq},
            {event_ts_ms},
            {diagnostic_surface},
            'trace-explain-concurrency-contract',
            'run-explain-concurrency-contract',
            'scenario-agent-swarm-concurrency',
            {statement_fingerprint},
            {plan_id},
            {table_name},
            {index_name},
            {range_id},
            {queue_name},
            {lease_key},
            {worker_id},
            {transaction_id},
            {hotspot_kind},
            {page_number},
            {page_start},
            {page_end},
            {predicted_conflict_count},
            {observed_conflict_count},
            {retry_count},
            {abort_count},
            {busy_family},
            {conflict_reason},
            {fallback_reason},
            {external_wait},
            {coordination_strategy},
            {diagnostics_available},
            {suggested_next_inspection},
            {first_failure_diag}
        );",
        event_seq = diagnostic.event_seq,
        event_ts_ms = 2_000 + diagnostic.event_seq,
        diagnostic_surface = sql_text(diagnostic.diagnostic_surface),
        statement_fingerprint = sql_text(diagnostic.statement_fingerprint),
        plan_id = sql_text(diagnostic.plan_id),
        table_name = sql_text(diagnostic.table_name),
        index_name = sql_optional_text(diagnostic.index_name),
        range_id = sql_optional_text(diagnostic.range_id),
        queue_name = sql_optional_text(diagnostic.queue_name),
        lease_key = sql_optional_text(diagnostic.lease_key),
        worker_id = sql_text(diagnostic.worker_id),
        transaction_id = sql_text(diagnostic.transaction_id),
        hotspot_kind = sql_text(diagnostic.hotspot_kind),
        page_number = sql_optional_i64(diagnostic.page_number),
        page_start = sql_optional_i64(diagnostic.page_start),
        page_end = sql_optional_i64(diagnostic.page_end),
        predicted_conflict_count = diagnostic.predicted_conflict_count,
        observed_conflict_count = diagnostic.observed_conflict_count,
        retry_count = diagnostic.retry_count,
        abort_count = diagnostic.abort_count,
        busy_family = sql_text(diagnostic.busy_family),
        conflict_reason = sql_optional_text(diagnostic.conflict_reason),
        fallback_reason = sql_optional_text(diagnostic.fallback_reason),
        external_wait = sql_optional_text(diagnostic.external_wait),
        coordination_strategy = sql_text(diagnostic.coordination_strategy),
        diagnostics_available = sql_bool(diagnostic.diagnostics_available),
        suggested_next_inspection = sql_text(diagnostic.suggested_next_inspection),
        first_failure_diag = sql_text(diagnostic.first_failure_diag),
    ))
    .await?;
    trace_concurrency_diagnostic(&diagnostic);
    Ok(())
}

fn trace_concurrency_diagnostic(diagnostic: &ConcurrencyDiagnostic<'_>) {
    tracing::info!(
        target: "fsqlite.explain_concurrency_contract",
        diagnostic_surface = diagnostic.diagnostic_surface,
        statement_fingerprint = diagnostic.statement_fingerprint,
        plan_id = diagnostic.plan_id,
        table_name = diagnostic.table_name,
        index_name = diagnostic.index_name.unwrap_or("none"),
        range_id = diagnostic.range_id.unwrap_or("none"),
        queue_name = diagnostic.queue_name.unwrap_or("none"),
        lease_key = diagnostic.lease_key.unwrap_or("none"),
        worker_id = diagnostic.worker_id,
        transaction_id = diagnostic.transaction_id,
        hotspot_kind = diagnostic.hotspot_kind,
        page_number = diagnostic.page_number.unwrap_or(-1),
        predicted_conflict_count = diagnostic.predicted_conflict_count,
        observed_conflict_count = diagnostic.observed_conflict_count,
        retry_count = diagnostic.retry_count,
        abort_count = diagnostic.abort_count,
        busy_family = diagnostic.busy_family,
        conflict_reason = diagnostic.conflict_reason.unwrap_or("none"),
        fallback_reason = diagnostic.fallback_reason.unwrap_or("none"),
        external_wait = diagnostic.external_wait.unwrap_or("none"),
        coordination_strategy = diagnostic.coordination_strategy,
        diagnostics_available = diagnostic.diagnostics_available,
        suggested_next_inspection = diagnostic.suggested_next_inspection,
        first_failure_diag = diagnostic.first_failure_diag,
    );
}

async fn seed_diagnostic_examples(conn: &Connection) -> TestResult {
    let examples = [
        ConcurrencyDiagnostic {
            event_seq: 1,
            diagnostic_surface: "EXPLAIN CONCURRENCY",
            statement_fingerprint: "fp-low-conflict-insert",
            plan_id: "plan-insert-disjoint-leaf",
            table_name: "jobs",
            index_name: Some("sqlite_autoindex_jobs_1"),
            range_id: Some("range-worker-a"),
            queue_name: None,
            lease_key: None,
            worker_id: "worker-a",
            transaction_id: "txn-100",
            hotspot_kind: "none",
            page_number: Some(40),
            page_start: Some(40),
            page_end: Some(40),
            predicted_conflict_count: 0,
            observed_conflict_count: 0,
            retry_count: 0,
            abort_count: 0,
            busy_family: "none",
            conflict_reason: Some("ok_low_conflict"),
            fallback_reason: None,
            external_wait: None,
            coordination_strategy: "page_mvcc",
            diagnostics_available: true,
            suggested_next_inspection: "none",
            first_failure_diag: "none",
        },
        ConcurrencyDiagnostic {
            event_seq: 2,
            diagnostic_surface: "EXPLAIN CONCURRENCY",
            statement_fingerprint: "fp-hot-page-update",
            plan_id: "plan-update-leaf-47",
            table_name: "jobs",
            index_name: None,
            range_id: None,
            queue_name: None,
            lease_key: None,
            worker_id: "worker-b",
            transaction_id: "txn-101",
            hotspot_kind: "page",
            page_number: Some(47),
            page_start: Some(47),
            page_end: Some(47),
            predicted_conflict_count: 3,
            observed_conflict_count: 2,
            retry_count: 1,
            abort_count: 1,
            busy_family: "busy_snapshot",
            conflict_reason: Some("hot_page_predicted"),
            fallback_reason: None,
            external_wait: None,
            coordination_strategy: "page_mvcc",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_page_heat",
            first_failure_diag: "same page had two concurrent writers",
        },
        ConcurrencyDiagnostic {
            event_seq: 3,
            diagnostic_surface: "PRAGMA fsqlite.concurrency_status",
            statement_fingerprint: "fp-queue-claim",
            plan_id: "plan-queue-claim",
            table_name: "fsqlite_queue",
            index_name: Some("idx_fsqlite_queue_claim"),
            range_id: None,
            queue_name: Some("replay-work"),
            lease_key: None,
            worker_id: "worker-c",
            transaction_id: "txn-102",
            hotspot_kind: "coordination",
            page_number: None,
            page_start: None,
            page_end: None,
            predicted_conflict_count: 1,
            observed_conflict_count: 1,
            retry_count: 1,
            abort_count: 0,
            busy_family: "busy",
            conflict_reason: Some("coordination_wait"),
            fallback_reason: None,
            external_wait: None,
            coordination_strategy: "coordination_row",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_coordination_owner",
            first_failure_diag: "queue item already claimed by worker-a",
        },
        ConcurrencyDiagnostic {
            event_seq: 4,
            diagnostic_surface: "PRAGMA fsqlite.concurrency_status",
            statement_fingerprint: "fp-lease-renew",
            plan_id: "plan-lease-renew",
            table_name: "fsqlite_lease",
            index_name: Some("sqlite_autoindex_fsqlite_lease_1"),
            range_id: None,
            queue_name: None,
            lease_key: Some("writer-slo-budget"),
            worker_id: "worker-d",
            transaction_id: "txn-103",
            hotspot_kind: "coordination",
            page_number: None,
            page_start: None,
            page_end: None,
            predicted_conflict_count: 1,
            observed_conflict_count: 1,
            retry_count: 0,
            abort_count: 0,
            busy_family: "none",
            conflict_reason: Some("coordination_wait"),
            fallback_reason: None,
            external_wait: None,
            coordination_strategy: "coordination_row",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_coordination_owner",
            first_failure_diag: "lease owner token mismatch",
        },
        ConcurrencyDiagnostic {
            event_seq: 5,
            diagnostic_surface: "EXPLAIN CONCURRENCY",
            statement_fingerprint: "fp-range-backfill",
            plan_id: "plan-range-disjoint",
            table_name: "events",
            index_name: Some("events_rowid"),
            range_id: Some("range-worker-e"),
            queue_name: None,
            lease_key: None,
            worker_id: "worker-e",
            transaction_id: "txn-104",
            hotspot_kind: "range",
            page_number: None,
            page_start: Some(100),
            page_end: Some(108),
            predicted_conflict_count: 0,
            observed_conflict_count: 0,
            retry_count: 0,
            abort_count: 0,
            busy_family: "none",
            conflict_reason: Some("ok_low_conflict"),
            fallback_reason: None,
            external_wait: None,
            coordination_strategy: "page_mvcc",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_range_balance",
            first_failure_diag: "none",
        },
        ConcurrencyDiagnostic {
            event_seq: 6,
            diagnostic_surface: "EXPLAIN CONCURRENCY",
            statement_fingerprint: "fp-window-fallback",
            plan_id: "plan-window-compat",
            table_name: "events",
            index_name: None,
            range_id: None,
            queue_name: None,
            lease_key: None,
            worker_id: "worker-f",
            transaction_id: "txn-105",
            hotspot_kind: "fallback",
            page_number: None,
            page_start: None,
            page_end: None,
            predicted_conflict_count: 0,
            observed_conflict_count: 0,
            retry_count: 0,
            abort_count: 0,
            busy_family: "none",
            conflict_reason: None,
            fallback_reason: Some("compatibility_fallback"),
            external_wait: None,
            coordination_strategy: "diagnostic_only",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_fallback_reason",
            first_failure_diag: "window frame still uses compatibility path",
        },
        ConcurrencyDiagnostic {
            event_seq: 7,
            diagnostic_surface: "PRAGMA fsqlite.concurrency_status",
            statement_fingerprint: "fp-vfs-wait",
            plan_id: "plan-checkpoint",
            table_name: "events",
            index_name: None,
            range_id: None,
            queue_name: None,
            lease_key: None,
            worker_id: "worker-g",
            transaction_id: "txn-106",
            hotspot_kind: "external",
            page_number: None,
            page_start: None,
            page_end: None,
            predicted_conflict_count: 0,
            observed_conflict_count: 0,
            retry_count: 2,
            abort_count: 0,
            busy_family: "busy_recovery",
            conflict_reason: Some("external_resource_wait"),
            fallback_reason: None,
            external_wait: Some("checkpoint_backpressure"),
            coordination_strategy: "external_wait",
            diagnostics_available: true,
            suggested_next_inspection: "inspect_vfs_or_runtime_wait",
            first_failure_diag: "checkpoint backpressure delayed publication",
        },
    ];

    for example in examples {
        record_diagnostic(conn, example).await?;
    }
    Ok(())
}

#[test]
fn reason_codes_are_stable_and_dimensioned() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            install_concurrency_schema(&conn).await?;

            let rows = conn
                .query(
                    "SELECT reason_code,
                hotspot_kind,
                retryable,
                contention_family,
                concurrency_impact,
                suggested_next_inspection
           FROM fsqlite_concurrency_reason_codes_contract
          ORDER BY reason_code;",
                )
                .await?;

            assert_eq!(
                rows_to_values(&rows),
                vec![
                    vec![
                        SqliteValue::Text("compatibility_fallback".into()),
                        SqliteValue::Text("fallback".into()),
                        SqliteValue::Integer(0),
                        SqliteValue::Text("compatibility".into()),
                        SqliteValue::Text("may_reduce_parallelism".into()),
                        SqliteValue::Text("inspect_fallback_reason".into()),
                    ],
                    vec![
                        SqliteValue::Text("coordination_wait".into()),
                        SqliteValue::Text("coordination".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("queue_lease_range".into()),
                        SqliteValue::Text("ownership_wait_only".into()),
                        SqliteValue::Text("inspect_coordination_owner".into()),
                    ],
                    vec![
                        SqliteValue::Text("diagnostics_unavailable".into()),
                        SqliteValue::Text("unknown".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("unknown".into()),
                        SqliteValue::Text("unknown".into()),
                        SqliteValue::Text("inspect_first_failure_diag".into()),
                    ],
                    vec![
                        SqliteValue::Text("external_resource_wait".into()),
                        SqliteValue::Text("external".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("external".into()),
                        SqliteValue::Text("outside_mvcc".into()),
                        SqliteValue::Text("inspect_vfs_or_runtime_wait".into()),
                    ],
                    vec![
                        SqliteValue::Text("hot_page_predicted".into()),
                        SqliteValue::Text("page".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Text("page_mvcc".into()),
                        SqliteValue::Text("retry_may_increase".into()),
                        SqliteValue::Text("inspect_page_heat".into()),
                    ],
                    vec![
                        SqliteValue::Text("ok_low_conflict".into()),
                        SqliteValue::Text("none".into()),
                        SqliteValue::Integer(0),
                        SqliteValue::Text("page_mvcc".into()),
                        SqliteValue::Text("lock_free_expected".into()),
                        SqliteValue::Text("none".into()),
                    ],
                ]
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn diagnostic_rows_cover_operator_contention_questions() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            install_concurrency_schema(&conn).await?;
            seed_diagnostic_examples(&conn).await?;

            let rows = conn
                .query(
                    "SELECT hotspot_kind,
                statement_fingerprint,
                plan_id,
                table_name,
                page_number,
                busy_family,
                conflict_reason,
                fallback_reason,
                external_wait,
                suggested_next_inspection
           FROM fsqlite_explain_concurrency_contract
          ORDER BY event_seq;",
                )
                .await?;

            assert_eq!(
                rows_to_values(&rows),
                vec![
                    vec![
                        SqliteValue::Text("none".into()),
                        SqliteValue::Text("fp-low-conflict-insert".into()),
                        SqliteValue::Text("plan-insert-disjoint-leaf".into()),
                        SqliteValue::Text("jobs".into()),
                        SqliteValue::Integer(40),
                        SqliteValue::Text("none".into()),
                        SqliteValue::Text("ok_low_conflict".into()),
                        SqliteValue::Null,
                        SqliteValue::Null,
                        SqliteValue::Text("none".into()),
                    ],
                    vec![
                        SqliteValue::Text("page".into()),
                        SqliteValue::Text("fp-hot-page-update".into()),
                        SqliteValue::Text("plan-update-leaf-47".into()),
                        SqliteValue::Text("jobs".into()),
                        SqliteValue::Integer(47),
                        SqliteValue::Text("busy_snapshot".into()),
                        SqliteValue::Text("hot_page_predicted".into()),
                        SqliteValue::Null,
                        SqliteValue::Null,
                        SqliteValue::Text("inspect_page_heat".into()),
                    ],
                    vec![
                        SqliteValue::Text("coordination".into()),
                        SqliteValue::Text("fp-queue-claim".into()),
                        SqliteValue::Text("plan-queue-claim".into()),
                        SqliteValue::Text("fsqlite_queue".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("busy".into()),
                        SqliteValue::Text("coordination_wait".into()),
                        SqliteValue::Null,
                        SqliteValue::Null,
                        SqliteValue::Text("inspect_coordination_owner".into()),
                    ],
                    vec![
                        SqliteValue::Text("coordination".into()),
                        SqliteValue::Text("fp-lease-renew".into()),
                        SqliteValue::Text("plan-lease-renew".into()),
                        SqliteValue::Text("fsqlite_lease".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("none".into()),
                        SqliteValue::Text("coordination_wait".into()),
                        SqliteValue::Null,
                        SqliteValue::Null,
                        SqliteValue::Text("inspect_coordination_owner".into()),
                    ],
                    vec![
                        SqliteValue::Text("range".into()),
                        SqliteValue::Text("fp-range-backfill".into()),
                        SqliteValue::Text("plan-range-disjoint".into()),
                        SqliteValue::Text("events".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("none".into()),
                        SqliteValue::Text("ok_low_conflict".into()),
                        SqliteValue::Null,
                        SqliteValue::Null,
                        SqliteValue::Text("inspect_range_balance".into()),
                    ],
                    vec![
                        SqliteValue::Text("fallback".into()),
                        SqliteValue::Text("fp-window-fallback".into()),
                        SqliteValue::Text("plan-window-compat".into()),
                        SqliteValue::Text("events".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("none".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("compatibility_fallback".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("inspect_fallback_reason".into()),
                    ],
                    vec![
                        SqliteValue::Text("external".into()),
                        SqliteValue::Text("fp-vfs-wait".into()),
                        SqliteValue::Text("plan-checkpoint".into()),
                        SqliteValue::Text("events".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("busy_recovery".into()),
                        SqliteValue::Text("external_resource_wait".into()),
                        SqliteValue::Null,
                        SqliteValue::Text("checkpoint_backpressure".into()),
                        SqliteValue::Text("inspect_vfs_or_runtime_wait".into()),
                    ],
                ]
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn summary_surface_distinguishes_mvcc_coordination_fallback_and_external_waits() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            install_concurrency_schema(&conn).await?;
            seed_diagnostic_examples(&conn).await?;

            let rows = conn
                .query(
                    "SELECT hotspot_kind,
                COUNT(*),
                SUM(predicted_conflict_count),
                SUM(observed_conflict_count),
                SUM(retry_count),
                SUM(abort_count)
           FROM fsqlite_explain_concurrency_contract
          GROUP BY hotspot_kind
          ORDER BY hotspot_kind;",
                )
                .await?;

            assert_eq!(
                rows_to_values(&rows),
                vec![
                    vec![
                        SqliteValue::Text("coordination".into()),
                        SqliteValue::Integer(2),
                        SqliteValue::Integer(2),
                        SqliteValue::Integer(2),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(0),
                    ],
                    vec![
                        SqliteValue::Text("external".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(2),
                        SqliteValue::Integer(0),
                    ],
                    vec![
                        SqliteValue::Text("fallback".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                    ],
                    vec![
                        SqliteValue::Text("none".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                    ],
                    vec![
                        SqliteValue::Text("page".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(3),
                        SqliteValue::Integer(2),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(1),
                    ],
                    vec![
                        SqliteValue::Text("range".into()),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                        SqliteValue::Integer(0),
                    ],
                ]
            );

            let serialized_rows = conn
                .query(
                    "SELECT coordination_strategy
           FROM fsqlite_explain_concurrency_contract
          WHERE coordination_strategy = 'global_writer_lock';",
                )
                .await?;
            assert!(
                serialized_rows.is_empty(),
                "diagnostics must never prescribe a global writer lock"
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}

#[test]
fn rollback_removes_unpublished_diagnostics() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = Connection::open(":memory:").await?;
            install_concurrency_schema(&conn).await?;

            conn.execute("BEGIN;").await?;
            record_diagnostic(
                &conn,
                ConcurrencyDiagnostic {
                    event_seq: 99,
                    diagnostic_surface: "EXPLAIN CONCURRENCY",
                    statement_fingerprint: "fp-rolled-back",
                    plan_id: "plan-rolled-back",
                    table_name: "jobs",
                    index_name: None,
                    range_id: None,
                    queue_name: None,
                    lease_key: None,
                    worker_id: "worker-z",
                    transaction_id: "txn-rollback",
                    hotspot_kind: "page",
                    page_number: Some(55),
                    page_start: Some(55),
                    page_end: Some(55),
                    predicted_conflict_count: 1,
                    observed_conflict_count: 1,
                    retry_count: 0,
                    abort_count: 0,
                    busy_family: "busy_snapshot",
                    conflict_reason: Some("hot_page_predicted"),
                    fallback_reason: None,
                    external_wait: None,
                    coordination_strategy: "page_mvcc",
                    diagnostics_available: true,
                    suggested_next_inspection: "inspect_page_heat",
                    first_failure_diag: "temporary page hotspot",
                },
            )
            .await?;

            let inside_txn = conn
                .query(
                    "SELECT COUNT(*)
           FROM fsqlite_explain_concurrency_contract
          WHERE statement_fingerprint = 'fp-rolled-back';",
                )
                .await?;
            assert_eq!(
                rows_to_values(&inside_txn),
                vec![vec![SqliteValue::Integer(1)]]
            );

            conn.execute("ROLLBACK;").await?;

            let after_rollback = conn
                .query(
                    "SELECT COUNT(*)
           FROM fsqlite_explain_concurrency_contract
          WHERE statement_fingerprint = 'fp-rolled-back';",
                )
                .await?;
            assert_eq!(
                rows_to_values(&after_rollback),
                vec![vec![SqliteValue::Integer(0)]]
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
    });
    outcome
}
