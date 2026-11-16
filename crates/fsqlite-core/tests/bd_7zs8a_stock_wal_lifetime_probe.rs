#![recursion_limit = "512"]

//! bd-7zs8a — does a stock SQLite connection delete our WAL out from under us?
//!
//! Nine `retained_autocommit_*_flushes_prior_batch` unit tests fail on main with
//! `CannotOpen { path: "....db-wal" }`, and all nine share one shape: our
//! Connection writes, then a STOCK `rusqlite` connection opens and closes the
//! same file, then our next statement fails. Bisected to 304280b37
//! ("feat(wal): native WAL-index SHM, append ownership, and reader leases");
//! v0.3.18 is clean and v0.4.4 is not, so the shipped release carries it.
//!
//! The hypothesis this probe tests: stock SQLite checkpoints and unlinks the
//! `-wal` and `-shm` when it believes it is the last connection, and it decides
//! that from the SHM dead-man-switch lock. If our connection no longer holds
//! that lock in a way stock recognises, stock deletes a WAL our open connection
//! still needs, and the next open of it — with `create = false` — raises
//! `CannotOpen` (crates/fsqlite-vfs/src/unix.rs, the generic open's NotFound
//! arm).
//!
//! This prints file existence at each step rather than asserting, so it reports
//! what actually happens instead of encoding today's behaviour as correct.
//!
//!   cargo test -p fsqlite-core --test bd_7zs8a_stock_wal_lifetime_probe -- --ignored --nocapture
//!
//! The non-ignored GH#540 keeper below separately asserts that unrelated
//! descriptor closes cannot let a foreign stock connection destroy live
//! sidecars or committed rows. Run it with this target's `gh540` filter.

use fsqlite_core::connection::Connection;
use std::path::Path;

fn report(label: &str, db: &Path) {
    // Build the companions the way the engine does — by appending to the full
    // file name. `with_extension` would mangle a name that already has one.
    let companion = |suffix: &str| {
        db.with_file_name(format!(
            "{}{suffix}",
            db.file_name().unwrap_or_default().to_string_lossy()
        ))
    };
    let wal = companion("-wal");
    let shm = companion("-shm");
    println!(
        "{label:<38} db={} wal={} shm={}",
        db.exists(),
        wal.exists(),
        shm.exists()
    );
}

#[test]
#[ignore = "bd-7zs8a diagnosis probe; prints WAL companion lifetime, run explicitly"]
fn stock_open_close_while_we_hold_the_database() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("stock_wal_lifetime.db");
    let db_display = db_path.clone();
    let path = db_path.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&path).await.expect("open ours");
        conn.execute("PRAGMA journal_mode=WAL").await.expect("wal");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
            .await
            .expect("ddl");
        conn.execute("INSERT INTO t VALUES (1,'alpha')")
            .await
            .expect("insert");
        report("after our write, ours still open", &db_display);

        // A stock connection reads the same file and closes. Our connection is
        // still open the whole time, so stock must NOT treat itself as the last
        // connection.
        let stock = rusqlite::Connection::open(&db_display).expect("stock open");
        let count: i64 = stock
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .expect("stock read");
        assert_eq!(count, 1, "stock must see our committed row");
        report("stock open, before its close", &db_display);
        drop(stock);
        report("after stock close (ours still open)", &db_display);

        // Whatever the files look like now, our next statement is the one the
        // failing tests trip on.
        let outcome = conn.execute("SAVEPOINT probe_sp").await;
        println!("our next statement after stock close: {outcome:?}");
        if outcome.is_ok() {
            let _ = conn.execute("ROLLBACK").await;
        }
        report("after our next statement", &db_display);
        let _ = conn.close().await;
        report("after our close", &db_display);
    });
}

#[cfg(all(target_os = "linux", feature = "native"))]
mod gh540 {
    use std::io::{BufRead as _, Read as _, Write as _};
    use std::os::unix::fs::MetadataExt as _;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::sync::mpsc::{self, Receiver};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};

    use fsqlite_core::connection::Connection;
    use fsqlite_types::value::SqliteValue;

    const CHILD_TEST: &str = "gh540::stock_child";
    const CHILD_DB: &str = "FSQLITE_GH540_STOCK_DB";
    const CHILD_ROWS: &str = "FSQLITE_GH540_STOCK_ROWS";
    const READY: &str = "GH540|ready";
    const WATCHDOG: Duration = Duration::from_secs(30);

    fn sidecar(db: &Path, suffix: &str) -> PathBuf {
        let mut name = db.as_os_str().to_owned();
        name.push(suffix);
        PathBuf::from(name)
    }

    fn sidecar_identities(db: &Path) -> [Option<(u64, u64)>; 2] {
        ["-wal", "-shm"].map(|suffix| {
            std::fs::metadata(sidecar(db, suffix))
                .ok()
                .map(|metadata| (metadata.dev(), metadata.ino()))
        })
    }

    fn expected_rows(count: i64) -> Vec<(i64, i64, String)> {
        (1..=count)
            .map(|id| (id, id % 7, "x".repeat(600)))
            .collect()
    }

    async fn assert_our_rows(conn: &Connection, count: i64) {
        let rows = conn
            .query("SELECT id, k, body FROM t1 ORDER BY id")
            .await
            .expect("read every committed row");
        let actual = rows
            .iter()
            .map(|row| match row.values() {
                [
                    SqliteValue::Integer(id),
                    SqliteValue::Integer(k),
                    SqliteValue::Text(body),
                ] => (*id, *k, body.to_string()),
                other => panic!("unexpected row: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected_rows(count), "committed rows must survive");
    }

    async fn insert_rows(conn: &Connection, first: i64, last: i64) {
        for id in first..=last {
            let sql = format!(
                "INSERT INTO t1 VALUES ({id}, {}, '{}')",
                id % 7,
                "x".repeat(600)
            );
            assert_eq!(conn.execute(&sql).await.expect("commit row"), 1);
        }
    }

    /// Close a completely independent descriptor, as a header inspector or
    /// backup tool in the embedding application might. No VFS cleanup runs.
    fn read_and_close(path: &Path) {
        let mut file = std::fs::File::open(path).expect("open unrelated descriptor");
        file.read_exact(&mut [0_u8; 100]).expect("read file header");
        drop(file);
    }

    #[test]
    fn stock_child() {
        let Some(path) = std::env::var_os(CHILD_DB) else {
            return;
        };
        let count = std::env::var(CHILD_ROWS)
            .expect("expected stock row count")
            .parse::<i64>()
            .expect("integer row count");
        let conn = rusqlite::Connection::open(Path::new(&path)).expect("stock opens database");
        conn.busy_timeout(Duration::from_secs(5))
            .expect("stock busy timeout");
        let actual = conn
            .prepare("SELECT id, k, body FROM t1 ORDER BY id")
            .expect("stock prepares row query")
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .expect("stock queries rows")
            .collect::<rusqlite::Result<Vec<(i64, i64, String)>>>()
            .expect("stock reads rows");
        assert_eq!(
            actual,
            expected_rows(count),
            "stock sees every committed row"
        );
        let integrity = conn
            .prepare("PRAGMA integrity_check")
            .expect("stock prepares integrity check")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("stock runs integrity check")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("stock reads integrity check");
        assert_eq!(integrity, ["ok"]);
        println!("{READY}");
        std::io::stdout().flush().expect("flush stock readiness");

        // Leave the connection open, but no statement or transaction active.
        // Refreshing its snapshot after the parent writes would mask the
        // stale last-close checkpoint that erased rows in GH#540.
        let mut line = String::new();
        assert_ne!(
            std::io::stdin().lock().read_line(&mut line).unwrap(),
            0,
            "parent closed stock input"
        );
        assert_eq!(line.trim(), "close");
        conn.close().expect("stock closes database");
    }

    struct StockProcess {
        child: Child,
        output: Receiver<String>,
        reader: Option<JoinHandle<()>>,
    }

    impl StockProcess {
        fn open(db: &Path, count: i64) -> Self {
            // The default oracle is always the bundled SQLite, in a separate
            // process. For a local historical-version run, an executable in
            // FSQLITE_GH540_STOCK_ORACLE may implement the identical protocol:
            // args <db> <count>, assert exact rows and integrity, print READY,
            // wait for "close", close the connection and exit successfully.
            // This override never skips a case or relaxes any parent checks.
            let mut command = if let Some(program) = std::env::var_os("FSQLITE_GH540_STOCK_ORACLE")
            {
                let mut command = Command::new(program);
                command.arg(db).arg(count.to_string());
                command
            } else {
                let mut command = Command::new(std::env::current_exe().expect("test binary"));
                command
                    .args([CHILD_TEST, "--exact", "--nocapture", "--test-threads=1"])
                    .env(CHILD_DB, db)
                    .env(CHILD_ROWS, count.to_string());
                command
            };
            let child = command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("spawn stock child");
            let (sender, output) = mpsc::channel();
            let mut owned = Self {
                child,
                output,
                reader: None,
            };
            let stdout = owned.child.stdout.take().expect("stock stdout");
            owned.reader = Some(std::thread::spawn(move || {
                for line in std::io::BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    if line.contains(READY) && sender.send(line).is_err() {
                        break;
                    }
                }
            }));
            owned
                .output
                .recv_timeout(WATCHDOG)
                .expect("stock child must read and acknowledge within watchdog");
            owned
        }

        fn close(mut self) {
            let stdin = self.child.stdin.as_mut().expect("stock stdin");
            writeln!(stdin, "close").expect("signal stock close");
            stdin.flush().expect("flush stock close signal");
            let deadline = Instant::now() + WATCHDOG;
            loop {
                if let Some(status) = self.child.try_wait().expect("poll stock child") {
                    assert!(status.success(), "stock child exited with {status}");
                    break;
                }
                assert!(Instant::now() < deadline, "stock child close timed out");
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }

    impl Drop for StockProcess {
        fn drop(&mut self) {
            drop(self.child.stdin.take());
            if !matches!(self.child.try_wait(), Ok(Some(_))) {
                let _ = self.child.kill();
            }
            let _ = self.child.wait();
            if let Some(reader) = self.reader.take() {
                let _ = reader.join();
            }
        }
    }

    #[test]
    fn committed_rows_survive_unrelated_descriptor_closes() {
        // The DB-only case is the original GH#540 schedule: SQLite before
        // 3.51 decides whether it is last from the main-file lock alone.
        // Newer SQLite also probes the SHM dead-man-switch byte. Closing an
        // unrelated SHM descriptor exercises that lifetime lock too and
        // reproduces the same loss with today's bundled stock oracle.
        for close_shm in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = dir.path().join("committed_rows.db");
            let path = db.to_str().expect("UTF-8 database path");
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(path).await.expect("open ours");
                for pragma in [
                    "PRAGMA journal_mode=WAL",
                    "PRAGMA synchronous=NORMAL",
                    "PRAGMA wal_autocheckpoint=1000",
                ] {
                    conn.execute(pragma).await.expect("configure WAL");
                }
                conn.execute(
                    "CREATE TABLE t1(id INTEGER PRIMARY KEY, k INTEGER NOT NULL, body TEXT NOT NULL)",
                )
                .await
                .expect("create table");
                conn.execute("CREATE INDEX t1_k ON t1(k, id)")
                    .await
                    .expect("create index");
                insert_rows(&conn, 1, 50).await;
                assert_our_rows(&conn, 50).await;
                let initial = sidecar_identities(&db);
                assert!(initial.iter().all(Option::is_some), "WAL sidecars exist");

                read_and_close(&db);
                if close_shm {
                    read_and_close(&sidecar(&db, "-shm"));
                }
                StockProcess::open(&db, 50).close();
                let after_first_close = sidecar_identities(&db);

                insert_rows(&conn, 51, 70).await;
                assert_our_rows(&conn, 70).await;
                let held = StockProcess::open(&db, 70);
                insert_rows(&conn, 71, 80).await;
                assert_our_rows(&conn, 80).await;
                held.close();
                let after_held_close = sidecar_identities(&db);

                // Check data before identities so a regression demonstrates
                // the actual lost commits, not merely missing sidecar files.
                assert_our_rows(&conn, 80).await;
                conn.close().await.expect("close ours");
                StockProcess::open(&db, 80).close();
                let reopened = Connection::open(path).await.expect("reopen ours");
                assert_our_rows(&reopened, 80).await;
                reopened.close().await.expect("close reopened connection");
                assert_eq!(
                    after_first_close, initial,
                    "first stock close must preserve sidecars"
                );
                assert_eq!(
                    after_held_close, initial,
                    "held stock close must preserve sidecars"
                );
            });
        }
    }

    /// bd-drufq: stock SQLite (rusqlite) and FrankenSQLite in the SAME process
    /// on one WAL database. Stock's locks are classic per-process `fcntl`
    /// locks; ours are open-file-description locks. Each engine must see the
    /// other's commits, and no sidecar may be unlinked while either is open.
    mod same_process {
        use super::{
            StockProcess, assert_our_rows, expected_rows, insert_rows, sidecar, sidecar_identities,
        };
        use std::os::unix::fs::MetadataExt as _;
        use std::path::Path;
        use std::time::Duration;

        use fsqlite_core::connection::Connection;

        fn stock_open(db: &Path) -> rusqlite::Connection {
            let stock = rusqlite::Connection::open(db).expect("stock opens database");
            stock
                .busy_timeout(Duration::from_secs(5))
                .expect("stock busy timeout");
            stock
        }

        fn assert_stock_rows(stock: &rusqlite::Connection, count: i64) {
            let actual = stock
                .prepare("SELECT id, k, body FROM t1 ORDER BY id")
                .expect("stock prepares row query")
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .expect("stock queries rows")
                .collect::<rusqlite::Result<Vec<(i64, i64, String)>>>()
                .expect("stock reads rows");
            assert_eq!(
                actual,
                expected_rows(count),
                "stock sees every committed row"
            );
            let integrity = stock
                .prepare("PRAGMA integrity_check")
                .expect("stock prepares integrity check")
                .query_map([], |row| row.get::<_, String>(0))
                .expect("stock runs integrity check")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("stock reads integrity check");
            assert_eq!(integrity, ["ok"], "stock integrity_check");
        }

        fn stock_insert(stock: &rusqlite::Connection, first: i64, last: i64) {
            for id in first..=last {
                stock
                    .execute(
                        "INSERT INTO t1 VALUES (?1, ?2, ?3)",
                        rusqlite::params![id, id % 7, "x".repeat(600)],
                    )
                    .expect("stock commits row");
            }
        }

        /// The kernel lock table entries for the database and its SHM, so a
        /// failure shows which owner (classic POSIX or OFD) held what.
        fn lock_table(db: &Path) -> String {
            let inodes = [db.to_path_buf(), sidecar(db, "-shm")]
                .iter()
                .filter_map(|path| std::fs::metadata(path).ok())
                .map(|metadata| format!(":{}", metadata.ino()))
                .collect::<Vec<_>>();
            std::fs::read_to_string("/proc/locks")
                .unwrap_or_default()
                .lines()
                .filter(|line| {
                    line.split_whitespace()
                        .nth(5)
                        .is_some_and(|field| inodes.iter().any(|ino| field.ends_with(ino.as_str())))
                })
                .map(|line| format!("{line}\n"))
                .collect()
        }

        async fn configure_ours(conn: &Connection) {
            for pragma in ["PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL"] {
                conn.execute(pragma).await.expect("configure WAL");
            }
        }

        const SCHEMA: &str =
            "CREATE TABLE t1(id INTEGER PRIMARY KEY, k INTEGER NOT NULL, body TEXT NOT NULL)";

        #[test]
        fn ours_first_then_stock_both_directions_and_close_orders() {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = dir.path().join("same_process_ours_first.db");
            let path = db.to_str().expect("UTF-8 database path");
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(path).await.expect("open ours");
                configure_ours(&conn).await;
                conn.execute(SCHEMA).await.expect("create table");
                insert_rows(&conn, 1, 20).await;
                let initial = sidecar_identities(&db);
                assert!(initial.iter().all(Option::is_some), "WAL sidecars exist");

                // Our commits are visible to a stock connection in this process.
                let stock = stock_open(&db);
                assert_stock_rows(&stock, 20);
                eprintln!("ours + stock open:\n{}", lock_table(&db));
                // Stock's commit is visible to us, and ours to stock again.
                stock_insert(&stock, 21, 21);
                assert_our_rows(&conn, 21).await;
                insert_rows(&conn, 22, 22).await;
                assert_stock_rows(&stock, 22);
                assert_eq!(sidecar_identities(&db), initial, "both open: sidecars kept");

                // Stock closes while ours is open: it is not the last connection.
                drop(stock);
                assert_eq!(
                    sidecar_identities(&db),
                    initial,
                    "stock close kept sidecars"
                );
                assert_our_rows(&conn, 22).await;
                insert_rows(&conn, 23, 23).await;

                // Stock reopens; then ours closes while stock stays open.
                let stock = stock_open(&db);
                assert_stock_rows(&stock, 23);
                stock_insert(&stock, 24, 24);
                conn.close().await.expect("close ours");
                eprintln!("stock open, ours closed:\n{}", lock_table(&db));
                assert_eq!(sidecar_identities(&db), initial, "our close kept sidecars");
                assert_stock_rows(&stock, 24);
                stock_insert(&stock, 25, 25);

                // Ours reopens beside the open stock connection.
                let conn = Connection::open(path).await.expect("reopen ours");
                assert_our_rows(&conn, 25).await;
                insert_rows(&conn, 26, 26).await;
                assert_stock_rows(&stock, 26);
                assert_eq!(sidecar_identities(&db), initial, "reopen kept sidecars");
                conn.close().await.expect("close reopened ours");
                assert_eq!(
                    sidecar_identities(&db),
                    initial,
                    "second close kept sidecars"
                );
                stock_insert(&stock, 27, 27);
                assert_stock_rows(&stock, 27);
                drop(stock);

                // Both closed: every commit is durable for a fresh pair.
                let conn = Connection::open(path)
                    .await
                    .expect("reopen ours after both");
                assert_our_rows(&conn, 27).await;
                let stock = stock_open(&db);
                assert_stock_rows(&stock, 27);
                drop(stock);
                conn.close().await.expect("close final ours");
            });
        }

        #[test]
        fn stock_first_then_ours_both_directions_and_close_orders() {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = dir.path().join("same_process_stock_first.db");
            let path = db.to_str().expect("UTF-8 database path");
            asupersync::test_utils::run_test(|| async {
                let stock = stock_open(&db);
                let mode: String = stock
                    .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
                    .expect("stock enters WAL mode");
                assert_eq!(mode, "wal");
                stock.execute(SCHEMA, []).expect("stock creates table");
                stock_insert(&stock, 1, 20);
                let initial = sidecar_identities(&db);
                assert!(initial.iter().all(Option::is_some), "WAL sidecars exist");

                let conn = Connection::open(path).await.expect("open ours");
                assert_our_rows(&conn, 20).await;
                insert_rows(&conn, 21, 21).await;
                assert_stock_rows(&stock, 21);
                stock_insert(&stock, 22, 22);
                assert_our_rows(&conn, 22).await;
                assert_eq!(sidecar_identities(&db), initial, "both open: sidecars kept");

                // Ours closes while stock is open; stock keeps working.
                conn.close().await.expect("close ours");
                assert_eq!(sidecar_identities(&db), initial, "our close kept sidecars");
                stock_insert(&stock, 23, 23);
                assert_stock_rows(&stock, 23);

                // Ours reopens; then stock closes while ours is open.
                let conn = Connection::open(path).await.expect("reopen ours");
                assert_our_rows(&conn, 23).await;
                drop(stock);
                assert_eq!(
                    sidecar_identities(&db),
                    initial,
                    "stock close kept sidecars"
                );
                insert_rows(&conn, 24, 24).await;
                let stock = stock_open(&db);
                assert_stock_rows(&stock, 24);
                stock_insert(&stock, 25, 25);
                assert_our_rows(&conn, 25).await;
                drop(stock);
                assert_eq!(
                    sidecar_identities(&db),
                    initial,
                    "stock close kept sidecars"
                );
                conn.close().await.expect("close ours");

                let stock = stock_open(&db);
                assert_stock_rows(&stock, 25);
                drop(stock);
            });
        }

        /// Our last close must not strip the classic locks a stock connection
        /// in this process still holds: a stock connection in ANOTHER process
        /// would then believe it is the last one and unlink the live WAL and
        /// SHM, and this process's stock commits would land in an orphan WAL.
        #[test]
        fn our_close_keeps_in_process_stock_claims_against_a_foreign_closer() {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = dir.path().join("same_process_foreign_closer.db");
            let path = db.to_str().expect("UTF-8 database path");
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(path).await.expect("open ours");
                configure_ours(&conn).await;
                conn.execute(SCHEMA).await.expect("create table");
                insert_rows(&conn, 1, 20).await;
                let initial = sidecar_identities(&db);
                assert!(initial.iter().all(Option::is_some), "WAL sidecars exist");

                let stock = stock_open(&db);
                assert_stock_rows(&stock, 20);
                stock_insert(&stock, 21, 21);
                conn.close().await.expect("close ours");
                let table = lock_table(&db);
                eprintln!("in-process stock open, ours closed:\n{table}");

                // A stock connection in another process opens and closes while
                // the in-process stock connection is still open.
                StockProcess::open(&db, 21).close();
                assert_eq!(
                    sidecar_identities(&db),
                    initial,
                    "a foreign stock close unlinked sidecars an in-process stock connection \
                     still uses; lock table after our close:\n{table}"
                );
                stock_insert(&stock, 22, 22);
                assert_stock_rows(&stock, 22);
                drop(stock);

                let conn = Connection::open(path).await.expect("reopen ours");
                assert_our_rows(&conn, 22).await;
                conn.close().await.expect("close reopened ours");
            });
        }
    }
}
