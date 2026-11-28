#![recursion_limit = "512"]
#![cfg(unix)]

//! bd-reality-check-1022-llk6g.5.3: Cross-process EOF page-number anti-alias
//! renumbering at the WAL append gate (Option A).
//!
//! When multiple OS processes run explicit `BEGIN; INSERT ...; COMMIT;`
//! transactions against separate tables with page-filling and overflow-chain
//! rows, two processes allocate the same fresh EOF page numbers above their
//! begin snapshots. The append gate must renumber the second committer's fresh
//! EOF pages above the durable floor (rewriting every interior-child,
//! cell-overflow, and overflow-next pointer inside the batch) so every row
//! commits cleanly, while true same-page conflicts still fail closed under
//! first-committer-wins.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use fsqlite_core::connection::{Connection, txn_replay_counts};
use fsqlite_types::value::SqliteValue;

const WORKER_ENV: &str = "FSQLITE_BD_LLK6G53_WORKER";
const DB_ENV: &str = "FSQLITE_BD_LLK6G53_DB";
const BARRIER_ENV: &str = "FSQLITE_BD_LLK6G53_BARRIER";
const ROWS_ENV: &str = "FSQLITE_BD_LLK6G53_ROWS";
const GROW_TEST: &str = "cross_process_separate_table_growing_rows_commit_every_row_matching_stock";
const FCW_TEST: &str = "cross_process_same_table_same_page_conflict_still_fails_fcw";
const FCW_ROLE_ENV: &str = "FSQLITE_BD_LLK6G53_FCW_ROLE";
const TRUNCATE_TEST: &str =
    "cross_process_growing_rows_after_peer_truncate_checkpoint_renumber_above_main_file";
const TRUNCATE_ROLE_ENV: &str = "FSQLITE_BD_LLK6G53_TRUNCATE_ROLE";
const TRACE_ENV: &str = "FSQLITE_LLK6G53_TRACE";

fn payload_for(worker: usize, seq: usize) -> String {
    // Mix 3800-byte page-filling rows (forces a leaf split nearly every txn)
    // with 9500-byte rows every 8th txn (forces a multi-page overflow chain).
    let target_len: usize = if seq.is_multiple_of(8) { 9500 } else { 3800 };
    let prefix = format!("w{worker:02}-s{seq:04}-");
    let suffix = format!("-end-{worker:02}-{seq:04}");
    let fill_len = target_len.saturating_sub(prefix.len() + suffix.len());
    let fill_char = char::from(b'a' + u8::try_from((worker + seq) % 26).unwrap_or(0));
    let mut out = String::with_capacity(target_len);
    out.push_str(&prefix);
    for _ in 0..fill_len {
        out.push(fill_char);
    }
    out.push_str(&suffix);
    out
}

fn tag_for(worker: usize, seq: usize) -> String {
    // Long index key every 12th row to also exercise index interior splits and
    // index-cell overflow chains.
    if seq.is_multiple_of(12) {
        format!("idx-{worker:02}-{seq:04}-{}", "k".repeat(900))
    } else {
        format!("idx-{worker:02}-{seq:04}-{}", "m".repeat(48))
    }
}

fn wait_for_file(path: &Path, timeout: Duration) {
    let start = Instant::now();
    while !path.exists() {
        assert!(
            start.elapsed() < timeout,
            "timed out waiting for barrier file {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn run_grow_worker(worker: usize, db_str: &str, barrier_dir: &Path, rows: usize) {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(db_str).await.expect("worker open");
        conn.execute("PRAGMA busy_timeout = 10000;")
            .await
            .expect("worker busy_timeout");
        std::fs::write(barrier_dir.join(format!("ready-{worker}")), b"ok")
            .expect("write ready marker");
        wait_for_file(&barrier_dir.join("go"), Duration::from_secs(60));

        // Even workers use explicit `BEGIN CONCURRENT;` (never replayed by
        // txn_replay); odd workers use plain `BEGIN;` and must still commit on
        // the first attempt with zero replay fallback (`replay_attempts == 0`).
        let begin_sql = if worker.is_multiple_of(2) {
            "BEGIN CONCURRENT;"
        } else {
            "BEGIN;"
        };
        let replays_before = txn_replay_counts();
        let mut committed = 0_usize;
        let mut errors = Vec::<String>::new();
        for seq in 0..rows {
            let payload = payload_for(worker, seq);
            let tag = tag_for(worker, seq);
            let sql = format!(
                "INSERT INTO t_{worker}(seq, tag, payload) VALUES ({seq}, '{tag}', '{payload}');"
            );
            let res: fsqlite_error::Result<()> = async {
                conn.execute(begin_sql).await?;
                conn.execute(&sql).await?;
                conn.execute("COMMIT;").await?;
                Ok(())
            }
            .await;
            match res {
                Ok(()) => committed += 1,
                Err(err) => {
                    errors.push(format!("seq={seq}: {err}"));
                    let _ = conn.execute("ROLLBACK;").await;
                }
            }
        }
        let replays_after = txn_replay_counts();
        let replay_attempts = replays_after
            .attempts
            .saturating_sub(replays_before.attempts);
        conn.close().await.expect("worker close");
        println!(
            "\nWORKER_RESULT worker={worker} committed={committed} errors={} replay_attempts={replay_attempts}",
            errors.len()
        );
        for err in errors.iter().take(10) {
            println!("WORKER_ERROR worker={worker} {err}");
        }
        std::io::stdout().flush().expect("flush worker stdout");
    });
}

#[test]
fn cross_process_separate_table_growing_rows_commit_every_row_matching_stock() {
    if let Ok(worker_str) = std::env::var(WORKER_ENV) {
        let worker: usize = worker_str.parse().expect("worker id");
        let db_str = std::env::var(DB_ENV).expect("db env");
        let barrier_dir = std::path::PathBuf::from(std::env::var(BARRIER_ENV).expect("barrier"));
        let rows: usize = std::env::var(ROWS_ENV)
            .expect("rows env")
            .parse()
            .expect("rows parse");
        run_grow_worker(worker, &db_str, &barrier_dir, rows);
        return;
    }

    let workers = 4_usize;
    let rows = 50_usize;
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("grow_multi.db");
    let stock_path = dir.path().join("grow_stock.db");
    let barrier_dir = dir.path().join("barrier");
    std::fs::create_dir_all(&barrier_dir).expect("create barrier dir");
    let db_str = db_path.to_str().expect("utf-8 db path").to_owned();

    // Seed schema identically on both fsqlite and stock SQLite (live rusqlite 3.54.1).
    let stock = rusqlite::Connection::open(&stock_path).expect("open stock oracle");
    stock
        .execute_batch("PRAGMA journal_mode = WAL;")
        .expect("stock WAL");
    for w in 0..workers {
        stock
            .execute_batch(&format!(
                "CREATE TABLE t_{w}(id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, tag TEXT NOT NULL, payload TEXT NOT NULL); \
                 CREATE INDEX idx_t_{w}_tag ON t_{w}(tag, seq);"
            ))
            .expect("stock create table");
        for seq in 0..rows {
            let payload = payload_for(w, seq);
            let tag = tag_for(w, seq);
            stock
                .execute(
                    &format!("INSERT INTO t_{w}(seq, tag, payload) VALUES (?1, ?2, ?3);"),
                    rusqlite::params![i64::try_from(seq).expect("seq"), tag, payload],
                )
                .expect("stock seed row");
        }
    }

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db_str).await.expect("open fsqlite seed");
        conn.query("PRAGMA journal_mode = WAL;")
            .await
            .expect("fsqlite WAL");
        for w in 0..workers {
            conn.execute(&format!(
                "CREATE TABLE t_{w}(id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, tag TEXT NOT NULL, payload TEXT NOT NULL);"
            ))
            .await
            .expect("create table");
            conn.execute(&format!("CREATE INDEX idx_t_{w}_tag ON t_{w}(tag, seq);"))
                .await
                .expect("create index");
        }
        conn.query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("seed checkpoint");
        conn.close().await.expect("close seed");
    });

    // Spawn `workers` independent OS processes racing on separate tables.
    let exe = std::env::current_exe().expect("current_exe");
    let mut children = Vec::with_capacity(workers);
    for w in 0..workers {
        let child = Command::new(&exe)
            .args([GROW_TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env(WORKER_ENV, w.to_string())
            .env(DB_ENV, &db_str)
            .env(BARRIER_ENV, &barrier_dir)
            .env(ROWS_ENV, rows.to_string())
            .env(TRACE_ENV, "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn worker process");
        children.push((w, child));
    }

    for w in 0..workers {
        wait_for_file(
            &barrier_dir.join(format!("ready-{w}")),
            Duration::from_secs(60),
        );
    }
    std::fs::write(barrier_dir.join("go"), b"go").expect("release go barrier");

    let mut total_committed = 0_usize;
    let mut total_errors = 0_usize;
    let mut total_replay_attempts = 0_u64;
    let mut seen_workers = 0_usize;
    let mut error_samples = Vec::<String>::new();
    let mut renumber_events = 0_usize;

    let draining: Vec<_> = children
        .into_iter()
        .map(|(w, child)| (w, std::thread::spawn(move || child.wait_with_output())))
        .collect();

    for (w, handle) in draining {
        let output = handle
            .join()
            .expect("join worker output thread")
            .expect("wait worker");
        assert!(
            output.status.success(),
            "worker {w} crashed: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        for line in stderr.lines().chain(stdout.lines()) {
            if line.contains("LLK6G53_EOF_RENUMBER") {
                renumber_events += 1;
            }
            if let Some(pos) = line.find("WORKER_ERROR ") {
                error_samples.push(line[pos + "WORKER_ERROR ".len()..].to_owned());
            }
            if let Some(pos) = line.find("WORKER_RESULT ") {
                let rest = &line[pos + "WORKER_RESULT ".len()..];
                let mut committed = None;
                let mut errs = None;
                let mut replays = 0_u64;
                for tok in rest.split_whitespace() {
                    if let Some(v) = tok.strip_prefix("committed=") {
                        committed = v.parse::<usize>().ok();
                    }
                    if let Some(v) = tok.strip_prefix("errors=") {
                        errs = v.parse::<usize>().ok();
                    }
                    if let Some(v) = tok.strip_prefix("replay_attempts=") {
                        replays = v.parse::<u64>().unwrap_or(0);
                    }
                }
                total_committed += committed.expect("committed count");
                total_errors += errs.expect("error count");
                total_replay_attempts += replays;
                seen_workers += 1;
            }
        }
    }

    assert_eq!(
        seen_workers, workers,
        "expected WORKER_RESULT from all {workers} worker processes, saw {seen_workers}"
    );
    println!(
        "LLK6G53_SUMMARY committed={total_committed}/{} errors={total_errors} replay_attempts={total_replay_attempts} renumber_events={renumber_events}",
        workers * rows
    );

    assert_eq!(
        total_errors, 0,
        "bd-reality-check-1022-llk6g.5.3: expected 0 EOF refusals across {workers}x{rows} \
         separate-table growing transactions, got {total_errors} errors (committed={total_committed}/{}, \
         replay_attempts={total_replay_attempts}): {:?}",
        workers * rows,
        error_samples
    );
    assert_eq!(
        total_committed,
        workers * rows,
        "expected every separate-table growing row to commit"
    );
    assert_eq!(
        total_replay_attempts, 0,
        "bd-reality-check-1022-llk6g.5.3: expected 0 txn_replay fallbacks for disjoint growing \
         tables once EOF renumbering lands (got {total_replay_attempts})"
    );
    assert!(
        renumber_events > 0,
        "non-vacuity: expected at least one append-gate EOF renumbering event across {workers} \
         concurrent growing processes, observed 0"
    );

    // Live stock SQLite (bundled rusqlite 3.54.1) verifies integrity, indexes,
    // and exact byte-for-byte payload parity across every table and row.
    let fsqlite_via_stock =
        rusqlite::Connection::open(&db_path).expect("stock open fsqlite db");
    let integrity: String = fsqlite_via_stock
        .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
        .expect("stock integrity_check");
    assert_eq!(integrity, "ok", "stock SQLite integrity_check failed");

    for w in 0..workers {
        let mut f_stmt = fsqlite_via_stock
            .prepare(&format!("SELECT seq, tag, payload FROM t_{w} ORDER BY seq;"))
            .expect("prepare fsqlite select");
        let mut s_stmt = stock
            .prepare(&format!("SELECT seq, tag, payload FROM t_{w} ORDER BY seq;"))
            .expect("prepare stock select");
        let f_rows: Vec<(i64, String, String)> = f_stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .expect("query fsqlite")
            .map(|r| r.expect("fsqlite row"))
            .collect();
        let s_rows: Vec<(i64, String, String)> = s_stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .expect("query stock")
            .map(|r| r.expect("stock row"))
            .collect();
        assert_eq!(
            f_rows, s_rows,
            "table t_{w} diverged from stock SQLite oracle"
        );

        // Verify secondary index lookups work for both normal and overflow index keys.
        for check_seq in [0_usize, 8, 12, 24, rows - 1] {
            let expected_tag = tag_for(w, check_seq);
            let found_seq: i64 = fsqlite_via_stock
                .query_row(
                    &format!("SELECT seq FROM t_{w} WHERE tag = ?1;"),
                    rusqlite::params![expected_tag],
                    |r| r.get(0),
                )
                .expect("index lookup");
            assert_eq!(found_seq, i64::try_from(check_seq).expect("seq"));
        }
    }
}

#[test]
fn cross_process_same_table_same_page_conflict_still_fails_fcw() {
    if std::env::var(FCW_ROLE_ENV).ok().as_deref() == Some("loser") {
        let db_str = std::env::var(DB_ENV).expect("db env");
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&db_str).await.expect("loser open");
            conn.execute("PRAGMA busy_timeout = 5000;")
                .await
                .expect("loser busy_timeout");
            conn.execute("BEGIN CONCURRENT;")
                .await
                .expect("loser begin");
            let big_w2 = "B".repeat(6000);
            conn.execute(&format!("UPDATE shared SET val = '{big_w2}' WHERE id = 1;"))
                .await
                .expect("loser stage same-page update + overflow");
            println!("LOSER_STAGED");
            std::io::stdout().flush().expect("flush");

            let mut line = String::new();
            let _ = BufReader::new(std::io::stdin()).read_line(&mut line);
            let err = conn
                .execute("COMMIT;")
                .await
                .expect_err("cross-process writer modifying same pre-existing page MUST fail FCW");
            let _ = conn.execute("ROLLBACK;").await;
            let err_dbg = format!("{err:?}");
            assert!(
                err_dbg.contains("BusySnapshot"),
                "expected BusySnapshot on cross-process same-page conflict, got {err_dbg}"
            );
            println!("LOSER_REFUSED_FCW");
            std::io::stdout().flush().expect("flush");
            conn.close().await.expect("loser close");
        });
        return;
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("fcw_negative.db");
    let db_str = db_path.to_str().expect("utf-8 path").to_owned();
    let big_w1 = "A".repeat(6000);

    asupersync::test_utils::run_test(|| async {
        let w1 = Connection::open(&db_str).await.expect("open w1");
        w1.query("PRAGMA journal_mode = WAL;").await.expect("WAL");
        w1.execute("CREATE TABLE shared(id INTEGER PRIMARY KEY, val TEXT);")
            .await
            .expect("create shared");
        w1.execute("INSERT INTO shared VALUES (1, 'initial');")
            .await
            .expect("seed shared");

        let mut loser = Command::new(std::env::current_exe().expect("current_exe"))
            .args([FCW_TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env(FCW_ROLE_ENV, "loser")
            .env(DB_ENV, &db_str)
            .env(TRACE_ENV, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn loser");
        let mut loser_stdin = loser.stdin.take().expect("loser stdin");
        let mut loser_stdout = BufReader::new(loser.stdout.take().expect("loser stdout"));
        loop {
            let mut line = String::new();
            let n = loser_stdout.read_line(&mut line).expect("read loser");
            assert!(n > 0, "loser exited before staging");
            if line.contains("LOSER_STAGED") {
                break;
            }
        }

        w1.execute("BEGIN CONCURRENT;").await.expect("w1 begin");
        w1.execute(&format!("UPDATE shared SET val = '{big_w1}' WHERE id = 1;"))
            .await
            .expect("w1 update");
        w1.execute("COMMIT;").await.expect("w1 commits first");

        writeln!(loser_stdin, "GO").expect("signal loser");
        loser_stdin.flush().expect("flush loser stdin");
        loop {
            let mut line = String::new();
            let n = loser_stdout
                .read_line(&mut line)
                .expect("read loser verdict");
            assert!(n > 0, "loser exited without LOSER_REFUSED_FCW");
            if line.contains("LOSER_REFUSED_FCW") {
                break;
            }
        }
        drop(loser_stdin);
        let status = loser.wait().expect("wait loser");
        assert!(status.success(), "loser process failed: {status}");

        let rows = w1
            .query("SELECT val FROM shared WHERE id = 1;")
            .await
            .expect("read winner");
        assert_eq!(rows[0].values()[0], SqliteValue::Text(big_w1.clone().into()));
        w1.close().await.expect("close w1");
    });

    let stock = rusqlite::Connection::open(&db_path).expect("stock open");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
        .expect("integrity_check");
    assert_eq!(integrity, "ok");
    let val: String = stock
        .query_row("SELECT val FROM shared WHERE id = 1;", [], |r| r.get(0))
        .expect("stock read winner");
    assert_eq!(val, big_w1);
}

#[test]
fn cross_process_growing_rows_after_peer_truncate_checkpoint_renumber_above_main_file() {
    if std::env::var(TRUNCATE_ROLE_ENV).ok().as_deref() == Some("peer") {
        let db_str = std::env::var(DB_ENV).expect("db env");
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(&db_str).await.expect("peer open");
            conn.execute("PRAGMA busy_timeout = 10000;")
                .await
                .expect("busy_timeout");
            conn.execute("BEGIN CONCURRENT;").await.expect("peer begin");
            for seq in 0..8_usize {
                let payload = payload_for(1, seq);
                conn.execute(&format!(
                    "INSERT INTO t_1(seq, payload) VALUES ({seq}, '{payload}');"
                ))
                .await
                .expect("peer stage growing row");
            }
            println!("PEER_STAGED");
            std::io::stdout().flush().expect("flush");

            let mut line = String::new();
            let _ = BufReader::new(std::io::stdin()).read_line(&mut line);
            conn.execute("COMMIT;").await.unwrap_or_else(|err| {
                panic!(
                    "bd-reality-check-1022-llk6g.5.3: peer growing commit after sibling growth + checkpoint failed: {err}"
                )
            });
            println!("PEER_COMMITTED");
            std::io::stdout().flush().expect("flush");
            conn.close().await.expect("peer close");
        });
        return;
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("truncate_floor.db");
    let db_str = db_path.to_str().expect("utf-8 path").to_owned();

    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(&db_str).await.expect("open main");
        conn.query("PRAGMA journal_mode = WAL;").await.expect("WAL");
        conn.execute("CREATE TABLE t_0(id INTEGER PRIMARY KEY, seq INTEGER, payload TEXT);")
            .await
            .expect("create t_0");
        conn.execute("CREATE TABLE t_1(id INTEGER PRIMARY KEY, seq INTEGER, payload TEXT);")
            .await
            .expect("create t_1");
        conn.query("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("initial checkpoint");

        let mut peer = Command::new(std::env::current_exe().expect("current_exe"))
            .args([TRUNCATE_TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env(TRUNCATE_ROLE_ENV, "peer")
            .env(DB_ENV, &db_str)
            .env(TRACE_ENV, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn peer");
        let mut peer_stdin = peer.stdin.take().expect("peer stdin");
        let mut peer_stdout = BufReader::new(peer.stdout.take().expect("peer stdout"));
        loop {
            let mut line = String::new();
            let n = peer_stdout.read_line(&mut line).expect("read peer");
            assert!(n > 0, "peer exited before staging");
            if line.contains("PEER_STAGED") {
                break;
            }
        }

        // Main process grows t_0 by 12 pages (including overflow) AND runs a
        // PASSIVE checkpoint while peer holds its pre-growth snapshot.
        conn.execute("BEGIN;").await.expect("main begin");
        for seq in 0..8_usize {
            let payload = payload_for(0, seq);
            conn.execute(&format!(
                "INSERT INTO t_0(seq, payload) VALUES ({seq}, '{payload}');"
            ))
            .await
            .expect("main insert");
        }
        conn.execute("COMMIT;").await.expect("main commit");
        let _ = conn.query("PRAGMA wal_checkpoint(PASSIVE);").await;

        writeln!(peer_stdin, "GO").expect("signal peer");
        peer_stdin.flush().expect("flush peer stdin");
        loop {
            let mut line = String::new();
            let n = peer_stdout.read_line(&mut line).expect("read peer commit");
            assert!(n > 0, "peer exited before committing");
            if line.contains("PEER_COMMITTED") {
                break;
            }
        }
        drop(peer_stdin);
        let status = peer.wait().expect("wait peer");
        assert!(status.success(), "peer failed: {status}");
        conn.close().await.expect("close main");
    });

    let stock = rusqlite::Connection::open(&db_path).expect("stock open");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
        .expect("stock integrity_check");
    assert_eq!(integrity, "ok");
    let c0: i64 = stock
        .query_row("SELECT count(*) FROM t_0;", [], |r| r.get(0))
        .expect("count t_0");
    let c1: i64 = stock
        .query_row("SELECT count(*) FROM t_1;", [], |r| r.get(0))
        .expect("count t_1");
    assert_eq!((c0, c1), (8, 8));
}
